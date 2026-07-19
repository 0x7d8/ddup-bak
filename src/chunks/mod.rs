use crate::{archive::CompressionFormat, repository::DeletionProgressCallback, varint};
use blake2::{Blake2b, Digest, digest::consts::U32};
use dashmap::DashMap;
use flate2::{
    read::{DeflateDecoder, GzDecoder},
    write::{DeflateEncoder, GzEncoder},
};
use parking_lot::Mutex;
use rayon::prelude::*;
use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io::{BufReader, Cursor, Read, Write},
    path::PathBuf,
    sync::{Arc, atomic::AtomicU64},
};

mod hasher;
pub mod lock;
pub mod reader;
pub mod storage;

pub type ChunkHash = [u8; 32];

pub type RebuildProgressCallback =
    Option<Arc<dyn Fn(u64, &ChunkHash, u64) + Send + Sync + 'static>>;

pub struct ChunkIndex {
    pub directory: PathBuf,
    pub storage: Arc<dyn storage::ChunkStorage>,

    pub lock: Arc<lock::RwLock>,

    next_id: Arc<AtomicU64>,
    deleted_chunks: Arc<Mutex<VecDeque<u64>>>,
    chunks: Arc<DashMap<u64, (ChunkHash, u64), hasher::RandomizingHasherBuilder>>,
    chunk_hashes: Arc<DashMap<ChunkHash, u64, hasher::RandomizingHasherBuilder>>,

    chunk_size: usize,
    max_chunk_count: usize,
}

impl Clone for ChunkIndex {
    fn clone(&self) -> Self {
        Self {
            directory: self.directory.clone(),
            storage: Arc::clone(&self.storage),

            lock: Arc::clone(&self.lock),

            next_id: Arc::clone(&self.next_id),
            deleted_chunks: Arc::clone(&self.deleted_chunks),
            chunks: Arc::clone(&self.chunks),
            chunk_hashes: Arc::clone(&self.chunk_hashes),

            chunk_size: self.chunk_size,
            max_chunk_count: self.max_chunk_count,
        }
    }
}

fn map_cdc_error(err: fastcdc::v2020::Error) -> std::io::Error {
    match err {
        fastcdc::v2020::Error::IoError(err) => err,
        other => std::io::Error::other(other.to_string()),
    }
}

impl ChunkIndex {
    pub fn new(
        directory: PathBuf,
        chunk_size: usize,
        max_chunk_count: usize,
        storage: Arc<dyn storage::ChunkStorage>,
    ) -> std::io::Result<Self> {
        let lock = lock::RwLock::new(directory.join("index.lock"))?;

        Ok(Self {
            directory,
            storage,

            lock: Arc::new(lock),

            next_id: Arc::new(AtomicU64::new(1)),
            deleted_chunks: Arc::new(Mutex::new(VecDeque::new())),
            chunks: Arc::new(DashMap::with_capacity_and_hasher_and_shard_amount(
                10_000,
                hasher::RandomizingHasherBuilder,
                1024,
            )),
            chunk_hashes: Arc::new(DashMap::with_capacity_and_hasher_and_shard_amount(
                10_000,
                hasher::RandomizingHasherBuilder,
                1024,
            )),

            chunk_size,
            max_chunk_count,
        })
    }

    pub fn open(
        directory: PathBuf,
        storage: Arc<dyn storage::ChunkStorage>,
    ) -> std::io::Result<Self> {
        let file = File::open(directory.join("index"))?;
        let mut decoder = DeflateDecoder::new(file);

        let mut buffer = [0; 32];
        decoder.read_exact(&mut buffer)?;

        fn map_err(_err: std::array::TryFromSliceError) -> std::io::Error {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Corrupted index file: invalid header",
            )
        }

        let deleted_chunks = u64::from_le_bytes(buffer[0..8].try_into().map_err(map_err)?) as usize;
        let chunk_size = u32::from_le_bytes(buffer[8..12].try_into().map_err(map_err)?) as usize;
        let max_chunk_count =
            u32::from_le_bytes(buffer[12..16].try_into().map_err(map_err)?) as usize;
        let chunk_count = u64::from_le_bytes(buffer[16..24].try_into().map_err(map_err)?) as usize;
        let next_id = u64::from_le_bytes(buffer[24..32].try_into().map_err(map_err)?);

        let mut result_deleted_chunks = VecDeque::with_capacity(deleted_chunks);
        let result_chunks = DashMap::with_capacity_and_hasher_and_shard_amount(
            chunk_count,
            hasher::RandomizingHasherBuilder,
            1024,
        );
        let result_chunk_hashes = DashMap::with_capacity_and_hasher_and_shard_amount(
            chunk_count,
            hasher::RandomizingHasherBuilder,
            1024,
        );

        for _ in 0..deleted_chunks {
            let id = varint::decode_u64(&mut decoder)?;
            result_deleted_chunks.push_back(id);
        }

        loop {
            let mut buffer = [0; 32];
            if decoder.read_exact(&mut buffer).is_err() {
                break;
            }

            let id = varint::decode_u64(&mut decoder)?;
            let count = varint::decode_u64(&mut decoder)?;

            result_chunks.insert(id, (buffer, count));
            result_chunk_hashes.insert(buffer, id);
        }

        let lock = lock::RwLock::new(directory.join("index.lock"))?;

        Ok(Self {
            directory,
            storage,

            lock: Arc::new(lock),

            next_id: Arc::new(AtomicU64::new(next_id)),
            deleted_chunks: Arc::new(Mutex::new(result_deleted_chunks)),
            chunks: Arc::new(result_chunks),
            chunk_hashes: Arc::new(result_chunk_hashes),

            chunk_size,
            max_chunk_count,
        })
    }

    pub fn rebuild(
        directory: PathBuf,
        archives_directory: &std::path::Path,
        chunk_size: usize,
        max_chunk_count: usize,
        storage: Arc<dyn storage::ChunkStorage>,
        progress: RebuildProgressCallback,
    ) -> std::io::Result<Self> {
        let chunk_hashes_on_disk: Vec<ChunkHash> = storage.list_chunk_hashes()?;

        let chunks: DashMap<u64, (ChunkHash, u64), hasher::RandomizingHasherBuilder> =
            DashMap::with_capacity_and_hasher_and_shard_amount(
                chunk_hashes_on_disk.len(),
                hasher::RandomizingHasherBuilder,
                1024,
            );
        let chunk_hashes_map: DashMap<ChunkHash, u64, hasher::RandomizingHasherBuilder> =
            DashMap::with_capacity_and_hasher_and_shard_amount(
                chunk_hashes_on_disk.len(),
                hasher::RandomizingHasherBuilder,
                1024,
            );

        let old_id_to_hash = Self::try_recover_old_id_map(&directory);

        let mut next_id: u64 = 1;
        let mut old_to_new_id: HashMap<u64, u64> = HashMap::new();

        for hash in &chunk_hashes_on_disk {
            let new_id = next_id;
            next_id += 1;

            chunks.insert(new_id, (*hash, 0));
            chunk_hashes_map.insert(*hash, new_id);
        }

        if let Some(ref old_map) = old_id_to_hash {
            for (old_id, hash) in old_map {
                if let Some(new_id_ref) = chunk_hashes_map.get(hash) {
                    old_to_new_id.insert(*old_id, *new_id_ref.value());
                }
            }
        }

        if archives_directory.exists() {
            for dir_entry in std::fs::read_dir(archives_directory)?.flatten() {
                let path = dir_entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("ddup") {
                    continue;
                }

                let archive = match crate::archive::Archive::open(path) {
                    Ok(a) => a,
                    Err(_) => continue,
                };

                Self::walk_archive_entries_for_refs(
                    archive.into_entries(),
                    &old_to_new_id,
                    &chunks,
                );
            }
        }

        if let Some(ref cb) = progress {
            for entry in chunks.iter() {
                let (id, (hash, count)) = entry.pair();
                cb(*id, hash, *count);
            }
        }

        let lock = lock::RwLock::new(directory.join("index.lock"))?;

        Ok(Self {
            directory,
            storage,

            lock: Arc::new(lock),

            next_id: Arc::new(AtomicU64::new(next_id)),
            deleted_chunks: Arc::new(Mutex::new(VecDeque::new())),
            chunks: Arc::new(chunks),
            chunk_hashes: Arc::new(chunk_hashes_map),

            chunk_size,
            max_chunk_count,
        })
    }

    fn try_recover_old_id_map(directory: &std::path::Path) -> Option<HashMap<u64, ChunkHash>> {
        let file = File::open(directory.join("index")).ok()?;
        let mut decoder = DeflateDecoder::new(file);

        let mut buffer = [0; 32];
        if decoder.read_exact(&mut buffer).is_err() {
            return None;
        }

        let deleted_count = u64::from_le_bytes(buffer[0..8].try_into().ok()?) as usize;

        for _ in 0..deleted_count {
            let mut one_byte = [0u8; 1];
            loop {
                if decoder.read_exact(&mut one_byte).is_err() {
                    return Some(HashMap::new());
                }
                if one_byte[0] & 0x80 == 0 {
                    break;
                }
            }
        }

        let mut map = HashMap::new();

        loop {
            let mut hash_buf = [0; 32];
            if decoder.read_exact(&mut hash_buf).is_err() {
                break;
            }

            let id = match crate::varint::decode_u64(&mut decoder) {
                Ok(v) => v,
                Err(_) => break,
            };

            if crate::varint::decode_u64(&mut decoder).is_err() {
                map.insert(id, hash_buf);
                break;
            }

            map.insert(id, hash_buf);
        }

        Some(map)
    }

    fn walk_archive_entries_for_refs(
        entries: Vec<crate::archive::entries::Entry>,
        old_to_new_id: &HashMap<u64, u64>,
        chunks: &DashMap<u64, (ChunkHash, u64), hasher::RandomizingHasherBuilder>,
    ) {
        for entry in entries {
            match entry {
                crate::archive::entries::Entry::File(mut file_entry) => {
                    while let Ok(Some(old_chunk_id)) = varint::decode_u64_opt(&mut file_entry) {
                        if let Some(&new_id) = old_to_new_id.get(&old_chunk_id)
                            && let Some(mut e) = chunks.get_mut(&new_id)
                        {
                            e.value_mut().1 += 1;
                        }
                    }
                }
                crate::archive::entries::Entry::Directory(dir_entry) => {
                    Self::walk_archive_entries_for_refs(dir_entry.entries, old_to_new_id, chunks);
                }
                _ => {}
            }
        }
    }

    pub fn save(&self) -> std::io::Result<()> {
        let index_path = self.directory.join("index");
        let tmp_path = self.directory.join("index.tmp");

        {
            let file = File::create(&tmp_path)?;
            let mut encoder = DeflateEncoder::new(file, flate2::Compression::default());

            let deleted_chunks = self.deleted_chunks.lock();

            encoder.write_all(&(deleted_chunks.len() as u64).to_le_bytes())?;
            encoder.write_all(&(self.chunk_size as u32).to_le_bytes())?;
            encoder.write_all(&(self.max_chunk_count as u32).to_le_bytes())?;
            encoder.write_all(&(self.chunks.len() as u64).to_le_bytes())?;
            encoder.write_all(
                &self
                    .next_id
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .to_le_bytes(),
            )?;

            for id in deleted_chunks.iter() {
                encoder.write_all(&varint::encode_u64(*id))?;
            }

            for entry in self.chunks.iter() {
                let (id, (chunk, count)) = entry.pair();

                encoder.write_all(chunk)?;
                encoder.write_all(&varint::encode_u64(*id))?;
                encoder.write_all(&varint::encode_u64(*count))?;
            }

            let inner = encoder.finish()?;
            inner.sync_all()?;
        }

        std::fs::rename(&tmp_path, &index_path)?;

        #[cfg(unix)]
        {
            if let Ok(dir) = File::open(&self.directory) {
                let _ = dir.sync_all();
            }
        }

        Ok(())
    }

    #[inline]
    pub fn references(&self, chunk: &ChunkHash) -> u64 {
        if let Some(id) = self.chunk_hashes.get(chunk) {
            let id = *id.value();

            if let Some(entry) = self.chunks.get(&id) {
                let (_, count) = entry.value();
                return *count;
            }
        }

        0
    }

    pub fn clean(&self, progress: DeletionProgressCallback) -> std::io::Result<()> {
        let chunks_to_delete: Vec<_> = self
            .chunks
            .iter()
            .filter_map(|entry| {
                let (id, (chunk, count)) = (entry.key(), entry.value());
                if *count == 0 {
                    Some((*id, *chunk))
                } else {
                    None
                }
            })
            .collect();

        let mut deleted_ids = Vec::with_capacity(chunks_to_delete.len());

        for (id, chunk) in chunks_to_delete {
            if let Some(f) = progress.clone() {
                f(id, true);
            }

            self.storage.delete_chunk_content(&chunk)?;

            self.chunk_hashes.remove(&chunk);
            self.chunks.remove(&id);

            deleted_ids.push(id);
        }

        let mut deleted_chunks = self.deleted_chunks.lock();
        for id in deleted_ids {
            deleted_chunks.push_back(id);
        }

        Ok(())
    }

    #[inline]
    pub fn dereference_chunk_id(&self, chunk_id: u64, clean: bool) -> Option<bool> {
        let mut entry = self.chunks.get_mut(&chunk_id)?;
        let (chunk, count) = entry.value_mut();
        let chunk = *chunk;

        if *count == 0 {
            return Some(false);
        }

        *count -= 1;

        if *count == 0 && clean {
            drop(entry);

            self.chunks.remove(&chunk_id);
            self.chunk_hashes.remove(&chunk);

            self.storage.delete_chunk_content(&chunk).ok()?;
            self.deleted_chunks.lock().push_back(chunk_id);

            return Some(true);
        }

        Some(false)
    }

    #[inline]
    pub fn read_chunk_id_content(&self, chunk_id: u64) -> std::io::Result<Box<dyn Read + Send>> {
        let entry = self.chunks.get(&chunk_id).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Chunk ID {chunk_id} not found"),
            )
        })?;

        let (chunk, _) = entry.value();
        let chunk = *chunk;
        drop(entry);

        let mut reader = self.storage.read_chunk_content(&chunk)?;

        let mut compression_bytes = [0; 1];
        reader.read_exact(&mut compression_bytes)?;
        let compression = CompressionFormat::try_decode(compression_bytes[0])?;

        match compression {
            CompressionFormat::None => Ok(reader),
            CompressionFormat::Gzip => Ok(Box::new(GzDecoder::new(reader))),
            CompressionFormat::Deflate => Ok(Box::new(DeflateDecoder::new(reader))),

            #[cfg(feature = "brotli")]
            CompressionFormat::Brotli => Ok(Box::new(brotli::Decompressor::new(reader, 4096))),
            #[cfg(not(feature = "brotli"))]
            CompressionFormat::Brotli => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Brotli support is not enabled. Please enable the 'brotli' feature.",
            )),
        }
    }

    #[inline]
    pub fn get_chunk_id(&self, chunk: &ChunkHash) -> Option<u64> {
        self.chunk_hashes.get(chunk).map(|v| *v)
    }

    #[inline]
    fn next_id(&self) -> u64 {
        if let Some(id) = self.deleted_chunks.lock().pop_front() {
            return id;
        }

        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn add_chunk(
        &self,
        chunk: &ChunkHash,
        data: &[u8],
        compression: CompressionFormat,
    ) -> std::io::Result<u64> {
        let entry = self.chunk_hashes.entry(*chunk);
        let (id, is_new) = match entry {
            dashmap::mapref::entry::Entry::Occupied(e) => (*e.get(), false),
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let id = self.next_id();
                e.insert(id);
                (id, true)
            }
        };

        if !is_new {
            return Ok(id);
        }

        let mut final_data = vec![compression.encode()];

        match compression {
            CompressionFormat::None => final_data.extend_from_slice(data),
            CompressionFormat::Gzip => {
                let mut encoder = GzEncoder::new(&mut final_data, flate2::Compression::default());
                encoder.write_all(data)?;
                encoder.finish()?;
            }
            CompressionFormat::Deflate => {
                let mut encoder =
                    DeflateEncoder::new(&mut final_data, flate2::Compression::default());
                encoder.write_all(data)?;
                encoder.finish()?;
            }
            #[cfg(feature = "brotli")]
            CompressionFormat::Brotli => {
                let mut encoder = brotli::CompressorWriter::new(&mut final_data, 4096, 11, 22);
                encoder.write_all(data)?;
                drop(encoder);
            }
            #[cfg(not(feature = "brotli"))]
            CompressionFormat::Brotli => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "Brotli support is not enabled. Please enable the 'brotli' feature.",
                ));
            }
        }

        self.storage
            .write_chunk_content(chunk, Box::new(Cursor::new(final_data)))?;

        Ok(id)
    }

    fn cdc_parameters(&self, len: usize) -> (usize, usize, usize) {
        let mut avg = self.chunk_size.max(1);

        if self.max_chunk_count > 0 {
            while len.div_ceil(avg) > self.max_chunk_count && avg < fastcdc::v2020::AVERAGE_MAX {
                avg = avg.saturating_mul(2);
            }
        }

        let avg = avg.clamp(fastcdc::v2020::AVERAGE_MIN, fastcdc::v2020::AVERAGE_MAX);
        let min = (avg / 4).clamp(fastcdc::v2020::MINIMUM_MIN, fastcdc::v2020::MINIMUM_MAX);
        let max = (avg * 4).clamp(fastcdc::v2020::MAXIMUM_MIN, fastcdc::v2020::MAXIMUM_MAX);

        (min, avg, max)
    }

    pub fn chunk_file(
        &self,
        path: &PathBuf,
        compression: CompressionFormat,
    ) -> std::io::Result<Vec<u64>> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;

        let (min_size, avg_size, max_size) = self.cdc_parameters(len);
        let estimated = len.div_ceil(avg_size);

        let batch_size = rayon::current_num_threads().max(1) * 2;

        let mut chunk_ids = Vec::with_capacity(estimated);
        let mut chunks = Vec::with_capacity(estimated);
        let mut batch: Vec<Vec<u8>> = Vec::with_capacity(batch_size);

        for chunk in
            fastcdc::v2020::StreamCDC::new(BufReader::new(file), min_size, avg_size, max_size)
        {
            batch.push(chunk.map_err(map_cdc_error)?.data);

            if batch.len() >= batch_size {
                self.store_batch(&mut batch, compression, &mut chunk_ids, &mut chunks)?;
            }
        }

        self.store_batch(&mut batch, compression, &mut chunk_ids, &mut chunks)?;

        for (i, chunk_id) in chunk_ids.iter().enumerate() {
            let mut entry = self
                .chunks
                .entry(*chunk_id)
                .or_insert_with(|| (chunks[i], 0));

            entry.1 += 1;
        }

        Ok(chunk_ids)
    }

    fn store_batch(
        &self,
        batch: &mut Vec<Vec<u8>>,
        compression: CompressionFormat,
        chunk_ids: &mut Vec<u64>,
        chunks: &mut Vec<ChunkHash>,
    ) -> std::io::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let stored = batch
            .par_iter()
            .map(|data| {
                let mut hasher = Blake2b::<U32>::new();
                hasher.update(data);

                let mut hash = [0; 32];
                hash.copy_from_slice(&hasher.finalize());

                Ok((self.add_chunk(&hash, data, compression)?, hash))
            })
            .collect::<std::io::Result<Vec<_>>>()?;

        for (chunk_id, hash) in stored {
            chunk_ids.push(chunk_id);
            chunks.push(hash);
        }

        batch.clear();

        Ok(())
    }
}
