use crate::{archive::CompressionFormat, repository::DeletionProgressCallback, varint};
use blake2::{Blake2b, Digest, digest::consts::U32};
use dashmap::DashMap;
use flate2::{
    read::{DeflateDecoder, GzDecoder},
    write::{DeflateEncoder, GzEncoder},
};
use std::{
    collections::VecDeque,
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{Arc, Mutex, RwLock, atomic::AtomicU64},
};

mod hasher;
pub mod lock;
pub mod reader;
pub mod storage;

pub type ChunkHash = [u8; 32];

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
        ChunkIndex {
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

impl ChunkIndex {
    pub fn new(
        directory: PathBuf,
        chunk_size: usize,
        max_chunk_count: usize,
        storage: Arc<dyn storage::ChunkStorage>,
    ) -> Self {
        let lock = lock::RwLock::new(directory.join("index.lock").to_str().unwrap()).unwrap();

        Self {
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
        }
    }

    pub fn open(
        directory: PathBuf,
        storage: Arc<dyn storage::ChunkStorage>,
    ) -> std::io::Result<Self> {
        let file = File::open(directory.join("index"))?;
        let mut decoder = DeflateDecoder::new(file);

        let mut buffer = [0; 32];
        decoder.read_exact(&mut buffer)?;

        let deleted_chunks = u64::from_le_bytes(buffer[0..8].try_into().unwrap()) as usize;
        let chunk_size = u32::from_le_bytes(buffer[8..12].try_into().unwrap()) as usize;
        let max_chunk_count = u32::from_le_bytes(buffer[12..16].try_into().unwrap()) as usize;
        let chunk_count = u64::from_le_bytes(buffer[16..24].try_into().unwrap()) as usize;
        let next_id = u64::from_le_bytes(buffer[24..32].try_into().unwrap());

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
            let id = varint::decode_u64(&mut decoder);
            result_deleted_chunks.push_back(id);
        }

        loop {
            let mut buffer = [0; 32];
            if decoder.read_exact(&mut buffer).is_err() {
                break;
            }

            let id = varint::decode_u64(&mut decoder);
            let count = varint::decode_u64(&mut decoder);

            result_chunks.insert(id, (buffer, count));
            result_chunk_hashes.insert(buffer, id);
        }

        let lock = lock::RwLock::new(directory.join("index.lock").to_str().unwrap())?;

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

    pub fn save(&self) -> std::io::Result<()> {
        let file = File::create(self.directory.join("index"))?;
        let mut encoder = DeflateEncoder::new(file, flate2::Compression::default());

        let deleted_chunks = self.deleted_chunks.lock().unwrap();

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

        encoder.finish()?;

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

        for (id, chunk) in chunks_to_delete {
            if let Some(f) = progress.clone() {
                f(id, true);
            }

            self.storage.delete_chunk_content(&chunk)?;

            self.chunk_hashes.remove(&chunk);
            self.chunks.remove(&id);

            self.deleted_chunks.lock().unwrap().push_back(id);
        }

        Ok(())
    }

    #[inline]
    pub fn dereference_chunk_id(&self, chunk_id: u64, clean: bool) -> Option<bool> {
        let mut entry = self.chunks.get_mut(&chunk_id)?;
        let (chunk, count) = entry.value_mut();
        let chunk = *chunk;

        *count -= 1;

        if *count == 0 && clean {
            drop(entry);

            self.chunks.remove(&chunk_id);
            self.chunk_hashes.remove(&chunk);

            self.storage.delete_chunk_content(&chunk).ok()?;
            self.deleted_chunks.lock().unwrap().push_back(chunk_id);

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
        let compression = CompressionFormat::decode(compression_bytes[0]);

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
        if let Some(id) = self.deleted_chunks.lock().unwrap().pop_front() {
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
        let id = self.chunk_hashes.get(chunk).map(|v| *v);
        let id = match id {
            Some(id) => id,
            None => {
                let id = self.next_id();
                self.chunk_hashes.insert(*chunk, id);

                id
            }
        };

        let has_references = if let Some(entry) = self.chunks.get(&id) {
            let (_, count) = entry.value();
            *count > 0
        } else {
            false
        };

        if has_references {
            return Ok(id);
        }

        let mut final_data = vec![compression.encode()];

        match compression {
            CompressionFormat::None => final_data.extend_from_slice(data),
            CompressionFormat::Gzip => {
                let mut encoder = GzEncoder::new(&mut final_data, flate2::Compression::default());
                encoder.write_all(data)?;
                encoder.flush()?;
            }
            CompressionFormat::Deflate => {
                let mut encoder =
                    DeflateEncoder::new(&mut final_data, flate2::Compression::default());
                encoder.write_all(data)?;
                encoder.flush()?;
            }
            #[cfg(feature = "brotli")]
            CompressionFormat::Brotli => {
                let mut encoder = brotli::CompressorWriter::new(&mut final_data, 4096, 11, 22);
                encoder.write_all(data)?;
                encoder.flush()?;
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

    pub fn chunk_file(
        &self,
        path: &PathBuf,
        compression: CompressionFormat,
        scope: Option<&rayon::Scope<'_>>,
    ) -> std::io::Result<Vec<u64>> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;

        let mut chunk_count = len / self.chunk_size;
        let mut chunk_size = self.chunk_size;
        let mut chunk_threshold = 50;
        if self.max_chunk_count > 0 {
            while chunk_count > self.max_chunk_count {
                chunk_count /= 2;
                chunk_size *= 2;
            }

            chunk_threshold = self.max_chunk_count / 2;
        }

        if chunk_count > chunk_threshold && scope.is_some() {
            let threads = rayon::current_num_threads();

            if let Some(scope) = scope {
                let path = path.clone();
                let self_clone = self.clone();

                let (sender, receiver) = std::sync::mpsc::channel();

                scope.spawn(move |_| {
                    match self_clone.chunk_file_parallel(
                        &path,
                        compression,
                        chunk_size,
                        chunk_count,
                        threads,
                    ) {
                        Ok(chunk_ids) => {
                            let _ = sender.send(Ok(chunk_ids));
                        }
                        Err(e) => {
                            let _ = sender.send(Err(e));
                        }
                    }
                });

                match receiver.recv() {
                    Ok(result) => result,
                    Err(_) => Err(std::io::Error::other(
                        "Failed to receive result from parallel chunking task",
                    )),
                }
            } else {
                self.chunk_file_parallel(path, compression, chunk_size, chunk_count, threads)
            }
        } else {
            let mut file = File::open(path)?;
            let mut chunks = Vec::with_capacity(chunk_count);
            let mut chunk_ids = Vec::with_capacity(chunk_count);
            let mut buffer = vec![0; chunk_size];
            let mut hasher = Blake2b::<U32>::new();

            loop {
                let bytes_read = file.read(&mut buffer)?;
                if bytes_read == 0 {
                    break;
                }

                hasher.update(&buffer[..bytes_read]);
                let hash = hasher.finalize_reset();
                let mut hash_array = [0; 32];
                hash_array.copy_from_slice(&hash);

                chunk_ids.push(self.add_chunk(&hash_array, &buffer[..bytes_read], compression)?);
                chunks.push(hash_array);
            }

            for (i, chunk_id) in chunk_ids.iter().enumerate() {
                let mut entry = self
                    .chunks
                    .entry(*chunk_id)
                    .or_insert_with(|| (chunks[i], 0));

                entry.1 += 1;
            }

            Ok(chunk_ids)
        }
    }

    fn chunk_file_parallel(
        &self,
        path: &PathBuf,
        compression: CompressionFormat,
        chunk_size: usize,
        chunk_count: usize,
        threads: usize,
    ) -> std::io::Result<Vec<u64>> {
        let file_size = std::fs::metadata(path)?.len() as usize;

        let mut chunk_boundaries = VecDeque::with_capacity(chunk_count);
        for i in 0..chunk_count {
            let start = i * chunk_size;
            let end = if i == chunk_count - 1 {
                file_size
            } else {
                (i + 1) * chunk_size
            };

            if start < file_size {
                chunk_boundaries.push_back((i, start, end.min(file_size)));
            }
        }

        let expected_chunks = chunk_boundaries.len();

        let pool_size = threads.min(expected_chunks);
        let path = path.clone();

        let chunk_queue = Arc::new(Mutex::new(chunk_boundaries));
        let results = Arc::new(Mutex::new(Vec::with_capacity(expected_chunks)));
        let error = Arc::new(RwLock::new(None));

        let mut handles = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let chunk_queue = Arc::clone(&chunk_queue);
            let results = Arc::clone(&results);
            let error = Arc::clone(&error);
            let path = path.clone();
            let self_clone = self.clone();

            let handle = std::thread::spawn(move || {
                loop {
                    let (idx, start, end) =
                        if let Some(chunk) = chunk_queue.lock().unwrap().pop_front() {
                            chunk
                        } else {
                            break;
                        };

                    if error.read().unwrap().is_some() {
                        continue;
                    }

                    let result = (|| {
                        let mut file = File::open(&path)?;
                        file.seek(SeekFrom::Start(start as u64))?;

                        let chunk_size = end - start;
                        let mut buffer = vec![0; chunk_size];
                        let bytes_read = file.read(&mut buffer[0..chunk_size])?;

                        if bytes_read == 0 && start < file_size {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                format!(
                                    "Read 0 bytes at position {start} (expected up to {chunk_size})"
                                ),
                            ));
                        }

                        buffer.truncate(bytes_read);

                        let mut hasher = Blake2b::<U32>::new();
                        hasher.update(&buffer);
                        let hash = hasher.finalize();

                        let mut hash_array = [0; 32];
                        hash_array.copy_from_slice(&hash);

                        let chunk_id = self_clone.add_chunk(&hash_array, &buffer, compression)?;

                        Ok((idx, chunk_id, hash_array))
                    })();

                    match result {
                        Ok(data) => {
                            results.lock().unwrap().push(data);
                        }
                        Err(e) => {
                            *error.write().unwrap() = Some(e);
                        }
                    }
                }
            });

            handles.push(handle);
        }

        for (i, handle) in handles.into_iter().enumerate() {
            if let Err(e) = handle.join() {
                return Err(std::io::Error::other(format!(
                    "Worker thread {i} panicked: {e:?}"
                )));
            }
        }

        if let Some(err) = error.write().unwrap().take() {
            return Err(err);
        }

        let mut results_lock = results.lock().unwrap();
        if results_lock.len() != expected_chunks {
            return Err(std::io::Error::other(format!(
                "Missing chunks: got {} out of {}",
                results_lock.len(),
                expected_chunks
            )));
        }

        results_lock.sort_by_key(|(idx, _, _)| *idx);

        let mut chunk_ids = Vec::with_capacity(results_lock.len());
        let mut chunks = Vec::with_capacity(results_lock.len());

        for (_, chunk_id, hash) in results_lock.iter() {
            chunk_ids.push(*chunk_id);
            chunks.push(*hash);
        }
        drop(results_lock);

        for (i, chunk_id) in chunk_ids.iter().enumerate() {
            let mut entry = self
                .chunks
                .entry(*chunk_id)
                .or_insert_with(|| (chunks[i], 0));

            entry.1 += 1;
        }

        Ok(chunk_ids)
    }
}
