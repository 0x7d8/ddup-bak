use crate::{archive::CompressionFormat, repository::DeletionProgressCallback, varint};
use blake2::{Blake2b, Digest, digest::consts::U32};
use flate2::{
    read::{DeflateDecoder, GzDecoder},
    write::{DeflateEncoder, GzEncoder},
};
use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex, RwLock, atomic::AtomicU64},
};

pub type ChunkHash = [u8; 32];

#[derive(Debug, Clone)]
pub struct ChunkIndex {
    pub directory: PathBuf,
    pub save_on_drop: bool,

    next_id: Arc<AtomicU64>,
    deleted_chunks: Arc<Mutex<VecDeque<u64>>>,
    chunks: Arc<RwLock<HashMap<u64, (ChunkHash, u64)>>>,
    chunk_hashes: Arc<RwLock<HashMap<ChunkHash, u64>>>,
    chunk_size: usize,
}

impl ChunkIndex {
    pub fn new(directory: PathBuf, chunk_size: usize) -> Self {
        ChunkIndex {
            directory,
            save_on_drop: true,
            next_id: Arc::new(AtomicU64::new(1)),
            deleted_chunks: Arc::new(Mutex::new(VecDeque::new())),
            chunks: Arc::new(RwLock::new(HashMap::new())),
            chunk_hashes: Arc::new(RwLock::new(HashMap::new())),
            chunk_size,
        }
    }

    pub fn open(directory: PathBuf) -> std::io::Result<Self> {
        let file = File::open(directory.join("chunks/index"))?;
        let mut decoder = DeflateDecoder::new(file);

        let mut buffer = [0; 28];
        decoder.read_exact(&mut buffer)?;

        let deleted_chunks = u64::from_le_bytes(buffer[0..8].try_into().unwrap()) as usize;
        let chunk_size = u32::from_le_bytes(buffer[8..12].try_into().unwrap()) as usize;
        let chunk_count = u64::from_le_bytes(buffer[12..20].try_into().unwrap()) as usize;
        let next_id = u64::from_le_bytes(buffer[20..28].try_into().unwrap());

        let mut result_deleted_chunks = VecDeque::with_capacity(deleted_chunks);
        let mut result_chunks = HashMap::with_capacity(chunk_count);
        let mut result_chunk_hashes = HashMap::with_capacity(chunk_count);

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

        Ok(Self {
            directory,
            save_on_drop: true,
            next_id: Arc::new(AtomicU64::new(next_id)),
            deleted_chunks: Arc::new(Mutex::new(result_deleted_chunks)),
            chunks: Arc::new(RwLock::new(result_chunks)),
            chunk_hashes: Arc::new(RwLock::new(result_chunk_hashes)),
            chunk_size,
        })
    }

    #[inline]
    fn path_from_chunk(&self, chunk: &ChunkHash) -> PathBuf {
        let mut path = self.directory.join("chunks");
        for byte in chunk.iter().take(2) {
            path.push(format!("{:02x}", byte));
        }

        let mut file_name = String::with_capacity(32 * 2 - 2 * 2 + 6);
        for byte in chunk.iter().skip(2) {
            file_name.push_str(&format!("{:02x}", byte));
        }
        file_name.push_str(".chunk");

        path.push(file_name);

        path
    }

    #[inline]
    pub fn set_save_on_drop(&mut self, save_on_drop: bool) {
        self.save_on_drop = save_on_drop;
    }

    #[inline]
    pub fn references(&self, chunk: &ChunkHash) -> u64 {
        let id = self.chunk_hashes.read().unwrap();
        let id = id.get(chunk);

        let id = match id {
            Some(id) => id,
            None => return 0,
        };

        self.chunks
            .read()
            .unwrap()
            .get(id)
            .copied()
            .map_or(0, |(_, count)| count)
    }

    pub fn clean(&mut self, progress: DeletionProgressCallback) -> std::io::Result<()> {
        for (id, (chunk, count)) in self.chunks.read().unwrap().iter() {
            if *count == 0 {
                if let Some(f) = progress {
                    f(*id, true)
                }

                let mut path = self.path_from_chunk(chunk);
                std::fs::remove_file(&path)?;

                self.chunk_hashes.write().unwrap().remove(chunk);

                while let Some(parent) = path.parent() {
                    if parent == self.directory {
                        break;
                    }

                    if std::fs::read_dir(parent)?.count() == 0 {
                        std::fs::remove_dir(parent)?;
                    } else {
                        break;
                    }

                    path = parent.to_path_buf();
                }

                self.deleted_chunks.lock().unwrap().push_back(*id);
            }
        }

        self.chunks
            .write()
            .unwrap()
            .retain(|_, (_, count)| *count > 0);

        Ok(())
    }

    #[inline]
    pub fn dereference_chunk_id(&mut self, chunk_id: u64, clean: bool) -> Option<bool> {
        let mut count = self.chunks.write().unwrap();
        let (_, count) = count.get_mut(&chunk_id)?;
        *count -= 1;

        if *count == 0 && clean {
            let (chunk, _) = *self.chunks.read().unwrap().get(&chunk_id)?;

            self.chunks.write().unwrap().remove(&chunk_id);
            self.chunk_hashes.write().unwrap().remove(&chunk);

            let mut path = self.path_from_chunk(&chunk);
            std::fs::remove_file(&path).ok()?;

            while let Some(parent) = path.parent() {
                if parent == self.directory {
                    break;
                }

                if std::fs::read_dir(parent).ok()?.count() == 0 {
                    std::fs::remove_dir(parent).ok()?;
                } else {
                    break;
                }

                path = parent.to_path_buf();
            }

            self.deleted_chunks.lock().unwrap().push_back(chunk_id);

            return Some(true);
        }

        Some(false)
    }

    #[inline]
    pub fn get_chunk_id_file(&self, chunk_id: u64) -> Option<Box<dyn Read + Send>> {
        let chunk = self.chunks.read().unwrap();
        let (chunk, _) = chunk.get(&chunk_id)?;
        let path = self.path_from_chunk(chunk);

        let mut file = File::open(path).ok()?;

        let mut compression_bytes = [0; 1];
        file.read_exact(&mut compression_bytes).ok()?;
        let compression = CompressionFormat::decode(compression_bytes[0]);

        match compression {
            CompressionFormat::None => Some(Box::new(file)),
            CompressionFormat::Gzip => {
                let decoder = GzDecoder::new(file);

                Some(Box::new(decoder))
            }
            CompressionFormat::Deflate => {
                let decoder = DeflateDecoder::new(file);

                Some(Box::new(decoder))
            }
        }
    }

    #[inline]
    pub fn get_chunk_id(&self, chunk: &ChunkHash) -> Option<u64> {
        self.chunk_hashes.read().unwrap().get(chunk).copied()
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
    ) -> std::io::Result<()> {
        let id = self.chunk_hashes.read().unwrap().get(chunk).copied();
        let id = match id {
            Some(id) => id,
            None => {
                let id = self.next_id();
                self.chunk_hashes.write().unwrap().insert(*chunk, id);

                id
            }
        };

        let count = self.chunks.read().unwrap().get(&id).copied();
        let (_, count) = match count {
            Some((_, count)) => (chunk, count),
            None => {
                self.chunks.write().unwrap().insert(id, (*chunk, 0));

                (chunk, 0)
            }
        };

        if count > 0 {
            return Ok(());
        }

        self.chunks.write().unwrap().entry(id).and_modify(|e| {
            e.1 += 1;
        });

        let path = self.path_from_chunk(chunk);

        std::fs::create_dir_all(path.parent().unwrap())?;
        let mut file = std::fs::File::create(path)?;
        file.write_all(&[compression.encode()])?;

        match compression {
            CompressionFormat::None => file.write_all(data)?,
            CompressionFormat::Gzip => {
                let mut encoder = GzEncoder::new(&file, flate2::Compression::default());
                encoder.write_all(data)?;
                encoder.flush()?;
            }
            CompressionFormat::Deflate => {
                let mut encoder = DeflateEncoder::new(&file, flate2::Compression::default());
                encoder.write_all(data)?;
                encoder.flush()?;
            }
        }

        file.flush()?;
        file.sync_all()?;

        Ok(())
    }

    pub fn chunk_file(
        &self,
        path: &PathBuf,
        compression: CompressionFormat,
    ) -> std::io::Result<Vec<ChunkHash>> {
        let mut file = File::open(path)?;
        let len = file.metadata()?.len();

        let mut chunks = Vec::with_capacity(len as usize / self.chunk_size);
        let mut buffer = vec![0; self.chunk_size];
        let mut hasher = Blake2b::<U32>::new();

        loop {
            let bytes_read = file.read(&mut buffer).unwrap();
            if bytes_read == 0 {
                break;
            }

            hasher.update(&buffer[..bytes_read]);
            let hash = hasher.finalize_reset();
            let mut hash_array = [0; 32];
            hash_array.copy_from_slice(&hash);

            self.add_chunk(&hash_array, &buffer[..bytes_read], compression)?;
            chunks.push(hash_array);
        }

        Ok(chunks)
    }
}

impl Drop for ChunkIndex {
    fn drop(&mut self) {
        let file = File::create(self.directory.join("chunks/index")).unwrap();
        let mut encoder = DeflateEncoder::new(file, flate2::Compression::default());

        encoder
            .write_all(&(self.deleted_chunks.lock().unwrap().len() as u64).to_le_bytes())
            .unwrap();
        encoder
            .write_all(&(self.chunk_size as u32).to_le_bytes())
            .unwrap();
        encoder
            .write_all(&(self.chunks.read().unwrap().len() as u64).to_le_bytes())
            .unwrap();
        encoder
            .write_all(
                &self
                    .next_id
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .to_le_bytes(),
            )
            .unwrap();

        for id in self.deleted_chunks.lock().unwrap().iter() {
            encoder.write_all(&varint::encode_u64(*id)).unwrap();
        }

        for (id, (chunk, count)) in self.chunks.read().unwrap().iter() {
            encoder.write_all(chunk).unwrap();
            encoder.write_all(&varint::encode_u64(*id)).unwrap();
            encoder.write_all(&varint::encode_u64(*count)).unwrap();
        }

        encoder.finish().unwrap();
    }
}
