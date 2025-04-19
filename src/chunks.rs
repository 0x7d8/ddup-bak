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
};

pub type ChunkHash = [u8; 32];

pub struct ChunkIndex {
    pub directory: PathBuf,
    pub save_on_drop: bool,

    next_id: u64,
    deleted_chunks: VecDeque<u64>,
    chunks: HashMap<u64, (ChunkHash, u64)>,
    chunk_hashes: HashMap<ChunkHash, u64>,
    chunk_size: usize,
}

impl ChunkIndex {
    pub fn new(directory: PathBuf, chunk_size: usize) -> Self {
        ChunkIndex {
            directory,
            save_on_drop: true,
            next_id: 1,
            deleted_chunks: VecDeque::new(),
            chunks: HashMap::new(),
            chunk_hashes: HashMap::new(),
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

        let mut chunk_index = ChunkIndex {
            directory,
            save_on_drop: true,
            next_id,
            deleted_chunks: VecDeque::with_capacity(deleted_chunks),
            chunks: HashMap::with_capacity(chunk_count),
            chunk_hashes: HashMap::with_capacity(chunk_count),
            chunk_size,
        };

        for _ in 0..deleted_chunks {
            let id = varint::decode_u64(&mut decoder);
            chunk_index.deleted_chunks.push_back(id);
        }

        loop {
            let mut buffer = [0; 32];
            if decoder.read_exact(&mut buffer).is_err() {
                break;
            }

            let id = varint::decode_u64(&mut decoder);
            let count = varint::decode_u64(&mut decoder);
            chunk_index.chunks.insert(id, (buffer, count));
            chunk_index.chunk_hashes.insert(buffer, id);
        }

        Ok(chunk_index)
    }

    #[inline]
    fn path_from_chunk(&self, chunk: &ChunkHash) -> PathBuf {
        let mut path = self.directory.clone();
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
        let id = self.chunk_hashes.get(chunk);

        let id = match id {
            Some(id) => id,
            None => return 0,
        };

        self.chunks.get(id).copied().map_or(0, |(_, count)| count)
    }

    pub fn clean(&mut self, progress: DeletionProgressCallback) -> std::io::Result<()> {
        for (id, (chunk, count)) in &self.chunks {
            if *count == 0 {
                if let Some(f) = progress {
                    f(*id, true)
                }

                let mut path = self.path_from_chunk(chunk);
                std::fs::remove_file(&path)?;

                self.chunk_hashes.remove(chunk);

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

                self.deleted_chunks.push_back(*id);
            }
        }

        self.chunks.retain(|_, (_, count)| *count > 0);

        Ok(())
    }

    #[inline]
    pub fn dereference_chunk_id(&mut self, chunk_id: u64, clean: bool) -> Option<bool> {
        let (_, count) = self.chunks.get_mut(&chunk_id)?;
        *count -= 1;

        if *count == 0 && clean {
            let (chunk, _) = *self.chunks.get(&chunk_id)?;

            self.chunks.remove(&chunk_id);
            self.chunk_hashes.remove(&chunk);

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

            self.deleted_chunks.push_back(chunk_id);

            return Some(true);
        }

        Some(false)
    }

    #[inline]
    pub fn get_chunk_id_file(&self, chunk_id: u64) -> Option<Box<dyn Read + Send>> {
        let (chunk, _) = self.chunks.get(&chunk_id)?;
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
        self.chunk_hashes.get(chunk).copied()
    }

    #[inline]
    fn next_id(&mut self) -> u64 {
        if let Some(id) = self.deleted_chunks.pop_front() {
            return id;
        }

        let id = self.next_id;
        self.next_id += 1;

        id
    }

    fn add_chunk(
        &mut self,
        chunk: &ChunkHash,
        data: &[u8],
        compression: CompressionFormat,
    ) -> std::io::Result<()> {
        let id = match self.chunk_hashes.get(chunk) {
            Some(id) => *id,
            None => {
                let id = self.next_id();
                self.chunk_hashes.insert(*chunk, id);

                id
            }
        };

        let (_, count) = self.chunks.entry(id).or_insert((*chunk, 0));
        *count += 1;

        if *count > 1 {
            return Ok(());
        }

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
        &mut self,
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
            .write_all(&(self.deleted_chunks.len() as u64).to_le_bytes())
            .unwrap();
        encoder
            .write_all(&(self.chunk_size as u32).to_le_bytes())
            .unwrap();
        encoder
            .write_all(&(self.chunks.len() as u64).to_le_bytes())
            .unwrap();
        encoder.write_all(&self.next_id.to_le_bytes()).unwrap();

        for id in &self.deleted_chunks {
            encoder.write_all(&varint::encode_u64(*id)).unwrap();
        }

        for (id, (chunk, count)) in &self.chunks {
            encoder.write_all(chunk).unwrap();
            encoder.write_all(&varint::encode_u64(*id)).unwrap();
            encoder.write_all(&varint::encode_u64(*count)).unwrap();
        }

        encoder.finish().unwrap();
    }
}
