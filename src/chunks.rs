use crate::{archive::CompressionFormat, varint};
use blake2::{Blake2b, Digest, digest::consts::U32};
use flate2::{
    read::{DeflateDecoder, GzDecoder},
    write::{DeflateEncoder, GzEncoder},
};
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    path::PathBuf,
};

pub type ChunkHash = [u8; 32];

#[inline]
fn hash_to_hex_array(hash: &[u8]) -> [[u8; 2]; 32] {
    let mut hex_array = [[0; 2]; 32];
    for (i, byte) in hash.iter().enumerate() {
        hex_array[i][0] = (byte >> 4) & 0x0F;
        hex_array[i][1] = byte & 0x0F;
    }

    hex_array
}

pub struct ChunkIndex {
    pub directory: PathBuf,
    pub save_on_drop: bool,

    next_id: u64,
    chunks: HashMap<u64, (ChunkHash, u64)>,
    chunk_hashes: HashMap<ChunkHash, u64>,
    chunk_size: usize,
}

impl ChunkIndex {
    pub fn new(directory: PathBuf) -> std::io::Result<Self> {
        let file = File::open(directory.join("chunks/index"))?;
        let mut decoder = DeflateDecoder::new(file);

        let mut buffer = [0; 20];
        decoder.read_exact(&mut buffer)?;
        let chunk_size = u32::from_le_bytes(buffer[0..4].try_into().unwrap()) as usize;
        let chunk_count = u64::from_le_bytes(buffer[4..12].try_into().unwrap());
        let next_id = u64::from_le_bytes(buffer[12..20].try_into().unwrap());

        let mut chunk_index = ChunkIndex {
            directory,
            save_on_drop: true,
            next_id,
            chunks: HashMap::with_capacity(chunk_count as usize),
            chunk_hashes: HashMap::with_capacity(chunk_count as usize),
            chunk_size,
        };

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

    pub fn new_empty(directory: PathBuf, chunk_size: usize) -> Self {
        ChunkIndex {
            directory,
            save_on_drop: true,
            next_id: 1,
            chunks: HashMap::new(),
            chunk_hashes: HashMap::new(),
            chunk_size,
        }
    }

    #[inline]
    fn path_from_chunk(&self, chunk: &ChunkHash) -> PathBuf {
        let hex_array = hash_to_hex_array(chunk);
        let mut path = String::with_capacity(7 + 32 * 3 + 10);
        path.push_str("chunks/");

        for i in 0..31 {
            path.push_str(&format!("{:02x}/", hex_array[i][0] << 4 | hex_array[i][1]));
        }

        path.push_str(&format!(
            "{:02x}.chunk",
            hex_array[31][0] << 4 | hex_array[31][1]
        ));

        self.directory.join(path)
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

    #[inline]
    pub fn dereference_chunk_id(&mut self, chunk_id: u64) -> Option<bool> {
        let (_, count) = self.chunks.get_mut(&chunk_id)?;
        *count -= 1;

        if *count == 0 {
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
            .write_all(&(self.chunk_size as u32).to_le_bytes())
            .unwrap();
        encoder
            .write_all(&(self.chunks.len() as u64).to_le_bytes())
            .unwrap();
        encoder.write_all(&self.next_id.to_le_bytes()).unwrap();

        for (id, (chunk, count)) in &self.chunks {
            encoder.write_all(chunk).unwrap();
            encoder.write_all(&varint::encode_u64(*id)).unwrap();
            encoder.write_all(&varint::encode_u64(*count)).unwrap();
        }

        encoder.finish().unwrap();
    }
}
