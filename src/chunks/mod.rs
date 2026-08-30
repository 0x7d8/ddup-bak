use crate::{
    archive::{
        Archive, CompressionFormat, Compressor, decompressor,
        entries::{Entry, FileEntry},
    },
    varint,
};
use dashmap::DashMap;
use flate2::read::DeflateDecoder;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write},
    path::Path,
};
use storage::ChunkStorage;

pub mod reader;
pub mod storage;

pub type ChunkHash = [u8; 32];

/// Largest chunk `cdc_parameters` can produce, and therefore the most memory a single
/// verified chunk read will use.
pub const MAX_CHUNK_SIZE: usize = 4 * fastcdc::v2020::AVERAGE_MAX;

/// Chunk file: a format byte followed by the data.
const CHUNK_HEADER_LEN: u64 = 1;

/// Index format 3: raw, header carries the hash algorithm. Format 2 was Deflate-compressed
/// and always BLAKE3. Format 1 (before `DDUPIDX` magics) was Deflate-compressed, BLAKE2b and
/// keyed by chunk id; see `ChunkIndex::load_v1`.
const INDEX_MAGIC_V3: &[u8; 8] = b"DDUPIDX3";
const INDEX_MAGIC_V2: &[u8; 8] = b"DDUPIDX2";

/// Function identifying chunks by content. Fixed for the lifetime of a chunk store since chunk
/// files are named by their hash; recorded in the index header. BLAKE2b is the default so
/// repositories created by any version look the same; BLAKE3 is faster and opt-in.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HashAlgorithm {
    #[default]
    Blake2b256 = 0,
    Blake3 = 1,
}

impl HashAlgorithm {
    pub const ALL: [Self; 2] = [Self::Blake2b256, Self::Blake3];

    pub const fn encode(&self) -> u8 {
        *self as u8
    }

    pub fn try_decode(value: u8) -> std::io::Result<Self> {
        match value {
            0 => Ok(Self::Blake2b256),
            1 => Ok(Self::Blake3),
            _ => Err(invalid("invalid hash algorithm")),
        }
    }

    pub fn hash(&self, data: &[u8]) -> ChunkHash {
        match self {
            Self::Blake2b256 => {
                use blake2::{Blake2b, Digest, digest::consts::U32};
                Blake2b::<U32>::digest(data).into()
            }
            Self::Blake3 => *blake3::hash(data).as_bytes(),
        }
    }
}

impl std::str::FromStr for HashAlgorithm {
    type Err = std::io::Error;

    fn from_str(name: &str) -> std::io::Result<Self> {
        match name {
            "blake2b" => Ok(Self::Blake2b256),
            "blake3" => Ok(Self::Blake3),
            _ => Err(invalid(format!("unknown hash algorithm {name:?}"))),
        }
    }
}

/// Repository settings stored in the index header.
#[derive(Debug, Clone, Copy)]
pub struct IndexHeader {
    pub version: u8,
    pub chunk_size: usize,
    pub max_chunk_count: usize,
    pub hash_algorithm: HashAlgorithm,
}

/// Reference counts of every chunk in a repository. A cache: `rebuild` recreates it from the
/// archives and chunk storage.
pub struct ChunkIndex {
    pub chunk_size: usize,
    pub max_chunk_count: usize,
    pub hash_algorithm: HashAlgorithm,
    chunks: DashMap<ChunkHash, u64>,
}

impl ChunkIndex {
    pub fn new(chunk_size: usize, max_chunk_count: usize, hash_algorithm: HashAlgorithm) -> Self {
        Self {
            chunk_size,
            max_chunk_count,
            hash_algorithm,
            chunks: DashMap::new(),
        }
    }

    /// Loads an index in the current or the BLAKE3-only format. A format 1 index is an error;
    /// the repository migrates it first (`load_v1`).
    pub fn load(path: &Path) -> std::io::Result<Self> {
        let (header, mut reader, count) = Self::open(path)?;
        if header.version == 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "index uses format 1; the repository has not been migrated",
            ));
        }

        let index = Self::new(
            header.chunk_size,
            header.max_chunk_count,
            header.hash_algorithm,
        );
        let mut hash = [0; 32];
        for _ in 0..count {
            reader.read_exact(&mut hash)?;
            index.chunks.insert(hash, varint::decode(&mut reader)?);
        }
        if reader.read(&mut [0])? != 0 {
            return Err(invalid("index has trailing data"));
        }

        Ok(index)
    }

    /// Loads a format 1 index: reference counts plus the chunk id map that archives of that
    /// era reference chunks by.
    pub fn load_v1(path: &Path) -> std::io::Result<(Self, HashMap<u64, ChunkHash>)> {
        let (header, mut reader, count) = Self::open(path)?;
        if header.version != 1 {
            return Err(invalid("index is not format 1"));
        }

        let index = Self::new(
            header.chunk_size,
            header.max_chunk_count,
            header.hash_algorithm,
        );
        let mut ids = HashMap::with_capacity(count.min(1 << 20) as usize);
        let mut hash = [0; 32];
        loop {
            match reader.read(&mut hash[..1])? {
                0 => break,
                _ => reader.read_exact(&mut hash[1..])?,
            }
            let id = varint::decode(&mut reader)?;
            let references = varint::decode(&mut reader)?;
            index.chunks.insert(hash, references);
            ids.insert(id, hash);
        }

        Ok((index, ids))
    }

    /// `load_v1` for a damaged index, keeping every record that decodes and stopping at the
    /// first one that does not. The records are a Deflate stream, so a truncated or corrupt
    /// index still yields everything written before the damage, and an archive whose chunk ids
    /// are all in that part migrates normally. Records are in hash order rather than id order,
    /// so what survives is an arbitrary subset of the ids: small archives come back far more
    /// often than large ones. Only for `rebuild`, since taking a partial map is a decision to
    /// give up on whatever it does not cover, and that is the caller's to make.
    pub fn salvage_v1(path: &Path) -> std::io::Result<(Self, HashMap<u64, ChunkHash>)> {
        let (header, mut reader, count) = Self::open(path)?;
        if header.version != 1 {
            return Err(invalid("index is not format 1"));
        }

        let index = Self::new(
            header.chunk_size,
            header.max_chunk_count,
            header.hash_algorithm,
        );
        let mut ids = HashMap::with_capacity(count.min(1 << 20) as usize);
        let mut hash = [0; 32];
        while matches!(reader.read(&mut hash[..1]), Ok(1)) {
            let Ok(()) = reader.read_exact(&mut hash[1..]) else {
                break;
            };
            let (Ok(id), Ok(references)) =
                (varint::decode(&mut reader), varint::decode(&mut reader))
            else {
                break;
            };
            index.chunks.insert(hash, references);
            ids.insert(id, hash);
        }

        Ok((index, ids))
    }

    pub fn load_header(path: &Path) -> std::io::Result<IndexHeader> {
        Ok(Self::open(path)?.0)
    }

    /// Identifies the index format, returning its header and a reader positioned at the first
    /// chunk record, plus the record count (unknown for format 1, which is read to EOF).
    fn open(path: &Path) -> std::io::Result<(IndexHeader, Box<dyn Read>, u64)> {
        let mut file = BufReader::new(File::open(path)?);
        let mut magic = [0; 8];
        if file.read_exact(&mut magic).is_ok() && magic == *INDEX_MAGIC_V3 {
            let mut header = [0; 17];
            file.read_exact(&mut header)?;
            let header_out = IndexHeader {
                version: 3,
                chunk_size: u32::from_le_bytes(header[..4].try_into().unwrap()) as usize,
                max_chunk_count: u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize,
                hash_algorithm: HashAlgorithm::try_decode(header[8])?,
            };
            let count = u64::from_le_bytes(header[9..].try_into().unwrap());
            return Ok((header_out, Box::new(file), count));
        }

        file.seek(SeekFrom::Start(0))?;
        let mut decoder = DeflateDecoder::new(file);
        let mut header = [0; 32];
        decoder.read_exact(&mut header[..8])?;
        if header[..8] == *INDEX_MAGIC_V2 {
            decoder.read_exact(&mut header[..16])?;
            let header_out = IndexHeader {
                version: 2,
                chunk_size: u32::from_le_bytes(header[..4].try_into().unwrap()) as usize,
                max_chunk_count: u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize,
                hash_algorithm: HashAlgorithm::Blake3,
            };
            let count = u64::from_le_bytes(header[8..16].try_into().unwrap());
            return Ok((header_out, Box::new(decoder), count));
        }

        // Format 1: deleted-id count, chunk size, max chunk count, chunk count, next id, then
        // the deleted ids as varints, then (hash, id, references) records to EOF.
        decoder.read_exact(&mut header[8..])?;
        let deleted = u64::from_le_bytes(header[..8].try_into().unwrap());
        let header_out = IndexHeader {
            version: 1,
            chunk_size: u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize,
            max_chunk_count: u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize,
            hash_algorithm: HashAlgorithm::Blake2b256,
        };
        let count = u64::from_le_bytes(header[16..24].try_into().unwrap());
        for _ in 0..deleted {
            varint::decode(&mut decoder)?;
        }
        Ok((header_out, Box::new(decoder), count))
    }

    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let tmp_path = path.with_extension("tmp");
        let mut writer = BufWriter::new(crate::fs::create_file(&tmp_path)?);

        writer.write_all(INDEX_MAGIC_V3)?;
        writer.write_all(&(self.chunk_size as u32).to_le_bytes())?;
        writer.write_all(&(self.max_chunk_count as u32).to_le_bytes())?;
        writer.write_all(&[self.hash_algorithm.encode()])?;
        writer.write_all(&(self.chunks.len() as u64).to_le_bytes())?;
        for entry in self.chunks.iter() {
            writer.write_all(entry.key())?;
            varint::encode(&mut writer, *entry.value())?;
        }
        writer.into_inner()?.sync_all()?;

        std::fs::rename(&tmp_path, path)?;
        crate::fs::sync_dir(path.parent().unwrap_or(Path::new(".")))
    }

    /// Recomputes reference counts from `archives`, one archive in memory at a time; chunks in
    /// storage that no archive references are kept with a count of zero so `clean` can delete
    /// them. `progress` receives every reference as it is counted.
    pub fn rebuild(
        chunk_size: usize,
        max_chunk_count: usize,
        hash_algorithm: HashAlgorithm,
        storage: &dyn ChunkStorage,
        archives: impl IntoIterator<Item = std::io::Result<Archive>>,
        progress: impl Fn(&ChunkHash, u64),
    ) -> std::io::Result<Self> {
        let index = Self::new(chunk_size, max_chunk_count, hash_algorithm);
        for hash in storage.list_chunk_hashes()? {
            index.chunks.insert(hash, 0);
        }
        for archive in archives {
            index.count_references(archive?.into_entries(), &progress)?;
        }
        Ok(index)
    }

    fn count_references(
        &self,
        entries: Vec<Entry>,
        progress: &impl Fn(&ChunkHash, u64),
    ) -> std::io::Result<()> {
        for entry in entries {
            match entry {
                Entry::File(mut file) => {
                    for hash in entry_hashes(&mut file)? {
                        progress(&hash, self.reference(&hash));
                    }
                }
                Entry::Directory(dir) => self.count_references(dir.entries, progress)?,
                Entry::Symlink(_) => {}
            }
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    pub fn contains(&self, hash: &ChunkHash) -> bool {
        self.chunks.contains_key(hash)
    }

    pub fn references(&self, hash: &ChunkHash) -> u64 {
        self.chunks.get(hash).map_or(0, |count| *count)
    }

    pub fn iter(&self) -> impl Iterator<Item = (ChunkHash, u64)> + '_ {
        self.chunks
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
    }

    /// Increments the reference count and returns the new count (1 means the chunk is new).
    pub fn reference(&self, hash: &ChunkHash) -> u64 {
        let mut count = self.chunks.entry(*hash).or_insert(0);
        *count += 1;
        *count
    }

    /// Decrements the reference count and returns the new count.
    pub fn dereference(&self, hash: &ChunkHash) -> u64 {
        self.chunks.get_mut(hash).map_or(0, |mut count| {
            *count = count.saturating_sub(1);
            *count
        })
    }

    pub fn remove(&self, hash: &ChunkHash) {
        self.chunks.remove(hash);
    }

    pub fn unreferenced(&self) -> Vec<ChunkHash> {
        self.chunks
            .iter()
            .filter(|entry| *entry.value() == 0)
            .map(|entry| *entry.key())
            .collect()
    }
}

/// Works out which algorithm named the chunks in `storage` by hashing one of them. Empty
/// storage gets the default.
pub fn detect_hash_algorithm(storage: &dyn ChunkStorage) -> std::io::Result<HashAlgorithm> {
    let Some(hash) = storage.list_chunk_hashes()?.into_iter().next() else {
        return Ok(HashAlgorithm::default());
    };
    let data = read_chunk_unverified(storage, &hash)?;
    HashAlgorithm::ALL
        .into_iter()
        .find(|algorithm| algorithm.hash(&data) == hash)
        .ok_or_else(|| {
            invalid(format!(
                "chunk {} does not match its content under any hash algorithm",
                hex(&hash)
            ))
        })
}

pub fn hex(hash: &ChunkHash) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut hex = String::with_capacity(64);
    for byte in hash {
        hex.push(DIGITS[(byte >> 4) as usize] as char);
        hex.push(DIGITS[(byte & 0xF) as usize] as char);
    }
    hex
}

/// FastCDC (min, average, max) sizes for a file of `len` bytes. The average doubles until the
/// expected chunk count fits `max_chunk_count` (0 disables the cap).
pub fn cdc_parameters(
    chunk_size: usize,
    max_chunk_count: usize,
    len: u64,
) -> (usize, usize, usize) {
    use fastcdc::v2020::{
        AVERAGE_MAX, AVERAGE_MIN, MAXIMUM_MAX, MAXIMUM_MIN, MINIMUM_MAX, MINIMUM_MIN,
    };

    let mut avg = chunk_size.clamp(AVERAGE_MIN, AVERAGE_MAX);
    while max_chunk_count > 0
        && len.div_ceil(avg as u64) > max_chunk_count as u64
        && avg < AVERAGE_MAX
    {
        avg = (avg * 2).min(AVERAGE_MAX);
    }

    (
        (avg / 4).clamp(MINIMUM_MIN, MINIMUM_MAX),
        avg,
        (avg * 4).clamp(MAXIMUM_MIN, MAXIMUM_MAX),
    )
}

/// Chunk hashes referenced by a repository file entry (archive format 2 and later).
pub fn entry_hashes(entry: &mut FileEntry) -> std::io::Result<Vec<ChunkHash>> {
    let body = entry_body(entry)?;
    if body.len() % 32 != 0 {
        return Err(invalid(format!(
            "entry {} has a malformed chunk list",
            entry.name
        )));
    }

    Ok(body.as_chunks::<32>().0.to_vec())
}

/// Chunk hashes of a format 1 repository file entry, which lists chunk ids as varints and
/// relies on the index of its era to resolve them.
pub fn entry_hashes_v1(
    entry: &mut FileEntry,
    ids: &HashMap<u64, ChunkHash>,
) -> std::io::Result<Vec<ChunkHash>> {
    let body = entry_body(entry)?;
    let mut cursor = Cursor::new(body.as_slice());
    let mut hashes = Vec::new();
    while (cursor.position() as usize) < body.len() {
        let id = varint::decode(&mut cursor)?;
        hashes.push(*ids.get(&id).ok_or_else(|| {
            invalid(format!(
                "entry {} references unknown chunk id {id}",
                entry.name
            ))
        })?);
    }
    Ok(hashes)
}

fn entry_body(entry: &mut FileEntry) -> std::io::Result<Vec<u8>> {
    let mut body = Vec::with_capacity(usize::try_from(entry.size).unwrap_or(0));
    entry.read_to_end(&mut body)?;
    Ok(body)
}

thread_local! {
    static ZSTD_COMPRESSOR: std::cell::RefCell<Option<zstd::bulk::Compressor<'static>>> = const { std::cell::RefCell::new(None) };
    static ZSTD_DECOMPRESSOR: std::cell::RefCell<Option<zstd::bulk::Decompressor<'static>>> = const { std::cell::RefCell::new(None) };
}

pub fn write_chunk(
    storage: &dyn ChunkStorage,
    hash: &ChunkHash,
    data: &[u8],
    compression: CompressionFormat,
) -> std::io::Result<()> {
    storage.write_chunk_content(hash, &encode_chunk(data, compression)?)
}

/// Chunk file body: a format byte followed by the data. Chunks that do not shrink are stored raw
/// so reads never decompress for nothing. Zstd reuses a per-thread context and records the
/// content size in the frame, which lets `read_chunk` allocate exactly once.
fn encode_chunk(data: &[u8], compression: CompressionFormat) -> std::io::Result<Vec<u8>> {
    let mut content = Vec::with_capacity(1 + data.len());
    content.push(compression.encode());

    match compression {
        CompressionFormat::None => content.extend_from_slice(data),
        CompressionFormat::Zstd => {
            content.reserve(zstd::zstd_safe::compress_bound(data.len()));
            let mut cursor = Cursor::new(content);
            cursor.set_position(1);
            ZSTD_COMPRESSOR.with_borrow_mut(|compressor| {
                match compressor {
                    Some(compressor) => compressor,
                    None => {
                        compressor.insert(zstd::bulk::Compressor::new(crate::archive::ZSTD_LEVEL)?)
                    }
                }
                .compress_to_buffer(data, &mut cursor)
            })?;
            content = cursor.into_inner();
        }
        other => {
            let mut encoder = Compressor::new(other, &mut content)?;
            encoder.write_all(data)?;
            encoder.finish()?;
        }
    }

    if content.len() > data.len() {
        content.clear();
        content.push(CompressionFormat::None.encode());
        content.extend_from_slice(data);
    }
    Ok(content)
}

/// Reads and decompresses a chunk, failing if it does not hash to `hash`.
pub fn read_chunk(
    storage: &dyn ChunkStorage,
    algorithm: HashAlgorithm,
    hash: &ChunkHash,
) -> std::io::Result<Vec<u8>> {
    let data = read_chunk_unverified(storage, hash)?;
    if algorithm.hash(&data) != *hash {
        return Err(invalid(format!("chunk {} is corrupted", hex(hash))));
    }
    Ok(data)
}

fn read_chunk_unverified(storage: &dyn ChunkStorage, hash: &ChunkHash) -> std::io::Result<Vec<u8>> {
    let mut content = Vec::new();
    storage
        .read_chunk_content(hash)?
        .take(MAX_CHUNK_SIZE as u64 + CHUNK_HEADER_LEN + 1)
        .read_to_end(&mut content)?;
    let corrupted = || invalid(format!("chunk {} is corrupted", hex(hash)));

    let Some(&format) = content.first() else {
        return Err(corrupted());
    };
    let format = CompressionFormat::try_decode(format)?;
    content.drain(..CHUNK_HEADER_LEN as usize);

    let data = match format {
        CompressionFormat::None => content,
        CompressionFormat::Zstd
            if let Ok(Some(size)) = zstd::zstd_safe::get_frame_content_size(&content) =>
        {
            if size > MAX_CHUNK_SIZE as u64 {
                return Err(corrupted());
            }
            let mut data = Vec::with_capacity(size as usize);
            ZSTD_DECOMPRESSOR.with_borrow_mut(|decompressor| {
                match decompressor {
                    Some(decompressor) => decompressor,
                    None => decompressor.insert(zstd::bulk::Decompressor::new()?),
                }
                .decompress_to_buffer(&content, &mut data)
            })?;
            data
        }
        format => {
            let mut data = Vec::new();
            decompressor(format, Cursor::new(content))?
                .take(MAX_CHUNK_SIZE as u64 + 1)
                .read_to_end(&mut data)?;
            data
        }
    };

    if data.len() > MAX_CHUNK_SIZE {
        return Err(corrupted());
    }
    Ok(data)
}

fn invalid(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}
