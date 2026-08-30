//! Per-repository cache of the chunk lists of files seen by the last backup. A file whose
//! metadata still matches is not read again; its chunks are re-referenced from the index.

use crate::{chunks::ChunkHash, varint};
use std::{
    collections::HashMap,
    fs::{File, Metadata},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::SystemTime,
};

const MAGIC: &[u8; 8] = b"DDUPFCH1";

/// Metadata that changes whenever a file's content could have. `ctime` is what makes this
/// robust: unlike mtime it cannot be set by userspace, and any write bumps it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Fingerprint {
    inode: u64,
    size: u64,
    mtime: (u64, u32),
    ctime: (i64, i64),
}

impl Fingerprint {
    pub fn of(metadata: &Metadata) -> Self {
        let mtime = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map_or((0, 0), |mtime| (mtime.as_secs(), mtime.subsec_nanos()));
        #[cfg(unix)]
        let (inode, ctime) = {
            use std::os::unix::fs::MetadataExt;
            (metadata.ino(), (metadata.ctime(), metadata.ctime_nsec()))
        };
        #[cfg(not(unix))]
        let (inode, ctime) = (0, (0, 0));

        Self {
            inode,
            size: metadata.len(),
            mtime,
            ctime,
        }
    }
}

pub struct CachedFile {
    pub fingerprint: Fingerprint,
    pub hashes: Vec<ChunkHash>,
}

#[derive(Default)]
pub struct FileCache {
    files: HashMap<PathBuf, CachedFile>,
}

impl FileCache {
    /// Loads the cache, treating a missing or unreadable one as empty; it only affects speed.
    pub fn load(path: &Path) -> Self {
        let files = File::open(path)
            .map(BufReader::new)
            .ok()
            .and_then(|mut reader| Self::read(&mut reader).ok())
            .unwrap_or_default();
        Self { files }
    }

    fn read(reader: &mut impl Read) -> std::io::Result<HashMap<PathBuf, CachedFile>> {
        let mut magic = [0; 8];
        reader.read_exact(&mut magic)?;
        if magic != *MAGIC {
            return Err(std::io::ErrorKind::InvalidData.into());
        }

        let mut files = HashMap::new();
        loop {
            let mut length = [0; 1];
            if reader.read(&mut length)? == 0 {
                return Ok(files);
            }
            let mut path = vec![0; varint::decode(&mut length.chain(&mut *reader))? as usize];
            reader.read_exact(&mut path)?;
            let path = PathBuf::from(
                String::from_utf8(path).map_err(|_| std::io::ErrorKind::InvalidData)?,
            );

            let mut fixed = [0; 44];
            reader.read_exact(&mut fixed)?;
            let u64_at = |at: usize| u64::from_le_bytes(fixed[at..at + 8].try_into().unwrap());
            let fingerprint = Fingerprint {
                inode: u64_at(0),
                size: u64_at(8),
                mtime: (
                    u64_at(16),
                    u32::from_le_bytes(fixed[24..28].try_into().unwrap()),
                ),
                ctime: (u64_at(28) as i64, u64_at(36) as i64),
            };

            let count = varint::decode(reader)?;
            let mut hashes = Vec::with_capacity(count.min(4096) as usize);
            for _ in 0..count {
                let mut hash = [0; 32];
                reader.read_exact(&mut hash)?;
                hashes.push(hash);
            }
            files.insert(
                path,
                CachedFile {
                    fingerprint,
                    hashes,
                },
            );
        }
    }

    pub fn get(&self, path: &Path, fingerprint: Fingerprint) -> Option<&[ChunkHash]> {
        self.files
            .get(path)
            .filter(|file| file.fingerprint == fingerprint)
            .map(|file| file.hashes.as_slice())
    }

    /// Atomically replaces the cache with `files`.
    pub fn save(
        path: &Path,
        files: impl IntoIterator<Item = (PathBuf, CachedFile)>,
    ) -> std::io::Result<()> {
        let tmp_path = path.with_extension("tmp");
        let mut writer = BufWriter::new(crate::fs::create_file(&tmp_path)?);
        writer.write_all(MAGIC)?;

        for (path, file) in files {
            let path = path.to_string_lossy();
            varint::encode(&mut writer, path.len() as u64)?;
            writer.write_all(path.as_bytes())?;

            let Fingerprint {
                inode,
                size,
                mtime,
                ctime,
            } = file.fingerprint;
            writer.write_all(&inode.to_le_bytes())?;
            writer.write_all(&size.to_le_bytes())?;
            writer.write_all(&mtime.0.to_le_bytes())?;
            writer.write_all(&mtime.1.to_le_bytes())?;
            writer.write_all(&ctime.0.to_le_bytes())?;
            writer.write_all(&ctime.1.to_le_bytes())?;

            varint::encode(&mut writer, file.hashes.len() as u64)?;
            for hash in &file.hashes {
                writer.write_all(hash)?;
            }
        }

        writer.into_inner()?;
        std::fs::rename(&tmp_path, path)
    }
}
