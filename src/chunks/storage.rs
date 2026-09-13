use super::ChunkHash;
use std::{
    io::{Read, Write},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

pub trait ChunkStorage: Send + Sync {
    fn path_from_chunk(&self, chunk: &ChunkHash) -> PathBuf {
        let hex = super::hex(chunk);
        let mut path = String::with_capacity(hex.len() + 8);
        path.push_str(&hex[..2]);
        path.push('/');
        path.push_str(&hex[2..4]);
        path.push('/');
        path.push_str(&hex[4..]);
        path.push_str(".chunk");
        PathBuf::from(path)
    }

    fn read_chunk_content(&self, chunk: &ChunkHash)
    -> std::io::Result<Box<dyn Read + Send + Sync>>;
    fn write_chunk_content(&self, chunk: &ChunkHash, content: &[u8]) -> std::io::Result<()>;
    fn delete_chunk_content(&self, chunk: &ChunkHash) -> std::io::Result<()>;
    fn list_chunk_hashes(&self) -> std::io::Result<Vec<ChunkHash>>;

    fn has_chunk(&self, chunk: &ChunkHash) -> std::io::Result<bool> {
        let mut content = match self.read_chunk_content(chunk) {
            Ok(content) => content,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(err),
        };
        // A directory opens but does not read; a chunk has at least its format byte.
        match content.read_exact(&mut [0u8; 1]) {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Removes leftovers of interrupted writes. Called by `clean` while nothing writes chunks.
    fn remove_leftovers(&self) -> std::io::Result<()> {
        Ok(())
    }

    /// Makes every chunk written so far durable; called once before an archive is published.
    fn sync(&self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct ChunkStorageLocal(pub PathBuf);

impl ChunkStorage for ChunkStorageLocal {
    fn read_chunk_content(
        &self,
        chunk: &ChunkHash,
    ) -> std::io::Result<Box<dyn Read + Send + Sync>> {
        Ok(Box::new(std::fs::File::open(
            self.0.join(self.path_from_chunk(chunk)),
        )?))
    }

    fn write_chunk_content(&self, chunk: &ChunkHash, content: &[u8]) -> std::io::Result<()> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let path = self.0.join(self.path_from_chunk(chunk));
        if is_chunk_file(&path) {
            return Ok(());
        }

        let parent = path.parent().unwrap();
        crate::fs::create_dir_all(parent)?;

        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_path = path.with_extension(format!("{}.{unique}.tmp", std::process::id()));
        let mut file = match crate::fs::create_new_file(&tmp_path) {
            // A file here was left by a dead process with the same pid.
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                std::fs::remove_file(&tmp_path)?;
                crate::fs::create_new_file(&tmp_path)?
            }
            file => file?,
        };

        if let Err(err) = file
            .write_all(content)
            .and_then(|()| crate::fs::flush_file(&file))
        {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }

        if let Err(err) = std::fs::rename(&tmp_path, &path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }
        crate::fs::flush_dir(parent)
    }

    fn has_chunk(&self, chunk: &ChunkHash) -> std::io::Result<bool> {
        Ok(is_chunk_file(&self.0.join(self.path_from_chunk(chunk))))
    }

    fn delete_chunk_content(&self, chunk: &ChunkHash) -> std::io::Result<()> {
        let path = self.0.join(self.path_from_chunk(chunk));
        std::fs::remove_file(&path)?;

        for parent in path.ancestors().skip(1).take(2) {
            if std::fs::read_dir(parent)?.next().is_some() {
                break;
            }
            std::fs::remove_dir(parent)?;
        }

        Ok(())
    }

    fn sync(&self) -> std::io::Result<()> {
        crate::fs::sync_filesystem(&self.0)
    }

    fn remove_leftovers(&self) -> std::io::Result<()> {
        // Only this storage's own names, in its own hex-named directories, never through symlinks.
        let is_shard = |entry: &std::fs::DirEntry| -> std::io::Result<bool> {
            Ok(entry.file_type()?.is_dir()
                && entry.file_name().to_str().is_some_and(|name| {
                    name.len() == 2 && name.bytes().all(|b| b.is_ascii_hexdigit())
                }))
        };
        for level in std::fs::read_dir(&self.0)? {
            let level = level?;
            if !is_shard(&level)? {
                continue;
            }
            for dir in std::fs::read_dir(level.path())? {
                let dir = dir?;
                if !is_shard(&dir)? {
                    continue;
                }
                for file in std::fs::read_dir(dir.path())? {
                    let file = file?;
                    if file.file_type()?.is_file()
                        && file.file_name().to_str().is_some_and(is_chunk_temporary)
                    {
                        std::fs::remove_file(file.path())?;
                    }
                }
            }
        }
        Ok(())
    }

    fn list_chunk_hashes(&self) -> std::io::Result<Vec<ChunkHash>> {
        let mut hashes = Vec::new();

        let entries = |path: PathBuf, dirs: bool| -> std::io::Result<Vec<std::fs::DirEntry>> {
            let entries = match std::fs::read_dir(path) {
                Ok(entries) => entries.collect::<Result<Vec<_>, _>>()?,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(err) => return Err(err),
            };
            entries
                .into_iter()
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir() == dirs))
                .map(Ok)
                .collect()
        };

        for first in entries(self.0.clone(), true)? {
            for second in entries(first.path(), true)? {
                for file in entries(second.path(), false)? {
                    let name = format!(
                        "{}{}{}",
                        first.file_name().to_string_lossy(),
                        second.file_name().to_string_lossy(),
                        file.file_name().to_string_lossy()
                    );
                    if let Some(hash) = parse_chunk_name(&name) {
                        hashes.push(hash);
                    }
                }
            }
        }

        Ok(hashes)
    }
}

fn parse_chunk_name(name: &str) -> Option<ChunkHash> {
    let hex = name.strip_suffix(".chunk")?;
    if hex.len() != 64 {
        return None;
    }

    let mut hash = [0; 32];
    for (byte, pair) in hash.iter_mut().zip(hex.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }

    Some(hash)
}

/// A regular file with at least the format byte; an empty one, as a crash leaves, is written over.
fn is_chunk_file(path: &std::path::Path) -> bool {
    path.metadata()
        .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
}

/// `<rest of the hex hash>.<pid>.<counter>.tmp`, as `write_chunk_content` names its work.
fn is_chunk_temporary(name: &str) -> bool {
    let parts: Vec<&str> = name.split('.').collect();
    parts.len() == 4
        && parts[3] == "tmp"
        && parts[0].len() == 60
        && parts[0].bytes().all(|b| b.is_ascii_hexdigit())
        && parts[1..3]
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
}
