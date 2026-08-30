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
        if path.exists() {
            return Ok(());
        }

        let parent = path.parent().unwrap();
        crate::fs::create_dir_all(parent)?;

        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_path = path.with_extension(format!("{}.{unique}.tmp", std::process::id()));
        let mut file = crate::fs::create_new_file(&tmp_path)?;

        if let Err(err) = file
            .write_all(content)
            .and_then(|()| crate::fs::flush_file(&file))
        {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }

        std::fs::rename(&tmp_path, &path)?;
        crate::fs::flush_dir(parent)
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
