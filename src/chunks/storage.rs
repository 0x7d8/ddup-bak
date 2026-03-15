use super::ChunkHash;
use std::{
    io::Write,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

pub trait ChunkStorage: Sync + Send {
    #[inline]
    fn path_from_chunk(&self, chunk: &ChunkHash) -> PathBuf {
        let mut path = PathBuf::new();
        for byte in chunk.iter().take(2) {
            path.push(format!("{byte:02x}"));
        }

        let mut file_name = String::with_capacity(32 * 2 - 2 * 2 + 6);
        for byte in chunk.iter().skip(2) {
            file_name.push_str(&format!("{byte:02x}"));
        }
        file_name.push_str(".chunk");

        path.push(file_name);

        path
    }

    fn read_chunk_content(
        &self,
        chunk: &ChunkHash,
    ) -> std::io::Result<Box<dyn std::io::Read + Send>>;
    fn write_chunk_content(
        &self,
        chunk: &ChunkHash,
        content: Box<dyn std::io::Read + Send>,
    ) -> std::io::Result<()>;
    fn delete_chunk_content(&self, chunk: &ChunkHash) -> std::io::Result<()>;

    fn list_chunk_hashes(&self) -> std::io::Result<Vec<ChunkHash>>;
}

pub struct ChunkStorageLocal(pub PathBuf);

impl ChunkStorageLocal {
    fn parse_chunk_path(dir1: &str, dir2: &str, filename: &str) -> Option<ChunkHash> {
        let stem = filename.strip_suffix(".chunk")?;

        if dir1.len() != 2 || dir2.len() != 2 || stem.len() != 60 {
            return None;
        }

        let mut hash = [0u8; 32];

        hash[0] = u8::from_str_radix(dir1, 16).ok()?;
        hash[1] = u8::from_str_radix(dir2, 16).ok()?;

        for i in 0..30 {
            hash[2 + i] = u8::from_str_radix(&stem[i * 2..i * 2 + 2], 16).ok()?;
        }

        Some(hash)
    }
}

impl ChunkStorage for ChunkStorageLocal {
    fn read_chunk_content(
        &self,
        chunk: &ChunkHash,
    ) -> std::io::Result<Box<dyn std::io::Read + Send>> {
        let path = self.0.join(self.path_from_chunk(chunk));
        let file = std::fs::File::open(path)?;

        Ok(Box::new(file))
    }

    fn write_chunk_content(
        &self,
        chunk: &ChunkHash,
        mut content: Box<dyn std::io::Read + Send>,
    ) -> std::io::Result<()> {
        let path = self.0.join(self.path_from_chunk(chunk));
        std::fs::create_dir_all(path.parent().unwrap())?;

        if path.exists() {
            return Ok(());
        }

        static WRITE_COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = WRITE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tid = std::thread::current().id();
        let tmp_path = path.with_extension(format!("tmp.{tid:?}.{unique}"));

        let write_result = (|| {
            let mut file = std::fs::File::create(&tmp_path)?;

            let mut buffer = [0; 4096];
            loop {
                let bytes_read = content.read(&mut buffer)?;
                if bytes_read == 0 {
                    break;
                }
                file.write_all(&buffer[..bytes_read])?;
            }

            file.sync_all()?;

            Ok(())
        })();

        if let Err(e) = write_result {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }

        std::fs::rename(&tmp_path, &path)?;

        Ok(())
    }

    fn delete_chunk_content(&self, chunk: &ChunkHash) -> std::io::Result<()> {
        let mut path = self.0.join(self.path_from_chunk(chunk));
        std::fs::remove_file(&path)?;

        while let Some(parent) = path.parent() {
            if parent == self.0 {
                break;
            }

            if std::fs::read_dir(parent)?.count() == 0 {
                std::fs::remove_dir(parent)?;
            } else {
                break;
            }

            path = parent.to_path_buf();
        }

        Ok(())
    }

    fn list_chunk_hashes(&self) -> std::io::Result<Vec<ChunkHash>> {
        let mut hashes = Vec::new();

        let root = &self.0;

        let top_entries = match std::fs::read_dir(root) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(hashes),
            Err(e) => return Err(e),
        };

        for top_entry in top_entries {
            let top_entry = top_entry?;
            if !top_entry.file_type()?.is_dir() {
                continue;
            }

            let dir1_name = match top_entry.file_name().into_string() {
                Ok(s) => s,
                Err(_) => continue,
            };

            if dir1_name.len() != 2 || !dir1_name.chars().all(|c| c.is_ascii_hexdigit()) {
                continue;
            }

            for mid_entry in std::fs::read_dir(top_entry.path())? {
                let mid_entry = mid_entry?;
                if !mid_entry.file_type()?.is_dir() {
                    continue;
                }

                let dir2_name = match mid_entry.file_name().into_string() {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                if dir2_name.len() != 2 || !dir2_name.chars().all(|c| c.is_ascii_hexdigit()) {
                    continue;
                }

                for file_entry in std::fs::read_dir(mid_entry.path())? {
                    let file_entry = file_entry?;
                    if !file_entry.file_type()?.is_file() {
                        continue;
                    }

                    let file_name = match file_entry.file_name().into_string() {
                        Ok(s) => s,
                        Err(_) => continue,
                    };

                    if file_name.contains(".tmp") {
                        continue;
                    }

                    if let Some(hash) = Self::parse_chunk_path(&dir1_name, &dir2_name, &file_name) {
                        hashes.push(hash);
                    }
                }
            }
        }

        Ok(hashes)
    }
}
