use super::ChunkHash;
use std::{io::Write, path::PathBuf};

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
}

pub struct ChunkStorageLocal(pub PathBuf);
impl ChunkStorage for ChunkStorageLocal {
    #[inline]
    fn read_chunk_content(
        &self,
        chunk: &ChunkHash,
    ) -> std::io::Result<Box<dyn std::io::Read + Send>> {
        let path = self.0.join(self.path_from_chunk(chunk));
        let file = std::fs::File::open(path)?;

        Ok(Box::new(file))
    }

    #[inline]
    fn write_chunk_content(
        &self,
        chunk: &ChunkHash,
        mut content: Box<dyn std::io::Read + Send>,
    ) -> std::io::Result<()> {
        let path = self.0.join(self.path_from_chunk(chunk));
        std::fs::create_dir_all(path.parent().unwrap())?;

        let mut file = std::fs::File::create(path)?;

        let mut buffer = [0; 4096];
        loop {
            let bytes_read = content.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            file.write_all(&buffer[..bytes_read])?;
        }

        Ok(())
    }

    #[inline]
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
}
