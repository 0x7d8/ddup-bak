use super::{ChunkHash, HashAlgorithm, storage::ChunkStorage};
use crate::lock::Lock;
use std::{io::Read, sync::Arc};

/// Streams a repository file entry chunk by chunk, holding a lock so its chunks can't be deleted.
pub struct EntryReader {
    hashes: std::vec::IntoIter<ChunkHash>,
    storage: Arc<dyn ChunkStorage>,
    algorithm: HashAlgorithm,
    _lock: Lock,
    /// Chunk being fetched, so a failed read retries it instead of skipping it.
    pending: Option<ChunkHash>,
    buffer: Vec<u8>,
    position: usize,
    /// Recorded file size, checked once all chunks are read.
    size: u64,
    total: u64,
}

impl EntryReader {
    pub fn new(
        hashes: Vec<ChunkHash>,
        size: u64,
        storage: Arc<dyn ChunkStorage>,
        algorithm: HashAlgorithm,
        lock: Lock,
    ) -> Self {
        Self {
            hashes: hashes.into_iter(),
            storage,
            algorithm,
            _lock: lock,
            pending: None,
            buffer: Vec::new(),
            position: 0,
            size,
            total: 0,
        }
    }
}

impl Read for EntryReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.position >= self.buffer.len() {
            let hash = match self.pending {
                Some(hash) => hash,
                None => {
                    let Some(hash) = self.hashes.next() else {
                        if self.total != self.size {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!(
                                    "chunks hold {} bytes of a file recorded as {}",
                                    self.total, self.size
                                ),
                            ));
                        }
                        return Ok(0);
                    };
                    *self.pending.insert(hash)
                }
            };

            self.buffer = super::read_chunk(&*self.storage, self.algorithm, &hash)?;
            self.total += self.buffer.len() as u64;
            self.pending = None;
            self.position = 0;
        }

        let available = &self.buffer[self.position..];
        let len = available.len().min(buf.len());
        buf[..len].copy_from_slice(&available[..len]);
        self.position += len;

        Ok(len)
    }
}
