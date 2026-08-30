use super::{ChunkHash, HashAlgorithm, storage::ChunkStorage};
use crate::lock::Lock;
use std::{io::Read, sync::Arc};

/// Streams a repository file entry chunk by chunk. Holds a shared repository lock so chunks
/// cannot be deleted while it is alive.
pub struct EntryReader {
    hashes: std::vec::IntoIter<ChunkHash>,
    storage: Arc<dyn ChunkStorage>,
    algorithm: HashAlgorithm,
    _lock: Lock,
    buffer: Vec<u8>,
    position: usize,
}

impl EntryReader {
    pub fn new(
        hashes: Vec<ChunkHash>,
        storage: Arc<dyn ChunkStorage>,
        algorithm: HashAlgorithm,
        lock: Lock,
    ) -> Self {
        Self {
            hashes: hashes.into_iter(),
            storage,
            algorithm,
            _lock: lock,
            buffer: Vec::new(),
            position: 0,
        }
    }
}

impl Read for EntryReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.position >= self.buffer.len() {
            let Some(hash) = self.hashes.next() else {
                return Ok(0);
            };

            self.buffer = super::read_chunk(&*self.storage, self.algorithm, &hash)?;
            self.position = 0;
        }

        let available = &self.buffer[self.position..];
        let len = available.len().min(buf.len());
        buf[..len].copy_from_slice(&available[..len]);
        self.position += len;

        Ok(len)
    }
}
