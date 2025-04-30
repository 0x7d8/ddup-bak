use super::ChunkIndex;
use crate::archive::entries::FileEntry;
use std::io::Read;

pub struct EntryReader {
    pub entry: Box<FileEntry>,
    pub chunk_index: ChunkIndex,

    finished: bool,
    buffer: Vec<u8>,
    buffer_pos: usize,
}

impl EntryReader {
    pub fn new(entry: Box<FileEntry>, chunk_index: ChunkIndex) -> Self {
        Self {
            entry,
            chunk_index,
            finished: false,
            buffer: Vec::new(),
            buffer_pos: 0,
        }
    }

    fn fill_buffer(&mut self) -> std::io::Result<()> {
        if self.finished {
            return Ok(());
        }

        if self.buffer_pos < self.buffer.len() {
            return Ok(());
        }

        self.buffer.clear();
        self.buffer_pos = 0;

        let chunk_id = crate::varint::decode_u64(&mut self.entry);
        if chunk_id == 0 {
            self.finished = true;
            return Ok(());
        }

        let mut chunk = self
            .chunk_index
            .read_chunk_id_content(chunk_id)
            .map_or_else(
                || {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Chunk not found: {}", chunk_id),
                    ))
                },
                Ok,
            )?;

        let mut temp_buf = Vec::new();
        chunk.read_to_end(&mut temp_buf)?;
        self.buffer.extend_from_slice(&temp_buf);

        Ok(())
    }
}

impl Read for EntryReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.buffer_pos >= self.buffer.len() {
            self.fill_buffer()?;
        }

        if self.buffer_pos >= self.buffer.len() {
            return Ok(0);
        }

        let bytes_available = self.buffer.len() - self.buffer_pos;
        let bytes_to_copy = std::cmp::min(bytes_available, buf.len());

        buf[..bytes_to_copy]
            .copy_from_slice(&self.buffer[self.buffer_pos..self.buffer_pos + bytes_to_copy]);

        self.buffer_pos += bytes_to_copy;

        if bytes_to_copy < buf.len() && self.buffer_pos >= self.buffer.len() && !self.finished {
            let additional_bytes = self.read(&mut buf[bytes_to_copy..])?;
            return Ok(bytes_to_copy + additional_bytes);
        }

        Ok(bytes_to_copy)
    }
}
