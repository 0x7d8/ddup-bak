use super::CompressionFormat;
use flate2::read::{DeflateDecoder, GzDecoder};
use positioned_io::ReadAt;
use std::{
    fmt::{Debug, Formatter},
    fs::{File, Permissions},
    io::Read,
    sync::Arc,
    time::SystemTime,
};

pub struct FileEntry {
    pub name: String,
    pub mode: Permissions,
    pub owner: (u32, u32),
    pub mtime: SystemTime,

    pub compression: CompressionFormat,
    pub size_compressed: Option<u64>,
    pub size: u64,

    pub(crate) file: Arc<File>,
    pub(crate) decoder: Option<Box<dyn Read + Send>>,
    pub(crate) offset: u64,
    pub(crate) consumed: u64,
}

impl Clone for FileEntry {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            mode: self.mode.clone(),
            owner: self.owner,
            mtime: self.mtime,
            compression: self.compression,
            size_compressed: self.size_compressed,
            size: self.size,
            file: Arc::clone(&self.file),
            decoder: None,
            offset: self.offset,
            consumed: 0,
        }
    }
}

impl Debug for FileEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileEntry")
            .field("name", &self.name)
            .field("mode", &self.mode)
            .field("owner", &self.owner)
            .field("mtime", &self.mtime)
            .field("offset", &self.offset)
            .field("compression", &self.compression)
            .field("size", &self.size)
            .field("size_compressed", &self.size_compressed)
            .finish()
    }
}

impl Read for FileEntry {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.consumed >= self.size {
            return Ok(0);
        }

        let remaining = self.size - self.consumed;

        match self.compression {
            CompressionFormat::None => {
                let bytes_read = self.file.read_at(self.offset + self.consumed, buf)?;

                if bytes_read > remaining as usize {
                    self.consumed += remaining;
                    return Ok(remaining as usize);
                }

                self.consumed += bytes_read as u64;
                Ok(bytes_read)
            }
            CompressionFormat::Gzip => {
                if self.decoder.is_none() {
                    let reader = BoundedReader {
                        file: Arc::clone(&self.file),
                        offset: self.offset,
                        position: 0,
                        compressed_size: self.size_compressed.unwrap(),
                    };

                    let decoder = Box::new(GzDecoder::new(reader));

                    self.decoder = Some(decoder);
                }

                let decoder = self.decoder.as_mut().unwrap();
                let bytes_read = decoder.read(buf)?;

                if bytes_read > remaining as usize {
                    self.decoder = None;
                    self.consumed += remaining;
                    return Ok(remaining as usize);
                }

                self.consumed += bytes_read as u64;
                Ok(bytes_read)
            }
            CompressionFormat::Deflate => {
                if self.decoder.is_none() {
                    let reader = BoundedReader {
                        file: Arc::clone(&self.file),
                        offset: self.offset,
                        position: 0,
                        compressed_size: self.size_compressed.unwrap(),
                    };

                    let decoder = Box::new(DeflateDecoder::new(reader));

                    self.decoder = Some(decoder);
                }

                let decoder = self.decoder.as_mut().unwrap();
                let bytes_read = decoder.read(buf)?;

                if bytes_read > remaining as usize {
                    self.decoder = None;
                    self.consumed += remaining;
                    return Ok(remaining as usize);
                }

                self.consumed += bytes_read as u64;
                Ok(bytes_read)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct DirectoryEntry {
    pub name: String,
    pub mode: Permissions,
    pub owner: (u32, u32),
    pub mtime: SystemTime,
    pub entries: Vec<Entry>,
}

#[derive(Clone, Debug)]
pub struct SymlinkEntry {
    pub name: String,
    pub mode: Permissions,
    pub owner: (u32, u32),
    pub mtime: SystemTime,
    pub target: String,
    pub target_dir: bool,
}

#[derive(Clone, Debug)]
pub enum Entry {
    File(Box<FileEntry>),
    Directory(Box<DirectoryEntry>),
    Symlink(Box<SymlinkEntry>),
}

impl Entry {
    /// Returns the name of the entry.
    /// This is the name of the file or directory, not the full path.
    /// For example, if the entry is under `path/to/file.txt`, this will return `file.txt`.
    #[inline]
    pub fn name(&self) -> &str {
        match self {
            Entry::File(entry) => &entry.name,
            Entry::Directory(entry) => &entry.name,
            Entry::Symlink(entry) => &entry.name,
        }
    }

    /// Returns the mode of the entry.
    /// This is the file permissions of the entry.
    #[inline]
    pub const fn mode(&self) -> &Permissions {
        match self {
            Entry::File(entry) => &entry.mode,
            Entry::Directory(entry) => &entry.mode,
            Entry::Symlink(entry) => &entry.mode,
        }
    }

    /// Returns the owner of the entry.
    /// This is the user ID and group ID of the entry.
    #[inline]
    pub const fn owner(&self) -> (u32, u32) {
        match self {
            Entry::File(entry) => entry.owner,
            Entry::Directory(entry) => entry.owner,
            Entry::Symlink(entry) => entry.owner,
        }
    }

    /// Returns the modification time of the entry.
    /// This is the time the entry was last modified.
    #[inline]
    pub const fn mtime(&self) -> SystemTime {
        match self {
            Entry::File(entry) => entry.mtime,
            Entry::Directory(entry) => entry.mtime,
            Entry::Symlink(entry) => entry.mtime,
        }
    }
}

struct BoundedReader {
    file: Arc<File>,
    offset: u64,
    position: u64,
    compressed_size: u64,
}

impl Read for BoundedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.position >= self.compressed_size {
            return Ok(0);
        }

        let remaining = self.compressed_size - self.position;
        let to_read = std::cmp::min(buf.len(), remaining as usize);

        let bytes_read = self
            .file
            .read_at(self.offset + self.position, &mut buf[0..to_read])?;
        self.position += bytes_read as u64;

        Ok(bytes_read)
    }
}
