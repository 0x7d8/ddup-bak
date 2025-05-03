use super::CompressionFormat;
use flate2::read::{DeflateDecoder, GzDecoder};
use positioned_io::ReadAt;
use std::{
    fmt::{Debug, Formatter},
    fs::File,
    io::Read,
    ops::Deref,
    sync::Arc,
    time::SystemTime,
};

#[derive(Clone, Copy)]
pub struct Permissions(u16);

impl Permissions {
    #[inline]
    pub fn new(mode: u16) -> Self {
        Self(mode)
    }

    #[inline]
    pub fn bits(&self) -> u16 {
        self.0
    }

    /// Returns the user permissions (read, write, execute).
    #[inline]
    pub fn user(&self) -> (bool, bool, bool) {
        (
            self.0 & 0o400 != 0,
            self.0 & 0o200 != 0,
            self.0 & 0o100 != 0,
        )
    }

    /// Returns the group permissions (read, write, execute).
    #[inline]
    pub fn group(&self) -> (bool, bool, bool) {
        (
            self.0 & 0o040 != 0,
            self.0 & 0o020 != 0,
            self.0 & 0o010 != 0,
        )
    }

    /// Returns the other permissions (read, write, execute).
    #[inline]
    pub fn other(&self) -> (bool, bool, bool) {
        (
            self.0 & 0o004 != 0,
            self.0 & 0o002 != 0,
            self.0 & 0o001 != 0,
        )
    }
}

impl Debug for Permissions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut mode = String::with_capacity(9);

        let (user_r, user_w, user_x) = self.user();
        let (group_r, group_w, group_x) = self.group();
        let (other_r, other_w, other_x) = self.other();

        mode.push(if user_r { 'r' } else { '-' });
        mode.push(if user_w { 'w' } else { '-' });
        mode.push(if user_x { 'x' } else { '-' });
        mode.push(if group_r { 'r' } else { '-' });
        mode.push(if group_w { 'w' } else { '-' });
        mode.push(if group_x { 'x' } else { '-' });
        mode.push(if other_r { 'r' } else { '-' });
        mode.push(if other_w { 'w' } else { '-' });
        mode.push(if other_x { 'x' } else { '-' });

        f.debug_struct("Permissions").field("mode", &mode).finish()
    }
}

impl Default for Permissions {
    #[inline]
    fn default() -> Self {
        Self(0o644)
    }
}

impl From<u32> for Permissions {
    #[inline]
    fn from(mode: u32) -> Self {
        Self(mode as u16)
    }
}

impl From<u16> for Permissions {
    #[inline]
    fn from(mode: u16) -> Self {
        Self(mode)
    }
}

impl From<std::fs::Permissions> for Permissions {
    #[inline]
    fn from(permissions: std::fs::Permissions) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            Self((permissions.mode() & 0o777) as u16)
        }
        #[cfg(not(unix))]
        {
            Self(if permissions.readonly() { 0o444 } else { 0o666 })
        }
    }
}

impl From<Permissions> for std::fs::Permissions {
    #[inline]
    fn from(permissions: Permissions) -> std::fs::Permissions {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::Permissions::from_mode(permissions.0 as u32)
        }
        #[cfg(not(unix))]
        {
            let mut permissions: std::fs::Permissions = unsafe { std::mem::zeroed() };
            permissions.set_readonly(permissions.0 & 0o444 != 0);

            permissions
        }
    }
}

impl Deref for Permissions {
    type Target = u16;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct FileEntry {
    pub name: String,
    pub mode: Permissions,
    pub owner: (u32, u32),
    pub mtime: SystemTime,

    pub compression: CompressionFormat,
    pub size_compressed: Option<u64>,
    pub size_real: u64,
    pub size: u64,

    pub file: Arc<File>,
    pub offset: u64,
    pub decoder: Option<Box<dyn Read + Send>>,
    pub consumed: u64,
}

impl Clone for FileEntry {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            mode: self.mode,
            owner: self.owner,
            mtime: self.mtime,
            compression: self.compression,
            size_compressed: self.size_compressed,
            size_real: self.size_real,
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
            .field("size_real", &self.size_real)
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
                        size: self.size_compressed.unwrap(),
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
                        size: self.size_compressed.unwrap(),
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

            #[cfg(feature = "brotli")]
            CompressionFormat::Brotli => {
                if self.decoder.is_none() {
                    let reader = BoundedReader {
                        file: Arc::clone(&self.file),
                        offset: self.offset,
                        position: 0,
                        size: self.size_compressed.unwrap(),
                    };

                    let decoder = Box::new(brotli::Decompressor::new(reader, 4096));
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
            #[cfg(not(feature = "brotli"))]
            CompressionFormat::Brotli => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Brotli support is not enabled. Please enable the 'brotli' feature.",
            )),
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
    pub const fn mode(&self) -> Permissions {
        match self {
            Entry::File(entry) => entry.mode,
            Entry::Directory(entry) => entry.mode,
            Entry::Symlink(entry) => entry.mode,
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
    size: u64,
    position: u64,
}

impl Read for BoundedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.position >= self.size {
            return Ok(0);
        }

        let remaining = self.size - self.position;
        let to_read = std::cmp::min(buf.len(), remaining as usize);

        let bytes_read = self
            .file
            .read_at(self.offset + self.position, &mut buf[..to_read])?;
        self.position += bytes_read as u64;

        Ok(bytes_read)
    }
}
