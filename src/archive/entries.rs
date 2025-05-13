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
pub struct EntryMode(u32);

impl EntryMode {
    #[inline]
    pub const fn new(mode: u32) -> Self {
        Self(mode)
    }

    /// Returns the mode bits.
    #[inline]
    pub const fn bits(&self) -> u32 {
        self.0
    }

    /// Sets the mode bits.
    #[inline]
    pub const fn set_bits(&mut self, mode: u32) {
        self.0 = mode;
    }

    /// Returns the user permissions (read, write, execute).
    #[inline]
    pub const fn user(&self) -> (bool, bool, bool) {
        (
            self.0 & 0o400 != 0,
            self.0 & 0o200 != 0,
            self.0 & 0o100 != 0,
        )
    }

    /// Sets the user permissions (read, write, execute).
    #[inline]
    pub const fn set_user(&mut self, read: bool, write: bool, execute: bool) {
        self.0 &= !0o700;
        self.0 |= (read as u32) << 6 | (write as u32) << 5 | (execute as u32) << 4;
    }

    /// Returns the group permissions (read, write, execute).
    #[inline]
    pub const fn group(&self) -> (bool, bool, bool) {
        (
            self.0 & 0o040 != 0,
            self.0 & 0o020 != 0,
            self.0 & 0o010 != 0,
        )
    }

    /// Sets the group permissions (read, write, execute).
    #[inline]
    pub const fn set_group(&mut self, read: bool, write: bool, execute: bool) {
        self.0 &= !0o070;
        self.0 |= (read as u32) << 3 | (write as u32) << 2 | (execute as u32) << 1;
    }

    /// Returns the other permissions (read, write, execute).
    #[inline]
    pub const fn other(&self) -> (bool, bool, bool) {
        (
            self.0 & 0o004 != 0,
            self.0 & 0o002 != 0,
            self.0 & 0o001 != 0,
        )
    }

    /// Sets the other permissions (read, write, execute).
    #[inline]
    pub const fn set_other(&mut self, read: bool, write: bool, execute: bool) {
        self.0 &= !0o007;
        self.0 |= (read as u32) | (write as u32) << 1 | (execute as u32) << 2;
    }
}

impl Debug for EntryMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut mode = String::with_capacity(9);

        mode.push(if self.0 & 0o400 != 0 { 'r' } else { '-' });
        mode.push(if self.0 & 0o200 != 0 { 'w' } else { '-' });
        mode.push(if self.0 & 0o100 != 0 { 'x' } else { '-' });
        mode.push(if self.0 & 0o040 != 0 { 'r' } else { '-' });
        mode.push(if self.0 & 0o020 != 0 { 'w' } else { '-' });
        mode.push(if self.0 & 0o010 != 0 { 'x' } else { '-' });
        mode.push(if self.0 & 0o004 != 0 { 'r' } else { '-' });
        mode.push(if self.0 & 0o002 != 0 { 'w' } else { '-' });
        mode.push(if self.0 & 0o001 != 0 { 'x' } else { '-' });

        write!(f, "{} ({:o})", mode, self.0)
    }
}

impl Default for EntryMode {
    #[inline]
    fn default() -> Self {
        Self(0o644)
    }
}

impl From<u32> for EntryMode {
    #[inline]
    fn from(mode: u32) -> Self {
        Self(mode)
    }
}

impl From<EntryMode> for u32 {
    #[inline]
    fn from(mode: EntryMode) -> Self {
        mode.0
    }
}

impl From<std::fs::Permissions> for EntryMode {
    #[inline]
    fn from(permissions: std::fs::Permissions) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            Self(permissions.mode())
        }
        #[cfg(not(unix))]
        {
            Self(if permissions.readonly() { 0o444 } else { 0o666 })
        }
    }
}

impl From<EntryMode> for std::fs::Permissions {
    #[inline]
    fn from(permissions: EntryMode) -> std::fs::Permissions {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::Permissions::from_mode(permissions.0)
        }
        #[cfg(not(unix))]
        {
            let mut fs_permissions: std::fs::Permissions = unsafe { std::mem::zeroed() };
            fs_permissions.set_readonly(permissions.0 & 0o444 != 0);

            fs_permissions
        }
    }
}

impl Deref for EntryMode {
    type Target = u32;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct FileEntry {
    pub name: String,
    pub mode: EntryMode,
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
    pub mode: EntryMode,
    pub owner: (u32, u32),
    pub mtime: SystemTime,
    pub entries: Vec<Entry>,
}

#[derive(Clone, Debug)]
pub struct SymlinkEntry {
    pub name: String,
    pub mode: EntryMode,
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
    /// This also contains the file permissions of the entry.
    #[inline]
    pub const fn mode(&self) -> EntryMode {
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
