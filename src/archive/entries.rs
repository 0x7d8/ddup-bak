use super::CompressionFormat;
use positioned_io::ReadAt;
use std::{
    fmt::{Debug, Formatter},
    fs::File,
    io::Read,
    path::Path,
    sync::Arc,
    time::SystemTime,
};

/// Unix mode bits of an entry.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct EntryMode(u32);

impl EntryMode {
    pub const fn new(mode: u32) -> Self {
        Self(mode)
    }

    pub const fn bits(&self) -> u32 {
        self.0
    }

    /// Applies the mode to `path` without following symlinks (the path must not be a symlink).
    pub fn apply(self, path: &Path) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(self.0))
        }
        #[cfg(not(unix))]
        {
            let mut permissions = std::fs::metadata(path)?.permissions();
            permissions.set_readonly(self.0 & 0o200 == 0);
            std::fs::set_permissions(path, permissions)
        }
    }
}

impl Debug for EntryMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut mode = String::with_capacity(9);
        for (bit, symbol) in (0..9).rev().zip("rwxrwxrwx".chars()) {
            mode.push(if self.0 & (1 << bit) != 0 {
                symbol
            } else {
                '-'
            });
        }

        write!(f, "{} ({:o})", mode, self.0)
    }
}

impl Default for EntryMode {
    fn default() -> Self {
        Self(0o644)
    }
}

impl From<u32> for EntryMode {
    fn from(mode: u32) -> Self {
        Self(mode)
    }
}

impl From<EntryMode> for u32 {
    fn from(mode: EntryMode) -> Self {
        mode.0
    }
}

impl From<std::fs::Permissions> for EntryMode {
    fn from(permissions: std::fs::Permissions) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            Self(permissions.mode())
        }
        #[cfg(not(unix))]
        Self(if permissions.readonly() { 0o444 } else { 0o644 })
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
    pub decoder: Option<Box<dyn Read + Send + Sync>>,
    pub consumed: u64,
}

impl FileEntry {
    fn decoder(&mut self) -> std::io::Result<&mut (dyn Read + Send + Sync)> {
        if self.decoder.is_none() {
            let size = self.size_compressed.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "compressed entry without compressed size",
                )
            })?;

            let reader = BoundedReader {
                file: Arc::clone(&self.file),
                offset: self.offset,
                size,
                position: 0,
            };
            self.decoder = Some(super::decompressor(self.compression, reader)?);
        }

        Ok(self.decoder.as_mut().unwrap().as_mut())
    }
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
            offset: self.offset,
            decoder: None,
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
        let remaining = usize::try_from(self.size - self.consumed).unwrap_or(usize::MAX);
        let len = buf.len().min(remaining);
        let buf = &mut buf[..len];
        if buf.is_empty() {
            return Ok(0);
        }

        let bytes_read = match self.compression {
            CompressionFormat::None => self.file.read_at(self.offset + self.consumed, buf)?,
            _ => self.decoder()?.read(buf)?,
        };

        if bytes_read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "entry data ends before its declared size",
            ));
        }

        self.consumed += bytes_read as u64;
        Ok(bytes_read)
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
    pub fn name(&self) -> &str {
        match self {
            Entry::File(entry) => &entry.name,
            Entry::Directory(entry) => &entry.name,
            Entry::Symlink(entry) => &entry.name,
        }
    }

    pub const fn mode(&self) -> EntryMode {
        match self {
            Entry::File(entry) => entry.mode,
            Entry::Directory(entry) => entry.mode,
            Entry::Symlink(entry) => entry.mode,
        }
    }

    pub const fn owner(&self) -> (u32, u32) {
        match self {
            Entry::File(entry) => entry.owner,
            Entry::Directory(entry) => entry.owner,
            Entry::Symlink(entry) => entry.owner,
        }
    }

    pub const fn mtime(&self) -> SystemTime {
        match self {
            Entry::File(entry) => entry.mtime,
            Entry::Directory(entry) => entry.mtime,
            Entry::Symlink(entry) => entry.mtime,
        }
    }

    pub const fn is_file(&self) -> bool {
        matches!(self, Entry::File(_))
    }

    pub const fn is_directory(&self) -> bool {
        matches!(self, Entry::Directory(_))
    }

    pub const fn is_symlink(&self) -> bool {
        matches!(self, Entry::Symlink(_))
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
        let remaining = usize::try_from(self.size - self.position).unwrap_or(usize::MAX);
        let to_read = buf.len().min(remaining);
        if to_read == 0 {
            return Ok(0);
        }

        let bytes_read = self
            .file
            .read_at(self.offset + self.position, &mut buf[..to_read])?;
        self.position += bytes_read as u64;

        Ok(bytes_read)
    }
}
