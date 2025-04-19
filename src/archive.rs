use crate::varint;
use flate2::{
    read::{DeflateDecoder, GzDecoder},
    write::{DeflateEncoder, GzEncoder},
};
use positioned_io::ReadAt;
use std::{
    ffi::OsStr,
    fmt::{Debug, Formatter},
    fs::{DirEntry, File, Permissions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
    time::SystemTime,
};

pub const FILE_SIGNATURE: [u8; 7] = *b"DDUPBAK";
pub const FILE_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy)]
pub enum CompressionFormat {
    None,
    Gzip,
    Deflate,
}

impl CompressionFormat {
    pub fn encode(&self) -> u8 {
        match self {
            CompressionFormat::None => 0,
            CompressionFormat::Gzip => 1,
            CompressionFormat::Deflate => 2,
        }
    }

    pub fn decode(value: u8) -> Self {
        match value {
            0 => CompressionFormat::None,
            1 => CompressionFormat::Gzip,
            2 => CompressionFormat::Deflate,
            _ => panic!("Invalid compression format"),
        }
    }
}

#[inline]
fn encode_file_permissions(permissions: Permissions) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        permissions.mode()
    }
    #[cfg(windows)]
    {
        if permissions.readonly() { 1 } else { 0 }
    }
}
#[inline]
fn decode_file_permissions(mode: u32) -> Permissions {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        Permissions::from_mode(mode)
    }
    #[cfg(windows)]
    {
        let mut permissions = unsafe { std::mem::zeroed::<Permissions>() };
        if mode == 1 {
            permissions.set_readonly(true);
        } else {
            permissions.set_readonly(false);
        }

        permissions
    }
}

#[inline]
fn metadata_owner(_metadata: &std::fs::Metadata) -> (u32, u32) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        (_metadata.uid(), _metadata.gid())
    }
    #[cfg(windows)]
    {
        (0, 0)
    }
}

pub struct FileEntry {
    pub name: String,
    pub mode: Permissions,
    pub owner: (u32, u32),
    pub mtime: SystemTime,

    pub compression: CompressionFormat,
    pub size: u64,

    file: Arc<File>,
    decoder: Option<Box<dyn Read + Send>>,
    offset: u64,
    consumed: u64,
}

impl Clone for FileEntry {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            mode: self.mode.clone(),
            owner: self.owner,
            mtime: self.mtime,
            compression: self.compression,
            size: self.size,
            file: self.file.clone(),
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
                    self.file
                        .seek(SeekFrom::Start(self.offset + self.consumed))?;
                    let decoder = GzDecoder::new(self.file.try_clone()?);
                    self.decoder = Some(Box::new(decoder));
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
                    self.file
                        .seek(SeekFrom::Start(self.offset + self.consumed))?;
                    let decoder = DeflateDecoder::new(self.file.try_clone()?);
                    self.decoder = Some(Box::new(decoder));
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
    pub fn name(&self) -> &str {
        match self {
            Entry::File(entry) => &entry.name,
            Entry::Directory(entry) => &entry.name,
            Entry::Symlink(entry) => &entry.name,
        }
    }

    /// Returns the mode of the entry.
    /// This is the file permissions of the entry.
    pub fn mode(&self) -> Permissions {
        match self {
            Entry::File(entry) => entry.mode.clone(),
            Entry::Directory(entry) => entry.mode.clone(),
            Entry::Symlink(entry) => entry.mode.clone(),
        }
    }

    /// Returns the owner of the entry.
    /// This is the user ID and group ID of the entry.
    pub fn owner(&self) -> (u32, u32) {
        match self {
            Entry::File(entry) => entry.owner,
            Entry::Directory(entry) => entry.owner,
            Entry::Symlink(entry) => entry.owner,
        }
    }

    /// Returns the modification time of the entry.
    /// This is the time the entry was last modified.
    pub fn mtime(&self) -> SystemTime {
        match self {
            Entry::File(entry) => entry.mtime,
            Entry::Directory(entry) => entry.mtime,
            Entry::Symlink(entry) => entry.mtime,
        }
    }
}

pub type ProgressCallback = Option<fn(&std::path::PathBuf)>;
type CompressionFormatCallback =
    Option<fn(&std::path::PathBuf, &std::fs::Metadata) -> CompressionFormat>;

pub struct Archive {
    file: Arc<File>,
    version: u8,
    compression_callback: CompressionFormatCallback,

    entries: Vec<Entry>,
    entries_offset: u64,
}

impl Debug for Archive {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Archive")
            .field("version", &self.version)
            .field("entries", &self.entries)
            .finish()
    }
}

impl Archive {
    /// Creates a new archive file.
    /// The file signature is written to the beginning of the file.
    /// The file is truncated to 0 bytes.
    pub fn new(mut file: File) -> Self {
        file.set_len(0).unwrap();
        file.write_all(&FILE_SIGNATURE).unwrap();
        file.write_all(&[FILE_VERSION]).unwrap();
        file.sync_all().unwrap();

        Self {
            file: Arc::new(file),
            version: FILE_VERSION,
            compression_callback: None,
            entries: Vec::new(),
            entries_offset: 8,
        }
    }

    /// Opens an existing archive file for reading and writing.
    /// This will not overwrite the file, but append to it.
    pub fn open(path: &str) -> Result<Self, std::io::Error> {
        let mut file = File::open(path)?;
        let len = file.metadata()?.len();

        let mut buffer = [0; 8];
        file.read_exact(&mut buffer)?;
        if !buffer.starts_with(&FILE_SIGNATURE) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid file signature",
            ));
        }
        let version = buffer[7];

        file.read_exact_at(len - 16, &mut buffer)?;
        let entries_count = u64::from_le_bytes(buffer);
        file.read_exact_at(len - 8, &mut buffer)?;
        let entries_offset = u64::from_le_bytes(buffer);

        let mut entries = Vec::with_capacity(entries_count as usize);
        file.seek(SeekFrom::Start(entries_offset))?;

        let mut decoder = DeflateDecoder::new(file.try_clone()?);
        let file = Arc::new(file);
        for _ in 0..entries_count {
            let entry = Self::decode_entry(&mut decoder, file.clone())?;
            entries.push(entry);
        }

        Ok(Self {
            file,
            version,
            compression_callback: None,
            entries,
            entries_offset,
        })
    }

    /// Sets the compression callback for the archive.
    /// This callback is called for each added file entry in the archive.
    /// The callback should return the compression format to use for the file.
    pub fn set_compression_callback(&mut self, callback: CompressionFormatCallback) -> &mut Self {
        self.compression_callback = callback;

        self
    }

    /// Adds all files in the given directory to the archive. (including subdirectories)
    /// This will append the directory to the end of the archive, if this directory already exists, it will not be replaced.
    ///
    /// After this function is called, the existing header will be trimmed to the end of the archive, then readded upon completion.
    ///
    /// # Panics
    /// This function will panic if any filename is not valid UTF-8 or longer than 255 bytes.
    pub fn add_directory(
        &mut self,
        path: &str,
        progress: ProgressCallback,
    ) -> Result<&mut Self, std::io::Error> {
        self.trim_end_header()?;

        for entry in std::fs::read_dir(path)?.flatten() {
            self.encode_entry(None, entry, progress)?;
        }

        self.write_end_header()?;

        Ok(self)
    }

    /// Returns the entries in the archive.
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// Consumes the archive and returns the entries.
    pub fn into_entries(self) -> Vec<Entry> {
        self.entries
    }

    /// Adds a single file entry to the archive. (including subdirectories)
    /// This will append the entry to the end of the archive, if this entry already exists, it will not be replaced.
    ///
    /// After this function is called, the existing header will be trimmed to the end of the archive, then readded upon completion.
    ///
    /// # Panics
    /// This function will panic if any filename is not valid UTF-8 or longer than 255 bytes.
    pub fn add_entries(
        &mut self,
        entries: Vec<DirEntry>,
        progress: ProgressCallback,
    ) -> Result<&mut Self, std::io::Error> {
        self.trim_end_header()?;

        for entry in entries {
            self.encode_entry(None, entry, progress)?;
        }

        self.write_end_header()?;

        Ok(self)
    }

    fn recursive_find_archive_entry<'a>(
        entry: &'a crate::archive::Entry,
        entry_parts: &[&OsStr],
    ) -> std::io::Result<Option<&'a crate::archive::Entry>> {
        if entry_parts.is_empty() {
            return Ok(None);
        }

        if Some(entry.name()) == entry_parts.last().map(|s| s.to_string_lossy()).as_deref() {
            return Ok(Some(entry));
        }

        if let crate::archive::Entry::Directory(dir_entry) = entry {
            for sub_entry in &dir_entry.entries {
                if let Some(found) =
                    Self::recursive_find_archive_entry(sub_entry, &entry_parts[1..])?
                {
                    return Ok(Some(found));
                }
            }
        }

        Ok(None)
    }

    /// Finds an entry in the archive by name.
    /// Returns `None` if the entry is not found.
    /// The entry name is the path inside the archive.
    /// Example: "world/user/level.dat" would be a valid entry name.
    pub fn find_archive_entry(
        &self,
        entry_name: &Path,
    ) -> std::io::Result<Option<&crate::archive::Entry>> {
        let entry_parts = entry_name
            .components()
            .map(|c| c.as_os_str())
            .collect::<Vec<&OsStr>>();
        for entry in self.entries() {
            if let Some(found) = Self::recursive_find_archive_entry(entry, &entry_parts)? {
                return Ok(Some(found));
            }
        }

        Ok(None)
    }

    fn trim_end_header(&mut self) -> Result<(), std::io::Error> {
        if self.entries_offset == 0 {
            return Ok(());
        }

        self.file.set_len(self.entries_offset)?;
        self.file.flush()?;
        self.file.seek(SeekFrom::Start(self.entries_offset))?;

        Ok(())
    }

    fn write_end_header(&mut self) -> Result<(), std::io::Error> {
        let mut encoder = DeflateEncoder::new(&mut self.file, flate2::Compression::default());
        for entry in &self.entries {
            Self::encode_entry_metadata(&mut encoder, entry)?;
        }

        encoder.flush()?;
        encoder.finish()?;
        self.file.flush()?;

        self.file
            .write_all(&(self.entries.len() as u64).to_le_bytes())?;
        self.file.write_all(&self.entries_offset.to_le_bytes())?;
        self.file.flush()?;
        self.file.sync_all()?;

        Ok(())
    }

    fn encode_entry_metadata<S: Write>(
        writer: &mut S,
        entry: &Entry,
    ) -> Result<(), std::io::Error> {
        let name = entry.name();
        let name_length = name.len() as u8;

        let mut buffer = Vec::with_capacity(1 + name.len() + 4);

        buffer.push(name_length);
        buffer.extend_from_slice(name.as_bytes());

        let mode = encode_file_permissions(entry.mode());
        let compression = match entry {
            Entry::File(file_entry) => file_entry.compression,
            _ => CompressionFormat::None,
        };
        let entry_type = match entry {
            Entry::File(_) => 0,
            Entry::Directory(_) => 1,
            Entry::Symlink(_) => 2,
        };

        let type_compression_mode =
            (entry_type << 30) | ((compression.encode() as u32) << 26) | (mode & 0x3FFFFFFF);
        buffer.extend_from_slice(&type_compression_mode.to_le_bytes()[..4]);

        writer.write_all(&buffer)?;

        let (uid, gid) = entry.owner();
        writer.write_all(&varint::encode_u32(uid))?;
        writer.write_all(&varint::encode_u32(gid))?;

        let mtime = entry
            .mtime()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        writer.write_all(&varint::encode_u64(mtime.as_secs()))?;

        match entry {
            Entry::File(file_entry) => {
                writer.write_all(&varint::encode_u64(file_entry.size))?;
                writer.write_all(&varint::encode_u64(file_entry.offset))?;
            }
            Entry::Directory(dir_entry) => {
                writer.write_all(&varint::encode_u64(dir_entry.entries.len() as u64))?;

                for sub_entry in &dir_entry.entries {
                    Self::encode_entry_metadata(writer, sub_entry)?;
                }
            }
            Entry::Symlink(link_entry) => {
                writer.write_all(&varint::encode_u64(link_entry.target.len() as u64))?;
                writer.write_all(link_entry.target.as_bytes())?;
                writer.write_all(&[link_entry.target_dir as u8])?;
            }
        }

        Ok(())
    }

    fn encode_entry(
        &mut self,
        entries: Option<&mut Vec<Entry>>,
        fs_entry: DirEntry,
        progress: ProgressCallback,
    ) -> Result<(), std::io::Error> {
        let path = fs_entry.path();

        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        let metadata = path.symlink_metadata()?;

        if metadata.is_file() {
            let mut file = File::open(&path)?;
            let mut buffer = [0; 4096];
            let mut bytes_read = file.read(&mut buffer)?;

            let compression = self.compression_callback.map_or(
                if metadata.len() > 16 {
                    CompressionFormat::Deflate
                } else {
                    CompressionFormat::None
                },
                |f| f(&path, &metadata),
            );

            match compression {
                CompressionFormat::None => {
                    loop {
                        self.file.write_all(&buffer[..bytes_read])?;

                        bytes_read = file.read(&mut buffer)?;
                        if bytes_read == 0 {
                            break;
                        }
                    }

                    self.file.flush()?;
                }
                CompressionFormat::Gzip => {
                    let mut encoder =
                        GzEncoder::new(&mut self.file, flate2::Compression::default());
                    loop {
                        encoder.write_all(&buffer[..bytes_read])?;

                        bytes_read = file.read(&mut buffer)?;
                        if bytes_read == 0 {
                            break;
                        }
                    }

                    encoder.flush()?;
                    encoder.finish()?;
                }
                CompressionFormat::Deflate => {
                    let mut encoder =
                        DeflateEncoder::new(&mut self.file, flate2::Compression::default());
                    loop {
                        encoder.write_all(&buffer[..bytes_read])?;

                        bytes_read = file.read(&mut buffer)?;
                        if bytes_read == 0 {
                            break;
                        }
                    }

                    encoder.flush()?;
                    encoder.finish()?;
                }
            }

            let entry = FileEntry {
                name: file_name,
                mode: metadata.permissions(),
                file: self.file.clone(),
                owner: metadata_owner(&metadata),
                mtime: metadata.modified()?,
                decoder: None,
                size: metadata.len(),
                offset: self.entries_offset,
                consumed: 0,
                compression,
            };

            self.entries_offset = self.file.stream_position()?;

            if let Some(entries) = entries {
                entries.push(Entry::File(Box::new(entry)));
            } else {
                self.entries.push(Entry::File(Box::new(entry)));
            }
        } else if metadata.is_dir() {
            let mut dir_entries = Vec::new();
            for entry in std::fs::read_dir(&path)?.flatten() {
                self.encode_entry(Some(&mut dir_entries), entry, progress)?;
            }

            let dir_entry = DirectoryEntry {
                name: file_name,
                mode: metadata.permissions(),
                owner: metadata_owner(&metadata),
                mtime: metadata.modified()?,
                entries: dir_entries,
            };

            if let Some(entries) = entries {
                entries.push(Entry::Directory(Box::new(dir_entry)));
            } else {
                self.entries.push(Entry::Directory(Box::new(dir_entry)));
            }
        } else if metadata.is_symlink() {
            if let Ok(Ok(target)) = std::fs::read_link(&path).map(|p| p.canonicalize()) {
                let target = target.to_string_lossy().to_string();

                let link_entry = SymlinkEntry {
                    name: file_name,
                    mode: metadata.permissions(),
                    owner: metadata_owner(&metadata),
                    mtime: metadata.modified()?,
                    target,
                    target_dir: std::fs::metadata(&path)?.is_dir(),
                };

                if let Some(entries) = entries {
                    entries.push(Entry::Symlink(Box::new(link_entry)));
                } else {
                    self.entries.push(Entry::Symlink(Box::new(link_entry)));
                }
            }
        }

        if let Some(f) = progress {
            f(&path)
        }

        Ok(())
    }

    fn decode_entry<S: Read>(decoder: &mut S, file: Arc<File>) -> Result<Entry, std::io::Error> {
        let mut name_length = [0; 1];
        decoder.read_exact(&mut name_length)?;
        let name_length = name_length[0] as usize;

        let mut name_bytes = vec![0; name_length];
        decoder.read_exact(&mut name_bytes)?;
        let name = String::from_utf8(name_bytes).unwrap();

        let mut type_mode_bytes = [0; 4];
        decoder.read_exact(&mut type_mode_bytes)?;
        let type_compression_mode = u32::from_le_bytes(type_mode_bytes);

        let entry_type = (type_compression_mode >> 30) & 0b11;
        let compression = CompressionFormat::decode(((type_compression_mode >> 26) & 0b1111) as u8);
        let mode = decode_file_permissions(type_compression_mode & 0x3FFFFFFF);

        let uid = varint::decode_u32(decoder);
        let gid = varint::decode_u32(decoder);

        let mtime = varint::decode_u64(decoder);
        let mtime = SystemTime::UNIX_EPOCH + std::time::Duration::new(mtime, 0);

        let size = varint::decode_u64(decoder);

        match entry_type {
            0 => {
                let offset = varint::decode_u64(decoder);

                Ok(Entry::File(Box::new(FileEntry {
                    name,
                    mode,
                    owner: (uid, gid),
                    mtime,
                    file,
                    decoder: None,
                    size,
                    offset,
                    consumed: 0,
                    compression,
                })))
            }
            1 => {
                let mut entries: Vec<Entry> = Vec::with_capacity(size as usize);
                for _ in 0..size {
                    let entry = Self::decode_entry(decoder, file.clone())?;
                    entries.push(entry);
                }

                Ok(Entry::Directory(Box::new(DirectoryEntry {
                    name,
                    mode,
                    owner: (uid, gid),
                    mtime,
                    entries,
                })))
            }
            2 => {
                let mut target_bytes = vec![0; size as usize];
                decoder.read_exact(&mut target_bytes)?;

                let target = String::from_utf8(target_bytes).unwrap();

                let mut target_dir_bytes = [0; 1];
                decoder.read_exact(&mut target_dir_bytes)?;
                let target_dir = target_dir_bytes[0] != 0;

                Ok(Entry::Symlink(Box::new(SymlinkEntry {
                    name,
                    mode,
                    owner: (uid, gid),
                    mtime,
                    target,
                    target_dir,
                })))
            }
            _ => panic!("Unsupported entry type"),
        }
    }
}
