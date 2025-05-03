use crate::varint;
use entries::Permissions;
use flate2::{
    read::DeflateDecoder,
    write::{DeflateEncoder, GzEncoder},
};
use positioned_io::ReadAt;
use std::{
    ffi::OsStr,
    fmt::{Debug, Formatter},
    fs::{DirEntry, File, Metadata},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
    time::SystemTime,
};

pub mod entries;

pub const FILE_SIGNATURE: [u8; 7] = *b"DDUPBAK";
pub const FILE_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionFormat {
    None,
    Gzip,
    Deflate,
    Brotli,
}

impl CompressionFormat {
    pub fn encode(&self) -> u8 {
        match self {
            CompressionFormat::None => 0,
            CompressionFormat::Gzip => 1,
            CompressionFormat::Deflate => 2,
            CompressionFormat::Brotli => 3,
        }
    }

    pub fn decode(value: u8) -> Self {
        match value {
            0 => CompressionFormat::None,
            1 => CompressionFormat::Gzip,
            2 => CompressionFormat::Deflate,
            3 => CompressionFormat::Brotli,
            _ => panic!("Invalid compression format"),
        }
    }
}

#[inline]
fn metadata_owner(_metadata: &Metadata) -> (u32, u32) {
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

pub type ProgressCallback = Option<Arc<dyn Fn(&Path) + Send + Sync + 'static>>;
type CompressionFormatCallback =
    Option<Arc<dyn Fn(&Path, &Metadata) -> CompressionFormat + Send + Sync + 'static>>;
type RealSizeCallback = Option<Arc<dyn Fn(&Path) -> u64 + Send + Sync + 'static>>;

pub struct Archive {
    file: Arc<File>,
    version: u8,
    compression_callback: CompressionFormatCallback,
    real_size_callback: RealSizeCallback,

    entries: Vec<entries::Entry>,
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
            real_size_callback: None,
            entries: Vec::new(),
            entries_offset: 8,
        }
    }

    /// Opens an existing archive file for reading and writing.
    /// This will not overwrite the file, but append to it.
    pub fn open(path: &str) -> std::io::Result<Self> {
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
            real_size_callback: None,
            entries,
            entries_offset,
        })
    }

    /// Sets the compression callback for the archive.
    /// This callback is called for each added file entry in the archive.
    /// The callback should return the compression format to use for the file.
    #[inline]
    pub fn set_compression_callback(&mut self, callback: CompressionFormatCallback) -> &mut Self {
        self.compression_callback = callback;

        self
    }

    /// Sets the "real" size callback for the archive.
    /// This callback is called for each added file entry in the archive.
    /// The callback should return the "real" size of the file.
    #[inline]
    pub fn set_real_size_callback(&mut self, callback: RealSizeCallback) -> &mut Self {
        self.real_size_callback = callback;

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
    ) -> std::io::Result<&mut Self> {
        self.trim_end_header()?;

        for entry in std::fs::read_dir(path)?.flatten() {
            self.encode_entry(None, entry, progress.clone())?;
        }

        self.write_end_header()?;

        Ok(self)
    }

    /// Returns the entries in the archive.
    #[inline]
    pub fn entries(&self) -> &[entries::Entry] {
        &self.entries
    }

    /// Consumes the archive and returns the entries.
    #[inline]
    pub fn into_entries(self) -> Vec<entries::Entry> {
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
    ) -> std::io::Result<&mut Self> {
        self.trim_end_header()?;

        for entry in entries {
            self.encode_entry(None, entry, progress.clone())?;
        }

        self.write_end_header()?;

        Ok(self)
    }

    fn recursive_find_archive_entry<'a>(
        entry: &'a entries::Entry,
        entry_parts: &[&OsStr],
    ) -> std::io::Result<Option<&'a entries::Entry>> {
        if entry_parts.is_empty() {
            return Ok(None);
        }

        if Some(entry.name()) == entry_parts.last().map(|s| s.to_string_lossy()).as_deref() {
            return Ok(Some(entry));
        }

        if let entries::Entry::Directory(dir_entry) = entry {
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
    ) -> std::io::Result<Option<&entries::Entry>> {
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

    fn trim_end_header(&mut self) -> std::io::Result<()> {
        if self.entries_offset == 0 {
            return Ok(());
        }

        self.file.set_len(self.entries_offset)?;
        self.file.flush()?;
        self.file.seek(SeekFrom::Start(self.entries_offset))?;

        Ok(())
    }

    fn write_end_header(&mut self) -> std::io::Result<()> {
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
        entry: &entries::Entry,
    ) -> std::io::Result<()> {
        let name = entry.name();
        let name_length = name.len() as u8;

        writer.write_all(&varint::encode_u32(name_length as u32))?;

        let mut buffer = Vec::with_capacity(name.len() + 4);
        buffer.extend_from_slice(name.as_bytes());

        let mode = entry.mode().bits() as u32;
        let compression = match entry {
            entries::Entry::File(file_entry) => file_entry.compression,
            _ => CompressionFormat::None,
        };
        let entry_type = match entry {
            entries::Entry::File(_) => 0,
            entries::Entry::Directory(_) => 1,
            entries::Entry::Symlink(_) => 2,
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
            entries::Entry::File(file_entry) => {
                writer.write_all(&varint::encode_u64(file_entry.size))?;

                if let Some(size_compressed) = file_entry.size_compressed {
                    writer.write_all(&varint::encode_u64(size_compressed))?;
                }
                writer.write_all(&varint::encode_u64(file_entry.size_real))?;

                writer.write_all(&varint::encode_u64(file_entry.offset))?;
            }
            entries::Entry::Directory(dir_entry) => {
                writer.write_all(&varint::encode_u64(dir_entry.entries.len() as u64))?;

                for sub_entry in &dir_entry.entries {
                    Self::encode_entry_metadata(writer, sub_entry)?;
                }
            }
            entries::Entry::Symlink(link_entry) => {
                writer.write_all(&varint::encode_u64(link_entry.target.len() as u64))?;
                writer.write_all(link_entry.target.as_bytes())?;
                writer.write_all(&[link_entry.target_dir as u8])?;
            }
        }

        Ok(())
    }

    fn encode_entry(
        &mut self,
        entries: Option<&mut Vec<entries::Entry>>,
        fs_entry: DirEntry,
        progress: ProgressCallback,
    ) -> std::io::Result<()> {
        let path = fs_entry.path();

        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        let metadata = path.symlink_metadata()?;

        if metadata.is_file() {
            let mut file = File::open(&path)?;
            let mut buffer = [0; 4096];
            let mut bytes_read = file.read(&mut buffer)?;

            let compression = match self.compression_callback {
                Some(ref f) => f(&path, &metadata),
                None => {
                    if metadata.len() > 16 {
                        CompressionFormat::Deflate
                    } else {
                        CompressionFormat::None
                    }
                }
            };

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

                #[cfg(feature = "brotli")]
                CompressionFormat::Brotli => {
                    let mut encoder = brotli::CompressorWriter::new(&mut self.file, 4096, 11, 22);
                    loop {
                        encoder.write_all(&buffer[..bytes_read])?;

                        bytes_read = file.read(&mut buffer)?;
                        if bytes_read == 0 {
                            break;
                        }
                    }
                }
                #[cfg(not(feature = "brotli"))]
                CompressionFormat::Brotli => {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "Brotli support is not enabled. Please enable the 'brotli' feature.",
                    ))?;
                }
            }

            let entry = entries::FileEntry {
                name: file_name,
                mode: metadata.permissions().into(),
                file: self.file.clone(),
                owner: metadata_owner(&metadata),
                mtime: metadata.modified()?,
                decoder: None,
                size_compressed: match compression {
                    CompressionFormat::None => None,
                    _ => Some(self.file.stream_position()? - self.entries_offset),
                },
                size_real: match self.real_size_callback {
                    Some(ref f) => f(&path),
                    None => metadata.len(),
                },
                size: metadata.len(),
                offset: self.entries_offset,
                consumed: 0,
                compression,
            };

            self.entries_offset = self.file.stream_position()?;

            if let Some(entries) = entries {
                entries.push(entries::Entry::File(Box::new(entry)));
            } else {
                self.entries.push(entries::Entry::File(Box::new(entry)));
            }
        } else if metadata.is_dir() {
            let mut dir_entries = Vec::new();
            for entry in std::fs::read_dir(&path)?.flatten() {
                self.encode_entry(Some(&mut dir_entries), entry, progress.clone())?;
            }

            let dir_entry = entries::DirectoryEntry {
                name: file_name,
                mode: metadata.permissions().into(),
                owner: metadata_owner(&metadata),
                mtime: metadata.modified()?,
                entries: dir_entries,
            };

            if let Some(entries) = entries {
                entries.push(entries::Entry::Directory(Box::new(dir_entry)));
            } else {
                self.entries
                    .push(entries::Entry::Directory(Box::new(dir_entry)));
            }
        } else if metadata.is_symlink() {
            if let Ok(Ok(target)) = std::fs::read_link(&path).map(|p| p.canonicalize()) {
                let target = target.to_string_lossy().to_string();

                let link_entry = entries::SymlinkEntry {
                    name: file_name,
                    mode: metadata.permissions().into(),
                    owner: metadata_owner(&metadata),
                    mtime: metadata.modified()?,
                    target,
                    target_dir: std::fs::metadata(&path)?.is_dir(),
                };

                if let Some(entries) = entries {
                    entries.push(entries::Entry::Symlink(Box::new(link_entry)));
                } else {
                    self.entries
                        .push(entries::Entry::Symlink(Box::new(link_entry)));
                }
            }
        }

        if let Some(f) = progress.clone() {
            f(&path)
        }

        Ok(())
    }

    fn decode_entry<S: Read>(decoder: &mut S, file: Arc<File>) -> std::io::Result<entries::Entry> {
        let name_length = varint::decode_u32(decoder) as usize;

        let mut name_bytes = vec![0; name_length];
        decoder.read_exact(&mut name_bytes)?;
        let name = String::from_utf8(name_bytes).unwrap();

        let mut type_mode_bytes = [0; 4];
        decoder.read_exact(&mut type_mode_bytes)?;
        let type_compression_mode = u32::from_le_bytes(type_mode_bytes);

        let entry_type = (type_compression_mode >> 30) & 0b11;
        let compression = CompressionFormat::decode(((type_compression_mode >> 26) & 0b1111) as u8);
        let mode = Permissions::from(type_compression_mode & 0x3FFFFFFF);

        let uid = varint::decode_u32(decoder);
        let gid = varint::decode_u32(decoder);

        let mtime = varint::decode_u64(decoder);
        let mtime = SystemTime::UNIX_EPOCH + std::time::Duration::new(mtime, 0);

        let size = varint::decode_u64(decoder);

        match entry_type {
            0 => {
                let size_compressed = match compression {
                    CompressionFormat::None => None,
                    _ => Some(varint::decode_u64(decoder)),
                };
                let size_real = varint::decode_u64(decoder);
                let offset = varint::decode_u64(decoder);

                Ok(entries::Entry::File(Box::new(entries::FileEntry {
                    name,
                    mode,
                    owner: (uid, gid),
                    mtime,
                    file,
                    decoder: None,
                    size_compressed,
                    size_real,
                    size,
                    offset,
                    consumed: 0,
                    compression,
                })))
            }
            1 => {
                let mut entries: Vec<entries::Entry> = Vec::with_capacity(size as usize);
                for _ in 0..size {
                    let entry = Self::decode_entry(decoder, file.clone())?;
                    entries.push(entry);
                }

                Ok(entries::Entry::Directory(Box::new(
                    entries::DirectoryEntry {
                        name,
                        mode,
                        owner: (uid, gid),
                        mtime,
                        entries,
                    },
                )))
            }
            2 => {
                let mut target_bytes = vec![0; size as usize];
                decoder.read_exact(&mut target_bytes)?;

                let target = String::from_utf8(target_bytes).unwrap();

                let mut target_dir_bytes = [0; 1];
                decoder.read_exact(&mut target_dir_bytes)?;
                let target_dir = target_dir_bytes[0] != 0;

                Ok(entries::Entry::Symlink(Box::new(entries::SymlinkEntry {
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
