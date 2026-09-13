use crate::varint;
use entries::{DirectoryEntry, Entry, EntryMode, FileEntry, SymlinkEntry};
use flate2::{
    read::{DeflateDecoder, GzDecoder},
    write::{DeflateEncoder, GzEncoder},
};
use positioned_io::ReadAt;
use std::{
    ffi::OsStr,
    fmt::{Debug, Formatter},
    fs::{DirEntry, File, Metadata},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
};

pub mod entries;

pub const FILE_SIGNATURE: [u8; 7] = *b"DDUPBAK";
pub const FILE_VERSION: u8 = 2;

const HEADER_LEN: u64 = 8;
const FOOTER_LEN: u64 = 16;
/// Entry header word: 2 bits type, 4 bits compression, 26 bits mode.
const MODE_MASK: u32 = (1 << 26) - 1;
pub(crate) const ZSTD_LEVEL: i32 = 3;
/// Brotli quality/window used by every version so far.
#[cfg(feature = "brotli")]
const BROTLI_PARAMS: (usize, u32, u32) = (4096, 11, 22);

/// Format ids are part of the on-disk format: archive entries and chunk files store them.
/// Brotli is a Cargo feature; data using it fails to read with `ErrorKind::Unsupported` when
/// the feature is off.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CompressionFormat {
    #[default]
    None = 0,
    Gzip = 1,
    Deflate = 2,
    Brotli = 3,
    Zstd = 4,
}

impl CompressionFormat {
    pub const fn encode(&self) -> u8 {
        *self as u8
    }

    pub fn try_decode(value: u8) -> std::io::Result<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Gzip),
            2 => Ok(Self::Deflate),
            3 => Ok(Self::Brotli),
            4 => Ok(Self::Zstd),
            _ => Err(invalid("invalid compression format")),
        }
    }

    /// Whether this build can compress and decompress the format.
    pub const fn is_supported(&self) -> bool {
        match self {
            Self::None | Self::Gzip | Self::Deflate => true,
            Self::Brotli => cfg!(feature = "brotli"),
            Self::Zstd => true,
        }
    }

    pub fn unsupported_message(&self) -> String {
        format!(
            "{self:?} support is not enabled; build with the '{}' feature",
            format!("{self:?}").to_lowercase()
        )
    }

    pub(crate) fn unsupported(&self) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Unsupported, self.unsupported_message())
    }
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum Compressor<W: Write> {
    None(W),
    Gzip(GzEncoder<W>),
    Deflate(DeflateEncoder<W>),
    #[cfg(feature = "brotli")]
    Brotli(brotli::CompressorWriter<W>),
    Zstd(zstd::stream::write::Encoder<'static, W>),
}

impl<W: Write> Compressor<W> {
    pub fn new(format: CompressionFormat, writer: W) -> std::io::Result<Self> {
        Ok(match format {
            CompressionFormat::None => Self::None(writer),
            CompressionFormat::Gzip => {
                Self::Gzip(GzEncoder::new(writer, flate2::Compression::default()))
            }
            CompressionFormat::Deflate => {
                Self::Deflate(DeflateEncoder::new(writer, flate2::Compression::default()))
            }
            #[cfg(feature = "brotli")]
            CompressionFormat::Brotli => {
                let (buffer, quality, window) = BROTLI_PARAMS;
                Self::Brotli(brotli::CompressorWriter::new(
                    writer, buffer, quality, window,
                ))
            }
            CompressionFormat::Zstd => {
                Self::Zstd(zstd::stream::write::Encoder::new(writer, ZSTD_LEVEL)?)
            }
            #[allow(unreachable_patterns)]
            other => return Err(other.unsupported()),
        })
    }

    pub fn finish(self) -> std::io::Result<W> {
        match self {
            Self::None(writer) => Ok(writer),
            Self::Gzip(encoder) => encoder.finish(),
            Self::Deflate(encoder) => encoder.finish(),
            #[cfg(feature = "brotli")]
            Self::Brotli(mut encoder) => {
                encoder.flush()?;
                Ok(encoder.into_inner())
            }
            Self::Zstd(encoder) => encoder.finish(),
        }
    }
}

impl<W: Write> Write for Compressor<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::None(writer) => writer.write(buf),
            Self::Gzip(encoder) => encoder.write(buf),
            Self::Deflate(encoder) => encoder.write(buf),
            #[cfg(feature = "brotli")]
            Self::Brotli(encoder) => encoder.write(buf),
            Self::Zstd(encoder) => encoder.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::None(writer) => writer.flush(),
            Self::Gzip(encoder) => encoder.flush(),
            Self::Deflate(encoder) => encoder.flush(),
            #[cfg(feature = "brotli")]
            Self::Brotli(encoder) => encoder.flush(),
            Self::Zstd(encoder) => encoder.flush(),
        }
    }
}

pub(crate) fn decompressor<R: Read + Send + Sync + 'static>(
    format: CompressionFormat,
    reader: R,
) -> std::io::Result<Box<dyn Read + Send + Sync>> {
    Ok(match format {
        CompressionFormat::None => Box::new(reader),
        CompressionFormat::Gzip => Box::new(GzDecoder::new(reader)),
        CompressionFormat::Deflate => Box::new(DeflateDecoder::new(reader)),
        #[cfg(feature = "brotli")]
        CompressionFormat::Brotli => Box::new(brotli::Decompressor::new(reader, BROTLI_PARAMS.0)),
        CompressionFormat::Zstd => Box::new(zstd::stream::read::Decoder::new(reader)?),
        #[allow(unreachable_patterns)]
        other => return Err(other.unsupported()),
    })
}

fn invalid(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

/// Rejects names that could escape their directory when restored. A backslash only separates
/// path components on Windows; on Unix it is an ordinary filename byte that real trees use
/// (systemd escapes device names with it), so rejecting it there would refuse to back up or
/// migrate perfectly valid directories.
pub(crate) fn validate_name(name: &str, max_len: usize) -> std::io::Result<()> {
    let separator = |b: u8| b == b'/' || b == 0 || (cfg!(windows) && b == b'\\');
    if name.is_empty() || name == "." || name == ".." || name.bytes().any(separator) {
        return Err(invalid(format!("invalid entry name {name:?}")));
    }
    if name.len() > max_len {
        return Err(invalid(format!(
            "entry name is {} bytes, limit is {max_len}",
            name.len()
        )));
    }
    Ok(())
}

#[inline]
fn metadata_owner(_metadata: &Metadata) -> (u32, u32) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        (_metadata.uid(), _metadata.gid())
    }
    #[cfg(not(unix))]
    (0, 0)
}

/// Limits enforced while decoding an archive, keeping crafted inputs from exhausting memory.
#[derive(Debug, Clone, Copy)]
pub struct DecodeLimits {
    pub max_name_len: usize,
    pub max_target_len: usize,
    pub max_depth: usize,
    pub max_entry_count: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_name_len: 1024,
            max_target_len: 4096,
            max_depth: 256,
            max_entry_count: 10_000_000,
        }
    }
}

pub type ProgressCallback = Option<Arc<dyn Fn(&Path) + Send + Sync + 'static>>;
pub type CompressionFormatCallback =
    Option<Arc<dyn Fn(&Path, &Metadata) -> CompressionFormat + Send + Sync>>;
type RealSizeCallback = Option<Arc<dyn Fn(&Path) -> u64 + Send + Sync + 'static>>;

pub struct Archive {
    file: Arc<File>,
    version: u8,
    compression_callback: CompressionFormatCallback,
    real_size_callback: RealSizeCallback,

    pub entries: Vec<Entry>,
    /// Where the entry table starts; also the append position for new file content.
    entries_offset: AtomicU64,
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
    /// Starts a new archive in `file`, truncating it.
    pub fn new(mut file: File) -> std::io::Result<Self> {
        file.set_len(0)?;
        file.write_all(&FILE_SIGNATURE)?;
        file.write_all(&[FILE_VERSION])?;

        Ok(Self {
            file: Arc::new(file),
            version: FILE_VERSION,
            compression_callback: None,
            real_size_callback: None,
            entries: Vec::new(),
            entries_offset: AtomicU64::new(HEADER_LEN),
        })
    }

    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::open_with_limits(path, DecodeLimits::default())
    }

    pub fn open_with_limits(path: impl AsRef<Path>, limits: DecodeLimits) -> std::io::Result<Self> {
        Self::open_file_with_limits(File::open(path)?, limits)
    }

    pub fn open_file(file: File) -> std::io::Result<Self> {
        Self::open_file_with_limits(file, DecodeLimits::default())
    }

    pub fn open_file_with_limits(mut file: File, limits: DecodeLimits) -> std::io::Result<Self> {
        let len = file.metadata()?.len();
        if len < HEADER_LEN + FOOTER_LEN {
            return Err(invalid("archive is too short"));
        }

        let mut header = [0; 8];
        file.read_exact(&mut header)?;
        if !header.starts_with(&FILE_SIGNATURE) {
            return Err(invalid("invalid file signature"));
        }
        let version = header[7];
        if version == 0 || version > FILE_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!("unsupported archive version {version}"),
            ));
        }

        let mut footer = [0; 16];
        file.read_exact_at(len - FOOTER_LEN, &mut footer)?;
        let entries_count = u64::from_le_bytes(footer[..8].try_into().unwrap());
        let entries_offset = u64::from_le_bytes(footer[8..].try_into().unwrap());

        if entries_count > limits.max_entry_count as u64 {
            return Err(invalid(format!(
                "archive entry count {entries_count} exceeds limit {}",
                limits.max_entry_count
            )));
        }
        if entries_offset < HEADER_LEN || entries_offset > len - FOOTER_LEN {
            return Err(invalid("entry table offset is outside the archive"));
        }

        file.seek(SeekFrom::Start(entries_offset))?;
        let table = file.try_clone()?.take(len - FOOTER_LEN - entries_offset);
        let mut decoder = DeflateDecoder::new(table);
        let file = Arc::new(file);

        let mut entries = Vec::with_capacity(entries_count.min(4096) as usize);
        for _ in 0..entries_count {
            entries.push(Self::decode_entry(&mut decoder, &file, &limits, 0)?);
        }
        // Entries the footer leaves out would go uncounted and have their chunks cleaned away.
        if decoder.read(&mut [0])? != 0 {
            return Err(invalid(
                "entry table holds more entries than the footer counts",
            ));
        }

        Ok(Self {
            file,
            version,
            compression_callback: None,
            real_size_callback: None,
            entries,
            entries_offset: AtomicU64::new(entries_offset),
        })
    }

    pub const fn version(&self) -> u8 {
        self.version
    }

    /// Callback deciding the compression of each file added through `add_directory`/`add_entries`.
    pub fn set_compression_callback(&mut self, callback: CompressionFormatCallback) -> &mut Self {
        self.compression_callback = callback;
        self
    }

    /// Callback overriding the recorded "real" (uncompressed) size of files added through
    /// `add_directory`/`add_entries`.
    pub fn set_real_size_callback(&mut self, callback: RealSizeCallback) -> &mut Self {
        self.real_size_callback = callback;
        self
    }

    /// Appends the contents of a directory to the archive and rewrites the entry table.
    pub fn add_directory(
        &mut self,
        path: &str,
        progress: ProgressCallback,
    ) -> std::io::Result<&mut Self> {
        let entries = std::fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
        self.add_entries(entries, progress)
    }

    /// Appends filesystem entries to the archive and rewrites the entry table.
    pub fn add_entries(
        &mut self,
        entries: Vec<DirEntry>,
        progress: ProgressCallback,
    ) -> std::io::Result<&mut Self> {
        self.trim_end_header()?;

        for entry in entries {
            let entry = self.encode_entry(entry, &progress)?;
            self.entries.push(entry);
        }

        self.write_end_header()?;
        Ok(self)
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn into_entries(self) -> Vec<Entry> {
        self.entries
    }

    /// Writes file content to the archive and returns its entry. The caller owns placing the
    /// entry in the tree and calling `write_end_header` afterwards.
    #[allow(clippy::too_many_arguments)]
    pub fn write_file_entry(
        &mut self,
        mut reader: impl Read,
        size_real: Option<u64>,
        name: impl Into<String>,
        mode: EntryMode,
        mtime: SystemTime,
        owner: (u32, u32),
        compression: CompressionFormat,
    ) -> std::io::Result<Box<FileEntry>> {
        let name = name.into();
        validate_name(&name, DecodeLimits::default().max_name_len)?;

        let offset = self.entries_offset.load(Ordering::Acquire);
        (&*self.file).seek(SeekFrom::Start(offset))?;
        let mut encoder = Compressor::new(compression, &*self.file)?;
        let mut buffer = [0; 65536];
        let mut size = 0u64;

        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            encoder.write_all(&buffer[..bytes_read])?;
            size += bytes_read as u64;
        }
        encoder.finish()?;

        let end = (&*self.file).stream_position()?;
        self.entries_offset.store(end, Ordering::Release);
        let size_compressed = match compression {
            CompressionFormat::None => None,
            _ => Some(end - offset),
        };

        Ok(Box::new(FileEntry {
            name,
            mode,
            owner,
            mtime,
            compression,
            size_compressed,
            size_real: size_real.unwrap_or(size),
            size,
            file: Arc::clone(&self.file),
            offset,
            decoder: None,
            consumed: 0,
        }))
    }

    /// Appends uncompressed file content with a positioned write, so any number of threads can
    /// add entries at once. As with `write_file_entry`, the caller places the entry in the tree
    /// and calls `write_end_header` afterwards.
    pub fn write_raw_file_entry(
        &self,
        data: &[u8],
        size_real: Option<u64>,
        name: impl Into<String>,
        mode: EntryMode,
        mtime: SystemTime,
        owner: (u32, u32),
    ) -> std::io::Result<Box<FileEntry>> {
        let name = name.into();
        validate_name(&name, DecodeLimits::default().max_name_len)?;

        let offset = self
            .entries_offset
            .fetch_add(data.len() as u64, Ordering::AcqRel);
        crate::fs::write_all_at(&self.file, offset, data)?;

        Ok(Box::new(FileEntry {
            name,
            mode,
            owner,
            mtime,
            compression: CompressionFormat::None,
            size_compressed: None,
            size_real: size_real.unwrap_or(data.len() as u64),
            size: data.len() as u64,
            file: Arc::clone(&self.file),
            offset,
            decoder: None,
            consumed: 0,
        }))
    }

    /// Finds an entry by its path inside the archive, e.g. `world/level.dat`.
    pub fn find_archive_entry(&self, path: &Path) -> Option<&Entry> {
        let mut entries = self.entries.as_slice();
        let mut found = None;

        for part in path.components().map(|c| c.as_os_str()) {
            let entry = entries.iter().find(|e| OsStr::new(e.name()) == part)?;
            entries = match entry {
                Entry::Directory(dir) => &dir.entries,
                _ => &[],
            };
            found = Some(entry);
        }

        found
    }

    pub fn trim_end_header(&mut self) -> std::io::Result<()> {
        self.file
            .set_len(self.entries_offset.load(Ordering::Acquire))
    }

    pub fn write_end_header(&mut self) -> std::io::Result<()> {
        let entries_offset = self.entries_offset.load(Ordering::Acquire);
        let mut file = &*self.file;
        file.seek(SeekFrom::Start(entries_offset))?;

        let mut encoder = DeflateEncoder::new(file, flate2::Compression::default());
        for entry in &self.entries {
            Self::encode_entry_metadata(&mut encoder, entry)?;
        }
        encoder.finish()?;

        file.write_all(&(self.entries.len() as u64).to_le_bytes())?;
        file.write_all(&entries_offset.to_le_bytes())?;
        file.sync_all()
    }

    fn encode_entry_metadata<W: Write>(writer: &mut W, entry: &Entry) -> std::io::Result<()> {
        let name = entry.name();
        validate_name(name, DecodeLimits::default().max_name_len)?;
        varint::encode(writer, name.len() as u64)?;
        writer.write_all(name.as_bytes())?;

        let (entry_type, compression) = match entry {
            Entry::File(file) => (0, file.compression),
            Entry::Directory(_) => (1, CompressionFormat::None),
            Entry::Symlink(_) => (2, CompressionFormat::None),
        };
        let type_compression_mode = (entry_type << 30)
            | ((compression.encode() as u32) << 26)
            | (entry.mode().bits() & MODE_MASK);
        writer.write_all(&type_compression_mode.to_le_bytes())?;

        let (uid, gid) = entry.owner();
        varint::encode(writer, uid as u64)?;
        varint::encode(writer, gid as u64)?;

        let mtime = entry
            .mtime()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        varint::encode(writer, mtime.as_secs())?;

        match entry {
            Entry::File(file) => {
                varint::encode(writer, file.size)?;
                if let Some(size_compressed) = file.size_compressed {
                    varint::encode(writer, size_compressed)?;
                }
                varint::encode(writer, file.size_real)?;
                varint::encode(writer, file.offset)?;
            }
            Entry::Directory(dir) => {
                varint::encode(writer, dir.entries.len() as u64)?;
                for sub_entry in &dir.entries {
                    Self::encode_entry_metadata(writer, sub_entry)?;
                }
            }
            Entry::Symlink(link) => {
                varint::encode(writer, link.target.len() as u64)?;
                writer.write_all(link.target.as_bytes())?;
                writer.write_all(&[link.target_dir as u8])?;
            }
        }

        Ok(())
    }

    fn encode_entry(
        &mut self,
        fs_entry: DirEntry,
        progress: &ProgressCallback,
    ) -> std::io::Result<Entry> {
        let path = fs_entry.path();
        let name = path
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| invalid(format!("{} is not a valid UTF-8 file name", path.display())))?
            .to_owned();
        let metadata = path.symlink_metadata()?;
        let mode = metadata.permissions().into();
        let owner = metadata_owner(&metadata);
        let mtime = metadata.modified()?;

        let entry = if metadata.is_file() {
            let compression = match &self.compression_callback {
                Some(callback) => callback(&path, &metadata),
                None if metadata.len() > 16 => CompressionFormat::Deflate,
                None => CompressionFormat::None,
            };
            let size_real = self
                .real_size_callback
                .as_ref()
                .map_or(metadata.len(), |callback| callback(&path));

            Entry::File(self.write_file_entry(
                File::open(&path)?,
                Some(size_real),
                name,
                mode,
                mtime,
                owner,
                compression,
            )?)
        } else if metadata.is_dir() {
            let mut entries = Vec::new();
            for sub_entry in std::fs::read_dir(&path)? {
                entries.push(self.encode_entry(sub_entry?, progress)?);
            }

            Entry::Directory(Box::new(DirectoryEntry {
                name,
                mode,
                owner,
                mtime,
                entries,
            }))
        } else if metadata.is_symlink() {
            let target = std::fs::read_link(&path)?;
            let target = target
                .to_str()
                .ok_or_else(|| invalid(format!("{} has a non-UTF-8 target", path.display())))?
                .to_owned();

            Entry::Symlink(Box::new(SymlinkEntry {
                name,
                mode,
                owner,
                mtime,
                target,
                target_dir: path.is_dir(),
            }))
        } else {
            return Err(invalid(format!(
                "{} is not a file, directory or symlink",
                path.display()
            )));
        };

        if let Some(progress) = progress {
            progress(&path);
        }

        Ok(entry)
    }

    fn decode_entry<R: Read>(
        decoder: &mut R,
        file: &Arc<File>,
        limits: &DecodeLimits,
        depth: usize,
    ) -> std::io::Result<Entry> {
        let name_length = varint::decode_u32(decoder)? as usize;
        if name_length > limits.max_name_len {
            return Err(invalid(format!(
                "entry name length {name_length} exceeds limit {}",
                limits.max_name_len
            )));
        }

        let mut name = vec![0; name_length];
        decoder.read_exact(&mut name)?;
        let name = String::from_utf8(name).map_err(|e| invalid(e.to_string()))?;
        validate_name(&name, limits.max_name_len)?;

        let mut type_mode = [0; 4];
        decoder.read_exact(&mut type_mode)?;
        let type_compression_mode = u32::from_le_bytes(type_mode);

        let entry_type = type_compression_mode >> 30;
        let compression =
            CompressionFormat::try_decode(((type_compression_mode >> 26) & 0xF) as u8)?;
        let mode = EntryMode::from(type_compression_mode & MODE_MASK);

        let uid = varint::decode_u32(decoder)?;
        let gid = varint::decode_u32(decoder)?;
        let mtime = SystemTime::UNIX_EPOCH
            .checked_add(std::time::Duration::from_secs(varint::decode(decoder)?))
            .ok_or_else(|| invalid("entry timestamp is out of range"))?;
        let size = varint::decode(decoder)?;

        match entry_type {
            0 => {
                let size_compressed = match compression {
                    CompressionFormat::None => None,
                    _ => Some(varint::decode(decoder)?),
                };
                let size_real = varint::decode(decoder)?;
                let offset = varint::decode(decoder)?;

                Ok(Entry::File(Box::new(FileEntry {
                    name,
                    mode,
                    owner: (uid, gid),
                    mtime,
                    compression,
                    size_compressed,
                    size_real,
                    size,
                    file: Arc::clone(file),
                    offset,
                    decoder: None,
                    consumed: 0,
                })))
            }
            1 => {
                if size > limits.max_entry_count as u64 {
                    return Err(invalid(format!(
                        "directory child count {size} exceeds limit {}",
                        limits.max_entry_count
                    )));
                }
                if depth >= limits.max_depth {
                    return Err(invalid(format!(
                        "directory nesting exceeds limit {}",
                        limits.max_depth
                    )));
                }

                let mut entries = Vec::with_capacity(size.min(4096) as usize);
                for _ in 0..size {
                    entries.push(Self::decode_entry(decoder, file, limits, depth + 1)?);
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
                if size > limits.max_target_len as u64 {
                    return Err(invalid(format!(
                        "symlink target length {size} exceeds limit {}",
                        limits.max_target_len
                    )));
                }

                let mut target = vec![0; size as usize];
                decoder.read_exact(&mut target)?;
                let target = String::from_utf8(target).map_err(|e| invalid(e.to_string()))?;

                let mut target_dir = [0; 1];
                decoder.read_exact(&mut target_dir)?;

                Ok(Entry::Symlink(Box::new(SymlinkEntry {
                    name,
                    mode,
                    owner: (uid, gid),
                    mtime,
                    target,
                    target_dir: target_dir[0] != 0,
                })))
            }
            _ => Err(invalid("invalid entry type")),
        }
    }
}
