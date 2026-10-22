use super::ChunkHash;
use std::{
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

pub trait ChunkStorage: Send + Sync {
    #[inline]
    fn path_from_chunk(&self, chunk: &ChunkHash) -> PathBuf {
        let hex = super::hex(chunk);
        let mut path = String::with_capacity(hex.len() + 8);
        path.push_str(&hex[..2]);
        path.push('/');
        path.push_str(&hex[2..4]);
        path.push('/');
        path.push_str(&hex[4..]);
        path.push_str(".chunk");
        PathBuf::from(path)
    }

    fn read_chunk_content(&self, chunk: &ChunkHash)
    -> std::io::Result<Box<dyn Read + Send + Sync>>;
    fn write_chunk_content(&self, chunk: &ChunkHash, content: &[u8]) -> std::io::Result<()>;
    fn delete_chunk_content(&self, chunk: &ChunkHash) -> std::io::Result<()>;
    fn list_chunk_hashes(&self) -> std::io::Result<Vec<ChunkHash>>;

    fn has_chunk(&self, chunk: &ChunkHash) -> std::io::Result<bool> {
        let mut content = match self.read_chunk_content(chunk) {
            Ok(content) => content,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(err),
        };
        // A directory opens but can't be read; a chunk has at least its format byte.
        match content.read_exact(&mut [0u8; 1]) {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Removes leftovers of interrupted writes. `clean` calls it while nothing writes chunks.
    fn remove_leftovers(&self) -> std::io::Result<()> {
        Ok(())
    }

    /// Makes every chunk written so far durable. Backups call it before saving the index that
    /// counts them, so `write_chunk_content` need not sync each chunk. Required so a wrapper
    /// around another storage can't silently drop the inner one's sync.
    fn sync(&self) -> std::io::Result<()>;

    /// `write_chunk_content` for a chunk the saved index already counts, whose content must be
    /// durable before it appears rather than at the next `sync`. Required for the same reason.
    fn write_chunk_content_durable(&self, chunk: &ChunkHash, content: &[u8])
    -> std::io::Result<()>;
}

pub struct ChunkStorageLocal {
    path: PathBuf,
    durable: durable::State,
}

impl ChunkStorageLocal {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            durable: Default::default(),
        }
    }

    fn write(&self, chunk: &ChunkHash, content: &[u8], durable: bool) -> std::io::Result<()> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let path = self.path.join(self.path_from_chunk(chunk));
        let parent = path.parent().unwrap();
        std::fs::create_dir_all(parent)?;
        self.durable.begin(&self.path)?;
        let durable = durable || path.symlink_metadata().is_ok();

        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_path = path.with_extension(format!("{}.{unique}.tmp", std::process::id()));
        let mut file = match File::create_new(&tmp_path) {
            // Left by a dead process with the same pid.
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                std::fs::remove_file(&tmp_path)?;
                File::create_new(&tmp_path)?
            }
            file => file?,
        };

        if let Err(err) = file
            .write_all(content)
            .and_then(|()| self.durable.chunk(&file, durable))
        {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }

        if let Err(err) = std::fs::rename(&tmp_path, &path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }
        self.durable.wrote(&path, file);
        Ok(())
    }
}

impl ChunkStorage for ChunkStorageLocal {
    fn read_chunk_content(
        &self,
        chunk: &ChunkHash,
    ) -> std::io::Result<Box<dyn Read + Send + Sync>> {
        Ok(Box::new(File::open(
            self.path.join(self.path_from_chunk(chunk)),
        )?))
    }

    fn write_chunk_content(&self, chunk: &ChunkHash, content: &[u8]) -> std::io::Result<()> {
        self.write(chunk, content, false)
    }

    fn write_chunk_content_durable(
        &self,
        chunk: &ChunkHash,
        content: &[u8],
    ) -> std::io::Result<()> {
        self.write(chunk, content, true)
    }

    fn sync(&self) -> std::io::Result<()> {
        self.durable.all(&self.path)
    }

    fn has_chunk(&self, chunk: &ChunkHash) -> std::io::Result<bool> {
        Ok(is_chunk_file(&self.path.join(self.path_from_chunk(chunk))))
    }

    fn delete_chunk_content(&self, chunk: &ChunkHash) -> std::io::Result<()> {
        let path = self.path.join(self.path_from_chunk(chunk));
        std::fs::remove_file(&path)?;

        for parent in path.ancestors().skip(1).take(2) {
            if std::fs::read_dir(parent)?.next().is_some() {
                break;
            }
            std::fs::remove_dir(parent)?;
        }

        Ok(())
    }

    fn remove_leftovers(&self) -> std::io::Result<()> {
        // Only our own names in hex-named shard directories, never through symlinks.
        let is_shard = |entry: &std::fs::DirEntry| -> std::io::Result<bool> {
            Ok(entry.file_type()?.is_dir()
                && entry.file_name().to_str().is_some_and(|name| {
                    name.len() == 2 && name.bytes().all(|b| b.is_ascii_hexdigit())
                }))
        };
        for level in std::fs::read_dir(&self.path)? {
            let level = level?;
            if !is_shard(&level)? {
                continue;
            }
            for dir in std::fs::read_dir(level.path())? {
                let dir = dir?;
                if !is_shard(&dir)? {
                    continue;
                }
                for file in std::fs::read_dir(dir.path())? {
                    let file = file?;
                    if file.file_type()?.is_file()
                        && file.file_name().to_str().is_some_and(is_chunk_temporary)
                    {
                        std::fs::remove_file(file.path())?;
                    }
                }
            }
        }
        Ok(())
    }

    fn list_chunk_hashes(&self) -> std::io::Result<Vec<ChunkHash>> {
        let mut hashes = Vec::new();

        let entries = |path: PathBuf, dirs: bool| -> std::io::Result<Vec<std::fs::DirEntry>> {
            let entries = match std::fs::read_dir(path) {
                Ok(entries) => entries.collect::<Result<Vec<_>, _>>()?,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(err) => return Err(err),
            };
            entries
                .into_iter()
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir() == dirs))
                .map(Ok)
                .collect()
        };

        for first in entries(self.path.clone(), true)? {
            for second in entries(first.path(), true)? {
                for file in entries(second.path(), false)? {
                    let name = format!(
                        "{}{}{}",
                        first.file_name().to_string_lossy(),
                        second.file_name().to_string_lossy(),
                        file.file_name().to_string_lossy()
                    );
                    if let Some(hash) = parse_chunk_name(&name) {
                        hashes.push(hash);
                    }
                }
            }
        }

        Ok(hashes)
    }
}

fn parse_chunk_name(name: &str) -> Option<ChunkHash> {
    let hex = name.strip_suffix(".chunk")?;
    if hex.len() != 64 {
        return None;
    }

    let mut hash = [0; 32];
    for (byte, pair) in hash.iter_mut().zip(hex.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }

    Some(hash)
}

#[cfg(unix)]
mod durable {
    use parking_lot::Mutex;
    use std::{
        collections::HashSet,
        fs::File,
        path::{Path, PathBuf},
    };

    #[derive(Default)]
    pub struct State {
        /// Directories that chunks were renamed into since the last `all`.
        dirs: Mutex<HashSet<PathBuf>>,
        #[cfg(target_os = "linux")]
        batch: batch::Batch,
    }

    impl State {
        pub fn begin(&self, dir: &Path) -> std::io::Result<()> {
            #[cfg(target_os = "linux")]
            self.batch.begin(dir)?;
            #[cfg(not(target_os = "linux"))]
            let _ = dir;
            Ok(())
        }

        /// Syncs a written chunk unless `all` will. `now` makes it durable here regardless.
        pub fn chunk(&self, file: &File, now: bool) -> std::io::Result<()> {
            if now {
                return file.sync_all();
            }
            #[cfg(target_os = "linux")]
            if self.batch.active() {
                return Ok(());
            }
            sync(file)
        }

        pub fn wrote(&self, path: &Path, file: File) {
            let parent = path.parent().unwrap();
            let mut dirs = self.dirs.lock();
            if !dirs.contains(parent) {
                dirs.insert(parent.to_path_buf());
            }
            drop(dirs);
            #[cfg(target_os = "linux")]
            self.batch.wrote(file);
            #[cfg(not(target_os = "linux"))]
            let _ = file;
        }

        pub fn all(&self, dir: &Path) -> std::io::Result<()> {
            let dirs = std::mem::take(&mut *self.dirs.lock());
            #[cfg(target_os = "linux")]
            if self.batch.all()? {
                return Ok(());
            }
            if dirs.is_empty() {
                return Ok(());
            }
            sync_dirs(dirs.iter().map(PathBuf::as_path))?;
            flush(dir)
        }
    }

    /// Syncs the directories and the two levels above each, which `write_chunk_content` may have
    /// created.
    fn sync_dirs<'a>(dirs: impl IntoIterator<Item = &'a Path>) -> std::io::Result<()> {
        let dirs: HashSet<&Path> = dirs
            .into_iter()
            .flat_map(|dir| dir.ancestors().take(3))
            .collect();
        dirs.into_iter().try_for_each(|dir| match File::open(dir) {
            Ok(file) => sync(&file),
            // `clean` may have removed a directory that a failed backup wrote into.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        })
    }

    /// Plain `fsync` on Apple hands the data to the drive without flushing its cache, which
    /// `sync_all` would do per call; `flush` does that once.
    fn sync(file: &File) -> std::io::Result<()> {
        #[cfg(target_vendor = "apple")]
        // SAFETY: `file` owns a valid descriptor for the duration of the call.
        if unsafe { libc::fsync(std::os::fd::AsRawFd::as_raw_fd(file)) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        #[cfg(not(target_vendor = "apple"))]
        file.sync_all()?;
        Ok(())
    }

    /// F_FULLFSYNC flushes the drive's cache, which holds everything `sync` handed it.
    fn flush(dir: &Path) -> std::io::Result<()> {
        #[cfg(target_vendor = "apple")]
        crate::chunks::sync_dir(dir)?;
        #[cfg(not(target_vendor = "apple"))]
        let _ = dir;
        Ok(())
    }

    /// One `syncfs` for a backup's chunks instead of a sync per chunk, where that is safe.
    #[cfg(target_os = "linux")]
    mod batch {
        use parking_lot::Mutex;
        use std::{
            fs::File,
            os::fd::AsRawFd,
            path::Path,
            sync::{LazyLock, OnceLock},
        };

        /// Most chunks `all` syncs one by one. Past that a single `syncfs` is cheaper, though it
        /// also flushes everything else on the filesystem.
        const FSYNC_LIMIT: usize = 64;

        /// Filesystems whose `syncfs` makes the data durable and reports failed writes. FUSE
        /// doesn't pass it to its daemon and overlayfs may drop the errors, so anything else
        /// syncs each chunk.
        const SYNCFS_FILESYSTEMS: [u32; 5] = [
            libc::EXT4_SUPER_MAGIC as u32,
            XFS_SUPER_MAGIC,
            libc::BTRFS_SUPER_MAGIC as u32,
            libc::F2FS_SUPER_MAGIC as u32,
            ZFS_SUPER_MAGIC,
        ];
        /// Missing from libc on musl.
        const XFS_SUPER_MAGIC: u32 = 0x5846_5342;
        const ZFS_SUPER_MAGIC: u32 = 0x2FC1_2FC1;

        /// `syncfs` reports failed writes since Linux 5.8. Older kernels sync each chunk, as does
        /// a kernel whose version can't be read.
        static SYNCFS_REPORTS_ERRORS: LazyLock<bool> = LazyLock::new(|| {
            std::fs::read_to_string("/proc/sys/kernel/osrelease").is_ok_and(|release| {
                let mut parts = release
                    .split(|c: char| !c.is_ascii_digit())
                    .map(|part| part.parse::<u32>().unwrap_or(0));
                (parts.next(), parts.next()) >= (Some(5), Some(8))
            })
        });

        #[derive(Default)]
        pub struct Batch {
            /// Whether `syncfs` is used, decided by the first `begin`.
            syncfs: OnceLock<bool>,
            pending: Mutex<Pending>,
        }

        /// What `all` syncs, gathered since the last call.
        #[derive(Default)]
        struct Pending {
            /// The chunk directory, opened before the first chunk write: `syncfs` misses a failed
            /// write that another caller saw before its descriptor was opened, and one kept
            /// across backups reports failures from long before.
            dir: Option<File>,
            /// Kept open so a failed write is still reported when they are synced.
            files: Vec<File>,
            /// More than `FSYNC_LIMIT` chunks were written, so `files` was dropped for `syncfs`.
            overflowed: bool,
        }

        impl Batch {
            pub fn begin(&self, dir: &Path) -> std::io::Result<()> {
                if self.syncfs.get() == Some(&false) {
                    return Ok(());
                }
                let mut pending = self.pending.lock();
                if pending.dir.is_none() {
                    let dir = File::open(dir)?;
                    if self.syncfs.get().is_none() {
                        let _ = self
                            .syncfs
                            .set(*SYNCFS_REPORTS_ERRORS && syncfs_is_durable(&dir)?);
                    }
                    if self.active() {
                        pending.dir = Some(dir);
                    }
                }
                Ok(())
            }

            pub fn active(&self) -> bool {
                self.syncfs.get() == Some(&true)
            }

            pub fn wrote(&self, file: File) {
                if !self.active() {
                    return;
                }
                let mut pending = self.pending.lock();
                if !pending.overflowed && pending.files.len() < FSYNC_LIMIT {
                    pending.files.push(file);
                } else {
                    pending.overflowed = true;
                    pending.files.clear();
                }
            }

            /// Syncs the chunks written since the last call. `true` if it flushed the whole
            /// filesystem, directories included; a few chunks are synced on their own instead.
            pub fn all(&self) -> std::io::Result<bool> {
                let Pending {
                    dir,
                    files,
                    overflowed,
                } = std::mem::take(&mut *self.pending.lock());
                let Some(dir) = dir.filter(|_| overflowed) else {
                    files.iter().try_for_each(File::sync_all)?;
                    return Ok(false);
                };
                // SAFETY: `dir` owns a valid descriptor for the duration of the call.
                if unsafe { libc::syncfs(dir.as_raw_fd()) } != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(true)
            }
        }

        fn syncfs_is_durable(dir: &File) -> std::io::Result<bool> {
            let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
            // SAFETY: `dir` owns a valid descriptor and `stat` is writable for a `statfs`.
            if unsafe { libc::fstatfs(dir.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            // SAFETY: `fstatfs` succeeded, so it filled `stat`.
            let kind = unsafe { stat.assume_init() }.f_type as u32;
            Ok(SYNCFS_FILESYSTEMS.contains(&kind))
        }
    }
}

/// Directories can't be synced here, so only the chunks are.
#[cfg(not(unix))]
mod durable {
    use std::{fs::File, path::Path};

    #[derive(Default)]
    pub struct State;

    impl State {
        pub fn begin(&self, _dir: &Path) -> std::io::Result<()> {
            Ok(())
        }

        pub fn chunk(&self, file: &File, _now: bool) -> std::io::Result<()> {
            file.sync_all()
        }

        pub fn wrote(&self, _path: &Path, _file: File) {}

        pub fn all(&self, _dir: &Path) -> std::io::Result<()> {
            Ok(())
        }
    }
}

/// A regular file with at least the format byte. Empty ones, left by a crash, get rewritten.
fn is_chunk_file(path: &Path) -> bool {
    path.metadata()
        .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
}

/// Matches `write_chunk_content` temp names: `<hex>.<pid>.<counter>.tmp`.
fn is_chunk_temporary(name: &str) -> bool {
    let parts: Vec<&str> = name.split('.').collect();
    parts.len() == 4
        && parts[3] == "tmp"
        && parts[0].len() == 60
        && parts[0].bytes().all(|b| b.is_ascii_hexdigit())
        && parts[1..3]
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
}
