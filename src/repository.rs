use crate::{
    archive::{
        Archive, CompressionFormat, CompressionFormatCallback, FILE_VERSION, ProgressCallback,
        entries::{DirectoryEntry, Entry, SymlinkEntry},
    },
    cache::{CachedFile, FileCache, Fingerprint},
    chunks::{
        self, ChunkHash, ChunkIndex, HashAlgorithm,
        reader::EntryReader,
        storage::{ChunkStorage, ChunkStorageLocal},
    },
    lock::Lock,
};
use parking_lot::{Condvar, Mutex};
use rayon::prelude::*;
use std::{
    borrow::Cow,
    collections::HashMap,
    fs::{File, FileTimes, Metadata},
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

/// Chunks of one file read and decompressed concurrently while restoring. Bounds memory to
/// `threads * RESTORE_WINDOW` chunks.
const RESTORE_WINDOW: usize = 4;

/// Whether a damaged format 1 index should be read for whatever part of it still decodes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Salvage {
    No,
    Yes,
}

pub type DeletionProgressCallback = Option<Arc<dyn Fn(&ChunkHash, bool) + Send + Sync + 'static>>;
pub type RebuildProgressCallback = Option<Arc<dyn Fn(&ChunkHash, u64) + Send + Sync + 'static>>;

/// Repository locking:
/// - `chunks.lock` is held shared by anything reading or adding chunks (create, restore, readers)
///   and exclusively by anything deleting chunks (delete, clean, rebuild).
/// - `index.lock` is held exclusively while creating an archive, serialising index updates.
///
/// The index is loaded under the lock at the start of every mutating operation and saved before
/// the archive footer is published, so no operation works from a stale snapshot.
pub struct Repository {
    pub directory: PathBuf,
    chunks_directory: PathBuf,
    storage: Arc<dyn ChunkStorage>,
    chunk_size: usize,
    max_chunk_count: usize,
    hash_algorithm: HashAlgorithm,
}

impl Repository {
    /// Creates a repository with the default (BLAKE2b) chunk hash.
    pub fn new(
        directory: &Path,
        chunk_size: usize,
        max_chunk_count: usize,
        storage: Option<Arc<dyn ChunkStorage>>,
    ) -> std::io::Result<Self> {
        Self::new_with_hash(
            directory,
            chunk_size,
            max_chunk_count,
            HashAlgorithm::default(),
            storage,
        )
    }

    pub fn new_with_hash(
        directory: &Path,
        chunk_size: usize,
        max_chunk_count: usize,
        hash_algorithm: HashAlgorithm,
        storage: Option<Arc<dyn ChunkStorage>>,
    ) -> std::io::Result<Self> {
        let base = directory.join(".ddup-bak");
        for sub in ["archives", "archives-restored", "chunks"] {
            crate::fs::create_dir_all(&base.join(sub))?;
        }

        let repository = Self::with_storage(
            directory,
            None,
            storage,
            chunk_size,
            max_chunk_count,
            hash_algorithm,
        );
        ChunkIndex::new(chunk_size, max_chunk_count, hash_algorithm)
            .save(&repository.index_path())?;
        Ok(repository)
    }

    /// Opens a repository. One written by a version before archive format 2 is migrated in
    /// place first (see `migrate_v1`).
    pub fn open(
        directory: &Path,
        chunks_directory: Option<&Path>,
        storage: Option<Arc<dyn ChunkStorage>>,
    ) -> std::io::Result<Self> {
        let index_path = Self::chunks_directory(directory, chunks_directory).join("index");
        let header = ChunkIndex::load_header(&index_path)?;
        let repository = Self::with_storage(
            directory,
            chunks_directory,
            storage,
            header.chunk_size,
            header.max_chunk_count,
            header.hash_algorithm,
        );
        if header.version == 1 {
            repository.migrate_v1(Salvage::No)?;
        }
        Ok(repository)
    }

    /// Recreates the chunk index from the archives and chunk storage. The hash algorithm is
    /// recovered from the chunks themselves.
    pub fn rebuild(
        directory: &Path,
        chunk_size: usize,
        max_chunk_count: usize,
        chunks_directory: Option<&Path>,
        storage: Option<Arc<dyn ChunkStorage>>,
        progress: RebuildProgressCallback,
    ) -> std::io::Result<Self> {
        let mut repository = Self::with_storage(
            directory,
            chunks_directory,
            storage,
            chunk_size,
            max_chunk_count,
            HashAlgorithm::default(),
        );
        crate::fs::create_dir_all(&repository.chunks_directory)?;

        // Format 1 archives can only be resolved through the index of their era, so finish that
        // migration while it is still possible, keeping whatever a damaged index still decodes.
        if ChunkIndex::load_header(&repository.index_path()).is_ok_and(|header| header.version == 1)
        {
            repository.migrate_v1(Salvage::Yes)?;
        }

        let _lock = Lock::exclusive(&repository.chunks_lock_path())?;
        repository.hash_algorithm = chunks::detect_hash_algorithm(&*repository.storage)?;

        // An archive that cannot be read counts for nothing rather than failing the rebuild,
        // so the rest of the repository comes back. Its chunks stay: `clean` and
        // `delete_archive` refuse to remove anything while such an archive is present.
        let names = repository.list_archives()?;
        let index = ChunkIndex::rebuild(
            chunk_size,
            max_chunk_count,
            repository.hash_algorithm,
            &*repository.storage,
            names
                .iter()
                .filter_map(|name| repository.get_archive(name).ok().map(Ok)),
            |hash, references| {
                if let Some(progress) = &progress {
                    progress(hash, references);
                }
            },
        )?;
        index.save(&repository.index_path())?;

        Ok(repository)
    }

    /// Rewrites format 1 archives (chunk id lists) into hash lists and the format 1 index into
    /// the current format. Idempotent: an interrupted run leaves a mix of archive versions and
    /// the old index, and the next open picks up where it stopped. With `salvage`, a damaged
    /// index yields whatever part of it still decodes instead of failing, so the archives it
    /// covers are recovered; see `ChunkIndex::salvage_v1`.
    fn migrate_v1(&self, salvage: Salvage) -> std::io::Result<()> {
        if let Some(pid) = self.legacy_writer() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                format!(
                    "process {pid} is writing to this repository with a version of ddup-bak \
                     from before archive format 2. Stop it before opening the repository with \
                     this version, or it will save its own index over the migrated one"
                ),
            ));
        }
        let _chunks_lock = Lock::exclusive(&self.chunks_lock_path())?;
        let _index_lock = Lock::exclusive(&self.index_lock_path())?;
        if ChunkIndex::load_header(&self.index_path())?.version != 1 {
            return Ok(());
        }
        let (index, ids) = if salvage == Salvage::Yes {
            ChunkIndex::salvage_v1(&self.index_path())
        } else {
            ChunkIndex::load_v1(&self.index_path())
        }?;

        // An archive that is already damaged is left exactly as it is rather than failing the
        // migration, which would lock the reader out of every other archive beside it. Opening
        // that one archive still reports what is wrong with it.
        let damaged = |err: &std::io::Error| {
            matches!(
                err.kind(),
                std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
            )
        };

        for name in self.list_archives()? {
            let path = self.archive_path(&name)?;
            let archive = match Archive::open(&path) {
                Ok(archive) => archive,
                Err(err) if damaged(&err) => continue,
                Err(err) => return Err(err),
            };
            if archive.version() != 1 {
                continue;
            }

            let tmp_path = path.with_extension("ddup.migrate");
            let _ = std::fs::remove_file(&tmp_path);
            let mut migrated = Archive::new(crate::fs::create_new_file(&tmp_path)?)?;
            let result = migrate_v1_entries(archive.into_entries(), &migrated, &ids)
                .and_then(|entries| {
                    migrated.entries = entries;
                    migrated.write_end_header()
                })
                .and_then(|()| std::fs::rename(&tmp_path, &path));
            if let Err(err) = result {
                let _ = std::fs::remove_file(&tmp_path);
                // Anything that is not damage is the environment, and retrying later is right.
                if !damaged(&err) {
                    return Err(err);
                }
            }
        }
        crate::fs::sync_dir(&self.directory.join(".ddup-bak/archives"))?;

        index.save(&self.index_path())
    }

    /// The pid of a process holding a write lock taken by a version older than archive format
    /// 2, if one is running. Those versions used their own advisory scheme in `index.lock`,
    /// which this version cannot take part in: the file is a mode byte, a presence byte and a
    /// pid, each padded to eight bytes. A stale file is ignored, since the pid identifies it.
    fn legacy_writer(&self) -> Option<u32> {
        let state = std::fs::read(self.index_lock_path()).ok()?;
        if *state.get(8)? == 0 {
            return None;
        }
        let pid = u32::try_from(u64::from_le_bytes(state.get(16..24)?.try_into().ok()?)).ok()?;
        (pid != 0 && process_is_running(pid)).then_some(pid)
    }

    pub fn hash_algorithm(&self) -> HashAlgorithm {
        self.hash_algorithm
    }

    /// Kept for callers written against versions that buffered index changes in memory. Every
    /// operation now persists the index itself, so there is nothing to do.
    pub fn save(&self) -> std::io::Result<()> {
        Ok(())
    }

    /// See `save`.
    pub fn set_save_on_drop(&mut self, _save_on_drop: bool) -> &mut Self {
        self
    }

    /// The directory `restore_archive` restores `name` into.
    pub fn restored_path(&self, name: &str) -> std::io::Result<PathBuf> {
        crate::archive::validate_name(name, 255)?;
        Ok(self
            .directory
            .join(".ddup-bak/archives-restored")
            .join(name))
    }

    pub fn open_or_rebuild(
        directory: &Path,
        chunk_size: usize,
        max_chunk_count: usize,
        chunks_directory: Option<&Path>,
        storage: Option<Arc<dyn ChunkStorage>>,
        progress: RebuildProgressCallback,
    ) -> std::io::Result<Self> {
        match Self::open(directory, chunks_directory, storage.clone()) {
            Ok(repository) => Ok(repository),
            Err(_) => Self::rebuild(
                directory,
                chunk_size,
                max_chunk_count,
                chunks_directory,
                storage,
                progress,
            ),
        }
    }

    fn with_storage(
        directory: &Path,
        chunks_directory: Option<&Path>,
        storage: Option<Arc<dyn ChunkStorage>>,
        chunk_size: usize,
        max_chunk_count: usize,
        hash_algorithm: HashAlgorithm,
    ) -> Self {
        let chunks_directory = Self::chunks_directory(directory, chunks_directory);
        Self {
            directory: directory.to_path_buf(),
            storage: storage
                .unwrap_or_else(|| Arc::new(ChunkStorageLocal(chunks_directory.clone()))),
            chunks_directory,
            chunk_size,
            max_chunk_count,
            hash_algorithm,
        }
    }

    fn chunks_directory(directory: &Path, chunks_directory: Option<&Path>) -> PathBuf {
        chunks_directory.map_or_else(|| directory.join(".ddup-bak/chunks"), Path::to_path_buf)
    }

    fn index_path(&self) -> PathBuf {
        self.chunks_directory.join("index")
    }

    fn chunks_lock_path(&self) -> PathBuf {
        self.chunks_directory.join("chunks.lock")
    }

    fn index_lock_path(&self) -> PathBuf {
        self.chunks_directory.join("index.lock")
    }

    fn file_cache_path(&self) -> PathBuf {
        self.directory.join(".ddup-bak/filecache")
    }

    pub fn archive_path(&self, name: &str) -> std::io::Result<PathBuf> {
        crate::archive::validate_name(name, 255)?;
        Ok(self
            .directory
            .join(".ddup-bak/archives")
            .join(format!("{name}.ddup")))
    }

    /// Archive names without the `.ddup` extension.
    pub fn list_archives(&self) -> std::io::Result<Vec<String>> {
        let mut archives = Vec::new();
        for entry in std::fs::read_dir(self.directory.join(".ddup-bak/archives"))? {
            let name = entry?.file_name();
            if let Some(name) = name.to_str().and_then(|name| name.strip_suffix(".ddup")) {
                archives.push(name.to_owned());
            }
        }
        Ok(archives)
    }

    /// Opens an archive. File entries hold chunk hash lists, use `entry_reader` or
    /// `restore_archive` to get at file content.
    pub fn get_archive(&self, name: &str) -> std::io::Result<Archive> {
        let archive = Archive::open(self.archive_path(name)?)?;
        if archive.version() != FILE_VERSION {
            // Opening the repository migrates format 1 archives, so seeing one here means that
            // did not happen, which only occurs when the index it needs is unreadable.
            let reason = if archive.version() < FILE_VERSION {
                format!(
                    "it is migrated when the repository is opened, which needs {}. Restore that \
                     file and open the repository again; the chunk ids in a format {} archive \
                     cannot be resolved without it",
                    self.index_path().display(),
                    archive.version()
                )
            } else {
                "it was written by a newer version of ddup-bak".to_string()
            };
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!(
                    "archive {name} is in format version {}, this build reads version \
                     {FILE_VERSION}: {reason}",
                    archive.version()
                ),
            ));
        }
        Ok(archive)
    }

    /// Backs up `root` into a new archive. Without a `walker`, the default one is the one every
    /// version has used: `ignore`'s standard filters, which skip dot entries and honour
    /// `.gitignore` and `.ignore`, but not the user's global gitignore. Pass a walker built with
    /// `standard_filters(false)` to back up everything instead.
    pub fn create_archive(
        &self,
        name: &str,
        walker: Option<ignore::Walk>,
        root: Option<&Path>,
        progress: ProgressCallback,
        compression: CompressionFormatCallback,
        threads: usize,
    ) -> std::io::Result<Archive> {
        let archive_path = self.archive_path(name)?;
        if archive_path.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("archive {name} already exists"),
            ));
        }

        let _chunks_lock = Lock::shared(&self.chunks_lock_path())?;
        let _index_lock = Lock::exclusive(&self.index_lock_path())?;
        let index = ChunkIndex::load(&self.index_path())?;

        let root = root.unwrap_or(&self.directory);
        let walker = walker.unwrap_or_else(|| {
            ignore::WalkBuilder::new(root)
                .follow_links(false)
                .git_global(false)
                .build()
        });

        let cache = FileCache::load(&self.file_cache_path());
        let archive = Archive::new(crate::fs::create_new_file(&archive_path)?)?;
        let result = self
            .write_entries(
                archive,
                &index,
                &cache,
                walker,
                root,
                progress,
                compression,
                threads,
            )
            .and_then(|(mut archive, seen)| {
                self.storage.sync()?;
                index.save(&self.index_path())?;
                FileCache::save(&self.file_cache_path(), seen)?;
                archive.write_end_header()?;
                crate::fs::sync_dir(archive_path.parent().unwrap())?;
                Ok(archive)
            });

        if result.is_err() {
            let _ = std::fs::remove_file(&archive_path);
        }
        result
    }

    /// Returns the archive and the cache records of every file it contains.
    #[allow(clippy::too_many_arguments)]
    fn write_entries(
        &self,
        archive: Archive,
        index: &ChunkIndex,
        cache: &FileCache,
        walker: ignore::Walk,
        root: &Path,
        progress: ProgressCallback,
        compression: CompressionFormatCallback,
        threads: usize,
    ) -> std::io::Result<(Archive, Vec<(PathBuf, CachedFile)>)> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .map_err(std::io::Error::other)?;
        let job = Job {
            archive,
            index,
            cache,
            storage: &self.storage,
            compression: &compression,
            progress: &progress,
            chunk_size: self.chunk_size,
            max_chunk_count: self.max_chunk_count,
            files: Inflight::new(pool.current_num_threads() * 4),
            chunks: Inflight::new(pool.current_num_threads() * 2),
            error: Mutex::new(None),
            children: Mutex::new(HashMap::new()),
            seen: Mutex::new(Vec::new()),
        };

        pool.in_place_scope(|scope| {
            for item in walker {
                if job.error.lock().is_some() {
                    break;
                }

                let item = item.map_err(std::io::Error::other)?;
                let path = item.path();
                let relative = path.strip_prefix(root).map_err(|_| {
                    invalid(format!("{} is outside {}", path.display(), root.display()))
                })?;
                let Some(name) = relative.file_name() else {
                    continue;
                };
                let name = name
                    .to_str()
                    .ok_or_else(|| {
                        invalid(format!("{} is not a valid UTF-8 file name", path.display()))
                    })?
                    .to_owned();
                if name.starts_with(".ddup-bak") {
                    continue;
                }
                let parent = relative.parent().map(Path::to_path_buf).unwrap_or_default();

                let metadata = path.symlink_metadata()?;
                if metadata.is_file() {
                    job.files.acquire();
                    let (job, path, relative) = (&job, path.to_path_buf(), relative.to_path_buf());
                    scope.spawn(move |scope| {
                        record(
                            job.write_file(scope, &path, &relative, name, parent, metadata),
                            &job.error,
                        );
                        job.files.release();
                    });
                    continue;
                }

                let entry = if metadata.is_dir() {
                    Entry::Directory(Box::new(DirectoryEntry {
                        name,
                        mode: metadata.permissions().into(),
                        owner: owner(&metadata),
                        mtime: modified(&metadata),
                        entries: Vec::new(),
                    }))
                } else if metadata.is_symlink() {
                    let target = std::fs::read_link(path)?;
                    Entry::Symlink(Box::new(SymlinkEntry {
                        name,
                        mode: metadata.permissions().into(),
                        owner: owner(&metadata),
                        mtime: modified(&metadata),
                        target: target
                            .to_str()
                            .ok_or_else(|| {
                                invalid(format!("{} has a non-UTF-8 target", path.display()))
                            })?
                            .to_owned(),
                        target_dir: path.is_dir(),
                    }))
                } else {
                    continue;
                };
                job.add(path, parent, entry);
            }

            Ok::<(), std::io::Error>(())
        })?;

        if let Some(err) = job.error.into_inner() {
            return Err(err);
        }

        let mut archive = job.archive;
        archive.entries = assemble(PathBuf::new(), &mut job.children.into_inner());
        Ok((archive, job.seen.into_inner()))
    }

    /// Restores an archive into the repository's `.ddup-bak/archives-restored/<name>` directory,
    /// replacing whatever a previous restore left there, and returns that path.
    pub fn restore_archive(
        &self,
        name: &str,
        progress: ProgressCallback,
        threads: usize,
    ) -> std::io::Result<PathBuf> {
        let archive = self.get_archive(name)?;
        self.restore_entries(name, archive.into_entries(), progress, threads)
    }

    /// `restore_archive` for entries picked out of an archive.
    pub fn restore_entries(
        &self,
        name: &str,
        entries: Vec<Entry>,
        progress: ProgressCallback,
        threads: usize,
    ) -> std::io::Result<PathBuf> {
        let destination = self.restored_path(name)?;
        if destination.symlink_metadata().is_ok() {
            std::fs::remove_dir_all(&destination)?;
        }
        self.restore_entries_to(entries, &destination, progress, threads)?;
        Ok(destination)
    }

    pub fn restore_archive_to(
        &self,
        name: &str,
        destination: &Path,
        progress: ProgressCallback,
        threads: usize,
    ) -> std::io::Result<()> {
        let archive = self.get_archive(name)?;
        self.restore_entries_to(archive.into_entries(), destination, progress, threads)
    }

    /// Restores entries into `destination`, which is created if missing. Existing paths inside
    /// it are never overwritten or followed.
    pub fn restore_entries_to(
        &self,
        entries: Vec<Entry>,
        destination: &Path,
        progress: ProgressCallback,
        threads: usize,
    ) -> std::io::Result<()> {
        let _lock = Lock::shared(&self.chunks_lock_path())?;
        crate::fs::create_dir_all(destination)?;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .map_err(std::io::Error::other)?;
        let error = Mutex::new(None);

        pool.in_place_scope(|scope| {
            for entry in entries {
                let (storage, progress, error) = (&self.storage, &progress, &error);
                let algorithm = self.hash_algorithm;
                scope.spawn(move |_| {
                    record(
                        restore_entry(storage, algorithm, entry, destination, progress, error),
                        error,
                    );
                });
            }
        });

        error.into_inner().map_or(Ok(()), Err)
    }

    pub fn read_entry_content<W: Write>(
        &self,
        entry: Entry,
        stream: &mut W,
    ) -> std::io::Result<()> {
        std::io::copy(&mut self.entry_reader(entry)?, stream)?;
        Ok(())
    }

    pub fn entry_reader(&self, entry: Entry) -> std::io::Result<EntryReader> {
        let Entry::File(mut file) = entry else {
            return Err(invalid("entry is not a file"));
        };

        let lock = Lock::shared(&self.chunks_lock_path())?;
        Ok(EntryReader::new(
            chunks::entry_hashes(&mut file)?,
            Arc::clone(&self.storage),
            self.hash_algorithm,
            lock,
        ))
    }

    /// Archives this build cannot read, which is how a format 1 archive whose chunk ids the
    /// index no longer resolves is left behind. Their chunks are unaccounted for, so nothing
    /// may delete chunks while one is present.
    pub fn unreadable_archives(&self) -> std::io::Result<Vec<String>> {
        Ok(self
            .list_archives()?
            .into_iter()
            .filter(|name| self.get_archive(name).is_err())
            .collect())
    }

    fn refuse_deletion_while_unreadable(&self) -> std::io::Result<()> {
        let unreadable = self.unreadable_archives()?;
        if unreadable.is_empty() {
            return Ok(());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!(
                "cannot delete chunks while {} cannot be read: the chunks they hold are \
                 unaccounted for and would be taken as unreferenced. Open one of them to see \
                 what is wrong, or delete its file to give up on it",
                unreadable.join(", ")
            ),
        ))
    }

    pub fn delete_archive(
        &self,
        name: &str,
        progress: DeletionProgressCallback,
    ) -> std::io::Result<()> {
        let archive_path = self.archive_path(name)?;
        let _lock = Lock::exclusive(&self.chunks_lock_path())?;
        self.refuse_deletion_while_unreadable()?;
        let index = ChunkIndex::load(&self.index_path())?;

        let mut hashes = Vec::new();
        collect_hashes(self.get_archive(name)?.into_entries(), &mut hashes)?;
        std::fs::remove_file(&archive_path)?;

        hashes.into_par_iter().try_for_each(|hash| {
            let deleted = index.dereference(&hash) == 0;
            if deleted {
                self.delete_chunk(&hash)?;
                index.remove(&hash);
            }
            if let Some(progress) = &progress {
                progress(&hash, deleted);
            }
            Ok::<(), std::io::Error>(())
        })?;

        index.save(&self.index_path())
    }

    /// Deletes unreferenced chunks, including ones left behind by interrupted backups.
    pub fn clean(&self, progress: DeletionProgressCallback) -> std::io::Result<()> {
        let _lock = Lock::exclusive(&self.chunks_lock_path())?;
        self.refuse_deletion_while_unreadable()?;
        let index = ChunkIndex::load(&self.index_path())?;

        let orphans = self
            .storage
            .list_chunk_hashes()?
            .into_iter()
            .filter(|hash| !index.contains(hash));
        for hash in index.unreferenced().into_iter().chain(orphans) {
            self.delete_chunk(&hash)?;
            index.remove(&hash);
            if let Some(progress) = &progress {
                progress(&hash, true);
            }
        }

        index.save(&self.index_path())
    }

    fn delete_chunk(&self, hash: &ChunkHash) -> std::io::Result<()> {
        match self.storage.delete_chunk_content(hash) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            result => result,
        }
    }
}

fn restore_entry(
    storage: &Arc<dyn ChunkStorage>,
    algorithm: HashAlgorithm,
    entry: Entry,
    directory: &Path,
    progress: &ProgressCallback,
    error: &Mutex<Option<std::io::Error>>,
) -> std::io::Result<()> {
    let path = directory.join(entry.name());
    if let Some(progress) = progress {
        progress(&path);
    }

    match entry {
        Entry::File(mut file_entry) => {
            let mut file = crate::fs::create_new_file(&path)?;
            let hashes = chunks::entry_hashes(&mut file_entry)?;
            if hashes.len() > 1 {
                file.set_len(file_entry.size_real)?;
            }
            for window in hashes.chunks(RESTORE_WINDOW) {
                let data = match window {
                    [hash] => vec![chunks::read_chunk(&**storage, algorithm, hash)?],
                    _ => window
                        .par_iter()
                        .map(|hash| chunks::read_chunk(&**storage, algorithm, hash))
                        .collect::<std::io::Result<Vec<_>>>()?,
                };
                for chunk in data {
                    file.write_all(&chunk)?;
                }
            }

            file_entry.mode.apply(&path)?;
            file.set_times(FileTimes::new().set_modified(file_entry.mtime))?;
            chown(&path, file_entry.owner)
        }
        Entry::Directory(dir_entry) => {
            let DirectoryEntry {
                entries,
                mode,
                mtime,
                owner,
                ..
            } = *dir_entry;
            crate::fs::create_dir(&path)?;

            rayon::scope(|scope| {
                for child in entries {
                    let path = &path;
                    scope.spawn(move |_| {
                        record(
                            restore_entry(storage, algorithm, child, path, progress, error),
                            error,
                        );
                    });
                }
            });
            if error.lock().is_some() {
                return Ok(());
            }

            mode.apply(&path)?;
            File::open(&path)?.set_times(FileTimes::new().set_modified(mtime))?;
            chown(&path, owner)
        }
        Entry::Symlink(link_entry) => {
            symlink(&link_entry, &path)?;
            chown(&path, link_entry.owner)
        }
    }
}

fn record(result: std::io::Result<()>, error: &Mutex<Option<std::io::Error>>) {
    if let Err(err) = result {
        error.lock().get_or_insert(err);
    }
}

#[cfg(unix)]
fn process_is_running(pid: u32) -> bool {
    // Signal 0 checks for the process without delivering anything. Being told the signal is not
    // permitted still means it is there.
    let found = unsafe { libc::kill(pid as libc::pid_t, 0) } == 0;
    found || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn process_is_running(_pid: u32) -> bool {
    // No version old enough to use that lock ran anywhere but Unix.
    false
}

/// Copies a format 1 entry tree into `archive`, replacing chunk id lists with hash lists.
fn migrate_v1_entries(
    entries: Vec<Entry>,
    archive: &Archive,
    ids: &HashMap<u64, ChunkHash>,
) -> std::io::Result<Vec<Entry>> {
    entries
        .into_iter()
        .map(|entry| {
            Ok(match entry {
                Entry::File(mut file) => {
                    let hashes = chunks::entry_hashes_v1(&mut file, ids)?;
                    Entry::File(archive.write_raw_file_entry(
                        hashes.as_flattened(),
                        Some(file.size_real),
                        file.name,
                        file.mode,
                        file.mtime,
                        file.owner,
                    )?)
                }
                Entry::Directory(mut dir) => {
                    dir.entries =
                        migrate_v1_entries(std::mem::take(&mut dir.entries), archive, ids)?;
                    Entry::Directory(dir)
                }
                link @ Entry::Symlink(_) => link,
            })
        })
        .collect()
}

fn collect_hashes(entries: Vec<Entry>, hashes: &mut Vec<ChunkHash>) -> std::io::Result<()> {
    for entry in entries {
        match entry {
            Entry::File(mut file) => hashes.extend(chunks::entry_hashes(&mut file)?),
            Entry::Directory(dir) => collect_hashes(dir.entries, hashes)?,
            Entry::Symlink(_) => {}
        }
    }
    Ok(())
}

/// Nests entries collected per parent directory into a tree, starting at `dir`.
fn assemble(dir: PathBuf, children: &mut HashMap<PathBuf, Vec<Entry>>) -> Vec<Entry> {
    let mut entries = children.remove(&dir).unwrap_or_default();
    for entry in &mut entries {
        if let Entry::Directory(sub_dir) = entry {
            sub_dir.entries = assemble(dir.join(&sub_dir.name), children);
        }
    }
    entries
}

/// Shared state of one `create_archive` run. Files are chunked and hashed on worker threads;
/// new chunks are compressed and stored by further tasks when a slot is free, inline otherwise,
/// so no worker ever blocks waiting for another. Hash lists go into the archive with positioned
/// writes, so workers never contend on it.
struct Job<'a> {
    archive: Archive,
    index: &'a ChunkIndex,
    cache: &'a FileCache,
    storage: &'a Arc<dyn ChunkStorage>,
    compression: &'a CompressionFormatCallback,
    progress: &'a ProgressCallback,
    chunk_size: usize,
    max_chunk_count: usize,
    files: Inflight,
    chunks: Inflight,
    error: Mutex<Option<std::io::Error>>,
    children: Mutex<HashMap<PathBuf, Vec<Entry>>>,
    seen: Mutex<Vec<(PathBuf, CachedFile)>>,
}

impl Job<'_> {
    fn add(&self, path: &Path, parent: PathBuf, entry: Entry) {
        if let Some(progress) = self.progress {
            progress(path);
        }
        self.children.lock().entry(parent).or_default().push(entry);
    }

    /// `metadata` is the walker's lstat of `path`; it decides whether the cached chunk list can
    /// be reused without opening the file.
    fn write_file<'scope>(
        &'scope self,
        scope: &rayon::Scope<'scope>,
        path: &Path,
        relative: &Path,
        name: String,
        parent: PathBuf,
        metadata: Metadata,
    ) -> std::io::Result<()> {
        if self.error.lock().is_some() {
            return Ok(());
        }

        let fingerprint = Fingerprint::of(&metadata);
        let cached = self
            .cache
            .get(relative, fingerprint)
            .filter(|hashes| hashes.iter().all(|hash| self.index.contains(hash)));

        let (hashes, size, metadata) = match cached {
            Some(hashes) => {
                for hash in hashes {
                    self.index.reference(hash);
                }
                (hashes.to_vec(), metadata.len(), metadata)
            }
            None => {
                let file = open_nofollow(path)?;
                let metadata = file.metadata()?;
                if !metadata.is_file() {
                    return Err(std::io::Error::other(format!(
                        "{} changed while being read",
                        path.display()
                    )));
                }
                let (hashes, size) = self.chunk_file(scope, file, &metadata, relative)?;
                (hashes, size, metadata)
            }
        };

        let entry = self.archive.write_raw_file_entry(
            hashes.as_flattened(),
            Some(size),
            name,
            metadata.permissions().into(),
            modified(&metadata),
            owner(&metadata),
        )?;
        self.add(path, parent, Entry::File(entry));
        self.seen.lock().push((
            relative.to_path_buf(),
            CachedFile {
                fingerprint: Fingerprint::of(&metadata),
                hashes,
            },
        ));
        Ok(())
    }

    /// Chunks, hashes and stores `file`, returning its chunk hashes and byte count. Files no
    /// larger than one maximum chunk are read whole and cut in place, which needs one exact-size
    /// allocation and copies only chunks that are new; bigger files stream through FastCDC.
    fn chunk_file<'scope>(
        &'scope self,
        scope: &rayon::Scope<'scope>,
        file: File,
        metadata: &Metadata,
        relative: &Path,
    ) -> std::io::Result<(Vec<ChunkHash>, u64)> {
        let compression = self
            .compression
            .as_ref()
            .map_or(CompressionFormat::Deflate, |callback| {
                callback(relative, metadata)
            });
        let (min, avg, max) =
            chunks::cdc_parameters(self.chunk_size, self.max_chunk_count, metadata.len());
        let mut hashes = Vec::new();
        let mut size = 0u64;

        let mut data = Vec::new();
        if metadata.len() <= max as u64 {
            data.reserve_exact(metadata.len() as usize + 1);
            (&file).read_to_end(&mut data)?;
        }
        if metadata.len() <= max as u64 && data.len() <= max {
            for chunk in fastcdc::v2020::FastCDC::new(&data, min, avg, max) {
                let chunk = &data[chunk.offset..chunk.offset + chunk.length];
                size += chunk.len() as u64;
                hashes.push(self.store(scope, Cow::Borrowed(chunk), compression)?);
            }
        } else {
            // `data` holds whatever was read before the file turned out to be bigger than expected.
            let reader = Cursor::new(data).chain(file);
            for chunk in fastcdc::v2020::StreamCDC::new(reader, min, avg, max) {
                let data = chunk.map_err(cdc_error)?.data;
                size += data.len() as u64;
                hashes.push(self.store(scope, Cow::Owned(data), compression)?);
            }
        }

        Ok((hashes, size))
    }

    /// Hashes a chunk and stores it unless the index already has it.
    fn store<'scope>(
        &'scope self,
        scope: &rayon::Scope<'scope>,
        data: Cow<'_, [u8]>,
        compression: CompressionFormat,
    ) -> std::io::Result<ChunkHash> {
        let hash = self.index.hash_algorithm.hash(&data);
        if self.index.reference(&hash) > 1 {
            return Ok(hash);
        }

        if self.chunks.try_acquire() {
            let data = data.into_owned();
            scope.spawn(move |_| {
                let result = chunks::write_chunk(&**self.storage, &hash, &data, compression);
                record(result, &self.error);
                self.chunks.release();
            });
        } else {
            chunks::write_chunk(&**self.storage, &hash, &data, compression)?;
        }
        Ok(hash)
    }
}

/// Counting semaphore bounding queued work (and the memory it holds).
struct Inflight {
    limit: usize,
    count: Mutex<usize>,
    released: Condvar,
}

impl Inflight {
    fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            count: Mutex::new(0),
            released: Condvar::new(),
        }
    }

    fn acquire(&self) {
        let mut count = self.count.lock();
        while *count >= self.limit {
            self.released.wait(&mut count);
        }
        *count += 1;
    }

    fn try_acquire(&self) -> bool {
        let mut count = self.count.lock();
        if *count >= self.limit {
            return false;
        }
        *count += 1;
        true
    }

    fn release(&self) {
        *self.count.lock() -= 1;
        self.released.notify_one();
    }
}

fn open_nofollow(path: &Path) -> std::io::Result<File> {
    let mut options = File::options();
    options.read(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::custom_flags(&mut options, libc::O_NOFOLLOW);
    options.open(path)
}

fn modified(metadata: &Metadata) -> SystemTime {
    metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH)
}

fn owner(_metadata: &Metadata) -> (u32, u32) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        (_metadata.uid(), _metadata.gid())
    }
    #[cfg(not(unix))]
    (0, 0)
}

/// Restores ownership when the process is allowed to; unprivileged restores keep the restoring
/// user as owner instead of failing.
#[cfg(unix)]
fn chown(path: &Path, (uid, gid): (u32, u32)) -> std::io::Result<()> {
    match std::os::unix::fs::lchown(path, Some(uid), Some(gid)) {
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => Ok(()),
        result => result,
    }
}

#[cfg(not(unix))]
fn chown(_path: &Path, _owner: (u32, u32)) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn symlink(link: &SymlinkEntry, path: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(&link.target, path)
}

#[cfg(windows)]
fn symlink(link: &SymlinkEntry, path: &Path) -> std::io::Result<()> {
    if link.target_dir {
        std::os::windows::fs::symlink_dir(&link.target, path)
    } else {
        std::os::windows::fs::symlink_file(&link.target, path)
    }
}

fn cdc_error(err: fastcdc::v2020::Error) -> std::io::Error {
    match err {
        fastcdc::v2020::Error::IoError(err) => err,
        other => std::io::Error::other(other.to_string()),
    }
}

fn invalid(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}
