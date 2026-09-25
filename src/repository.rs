use crate::{
    archive::{
        Archive, CompressionFormat, CompressionFormatCallback, FILE_VERSION, ProgressCallback,
        entries::{DirectoryEntry, Entry, SymlinkEntry},
        metadata_owner,
    },
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

const RESTORE_WINDOW: usize = 4;

pub type DeletionProgressCallback = Option<Arc<dyn Fn(&ChunkHash, bool) + Send + Sync + 'static>>;
pub type RebuildProgressCallback = Option<Arc<dyn Fn(&ChunkHash, u64) + Send + Sync + 'static>>;

pub struct Repository {
    pub directory: PathBuf,
    chunks_directory: PathBuf,
    storage: Arc<dyn ChunkStorage>,
    chunk_size: usize,
    max_chunk_count: usize,
    hash_algorithm: HashAlgorithm,
}

impl Repository {
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
        for sub in ["archives", "archives-restored", "deleting", "chunks"] {
            std::fs::create_dir_all(base.join(sub))?;
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

    /// Opens an existing repository.
    /// The repository must be initialized with `new` before use.
    /// The repository directory must contain a `.ddup-bak` directory.
    /// Repositories from before archive format 2 are migrated in place first.
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
            repository.migrate_v1(false)?;
        } else if repository.legacy_ids_path().exists() {
            repository.finish_migration()?;
        }
        // Best effort, so a later delete on a full disk doesn't have to create it.
        let _ = std::fs::create_dir_all(repository.directory.join(".ddup-bak/deleting"));
        Ok(repository)
    }

    /// Rebuilds a corrupted repository by scanning archives and chunk storage.
    ///
    /// Use this when `open()` fails because the chunk index is corrupt or
    /// missing (e.g. after a disk-full event).
    ///
    /// After all archives are counted, `progress` is called once per chunk with its final
    /// reference count, including chunks nothing references any more.
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
        std::fs::create_dir_all(&repository.chunks_directory)?;

        // Format 1 archives need the old index to resolve, so migrate while it still exists.
        match ChunkIndex::load_header(&repository.index_path()) {
            Ok(header) if header.version == 1 => repository.migrate_v1(true)?,
            Ok(_) => {}
            // Missing or unreadable index: nothing to migrate.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound || chunks::is_damage(&err) => {}
            Err(err) => return Err(err),
        }

        let _lock = Lock::exclusive(&repository.chunks_lock_path())?;
        repository.hash_algorithm = match chunks::detect_hash_algorithm(&*repository.storage)? {
            Some(algorithm) => algorithm,
            // No chunks to detect from, so keep the index's algorithm.
            None => match ChunkIndex::load_header(&repository.index_path()) {
                Ok(header) => header.hash_algorithm,
                Err(err)
                    if err.kind() == std::io::ErrorKind::NotFound || chunks::is_damage(&err) =>
                {
                    HashAlgorithm::default()
                }
                Err(err) => return Err(err),
            },
        };

        let index = ChunkIndex::rebuild(
            chunk_size,
            max_chunk_count,
            repository.hash_algorithm,
            &*repository.storage,
            repository.countable_archives()?,
            |hash, references| {
                if let Some(progress) = &progress {
                    progress(hash, references);
                }
            },
        )?;
        // The new index excludes archives in `deleting`, so persist those moves first.
        repository.sync_archive_dirs()?;
        index.save(&repository.index_path())?;
        for marker in repository.pending_markers()? {
            std::fs::remove_file(marker)?;
        }

        Ok(repository)
    }

    /// Rewrites format 1 archives and the index to the current format. Safe to rerun after an
    /// interruption. With `salvage`, a damaged index is read as far as it decodes.
    fn migrate_v1(&self, salvage: bool) -> std::io::Result<()> {
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
        let header = ChunkIndex::load_header(&self.index_path())?;
        if header.version != 1 {
            return Ok(());
        }
        let (_, ids) = if salvage {
            ChunkIndex::salvage_v1(&self.index_path())
        } else {
            ChunkIndex::load_v1(&self.index_path())
        }?;

        let remaining = self.migrate_archives(&ids)?;

        // `rebuild` runs next and writes the index itself.
        if salvage {
            return Ok(());
        }
        // Keep the old index for archives that failed to migrate.
        if remaining {
            std::fs::rename(self.index_path(), self.legacy_ids_path())?;
        }
        // Recount: interrupted backups of that era left the old counts too low.
        ChunkIndex::rebuild(
            header.chunk_size,
            header.max_chunk_count,
            header.hash_algorithm,
            &*self.storage,
            self.countable_archives()?,
            |_, _| {},
        )?
        .save(&self.index_path())
    }

    /// Migrates every format 1 archive through `ids`, skipping damaged ones. Returns whether any
    /// were skipped.
    fn migrate_archives(&self, ids: &HashMap<u64, ChunkHash>) -> std::io::Result<bool> {
        let mut remaining = false;
        for name in self.list_archives()? {
            let path = self.archive_path(&name)?;
            let archive = match Archive::open(&path) {
                Ok(archive) => archive,
                Err(err) if chunks::is_damage(&err) => {
                    remaining = true;
                    continue;
                }
                Err(err) => return Err(err),
            };
            if archive.version() != 1 {
                continue;
            }

            // Hashed name: fits any archive name, and replaces a leftover from an earlier run.
            let tmp_path = path.with_file_name(format!(
                ".migrate-{}",
                &chunks::hex(blake3::hash(name.as_bytes()).as_bytes())[..16]
            ));
            let _ = std::fs::remove_file(&tmp_path);
            let mut migrated = Archive::new(create_new_file(&tmp_path)?)?;
            let result = migrate_v1_entries(archive.into_entries(), &mut migrated, ids)
                .and_then(|entries| {
                    migrated.entries = entries;
                    migrated.write_end_header()
                })
                .and_then(|()| std::fs::rename(&tmp_path, &path));
            if let Err(err) = result {
                let _ = std::fs::remove_file(&tmp_path);
                if !chunks::is_damage(&err) {
                    return Err(err);
                }
                remaining = true;
            }
        }
        chunks::sync_dir(&self.directory.join(".ddup-bak/archives"))?;
        Ok(remaining)
    }

    /// Migrates archives skipped earlier as damaged, then removes the old index once none remain.
    fn finish_migration(&self) -> std::io::Result<()> {
        let _chunks_lock = Lock::exclusive(&self.chunks_lock_path())?;
        let _index_lock = Lock::exclusive(&self.index_lock_path())?;
        let ids = match ChunkIndex::load_v1(&self.legacy_ids_path()) {
            Ok((_, ids)) => ids,
            Err(err) if chunks::is_damage(&err) => return Ok(()),
            Err(err) => return Err(err),
        };
        if self.migrate_archives(&ids)? {
            return Ok(());
        }
        self.recount()?;
        std::fs::remove_file(self.legacy_ids_path())
    }

    /// Old format 1 index, kept while unmigrated archives need it.
    fn legacy_ids_path(&self) -> PathBuf {
        self.chunks_directory.join("index.v1")
    }

    /// Recounts all references from the archives and saves the index, settling pending deletions.
    /// Needs the chunks lock held exclusively.
    fn recount(&self) -> std::io::Result<()> {
        let (index, markers) = self.recounted(self.pending_markers()?)?;
        self.save_settled(&index, markers)
    }

    /// Archives to count references from, skipping damaged and unmigrated ones.
    fn countable_archives(
        &self,
    ) -> std::io::Result<impl Iterator<Item = std::io::Result<Archive>> + '_> {
        let mut names = Vec::new();
        for name in self.list_archives()? {
            match self.readability(&name)? {
                Readability::Whole => names.push(name),
                Readability::Damaged => {}
                // Skipping it would undercount chunks a build with the feature still needs.
                Readability::Unsupported => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        format!(
                            "archive {name} uses a compression this build was made without, \
                             so its chunks cannot be counted; use a build that has it"
                        ),
                    ));
                }
            }
        }
        Ok(names.into_iter().map(move |name| self.get_archive(&name)))
    }

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

    /// No-op kept for compatibility; every operation saves the index itself.
    pub fn save(&self) -> std::io::Result<()> {
        Ok(())
    }

    /// No-op kept for compatibility; the index is never held back until drop.
    #[inline]
    pub const fn set_save_on_drop(&mut self, _save_on_drop: bool) -> &mut Self {
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

    /// Opens a repository, falling back to rebuild if the index is corrupt.
    ///
    /// Tries `open()` first. If that fails with an I/O error (corrupt or
    /// missing index), automatically runs `rebuild()`. This is the
    /// recommended entry point for applications that want resilience.
    pub fn open_or_rebuild(
        directory: &Path,
        chunk_size: usize,
        max_chunk_count: usize,
        chunks_directory: Option<&Path>,
        storage: Option<Arc<dyn ChunkStorage>>,
        progress: RebuildProgressCallback,
    ) -> std::io::Result<Self> {
        // `open` only reads the header, so load the whole index to check it.
        let opened = Self::open(directory, chunks_directory, storage.clone())
            .and_then(|repository| ChunkIndex::load(&repository.index_path()).map(|_| repository));
        match opened {
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

    #[inline]
    pub fn archive_path_parent<'a>(
        archive: &'a mut Archive,
        entry: &Path,
    ) -> Option<&'a mut Box<DirectoryEntry>> {
        archive
            .find_archive_entry_mut(entry.parent()?)
            .and_then(|e| match e {
                Entry::Directory(dir) => Some(dir),
                _ => None,
            })
    }

    /// Blocks chunk deletion while held, for keeping archive entries across calls.
    pub fn shared_lock(&self) -> std::io::Result<Lock> {
        Lock::shared(&self.chunks_lock_path())
    }

    fn chunks_lock_path(&self) -> PathBuf {
        self.chunks_directory.join("chunks.lock")
    }

    fn index_lock_path(&self) -> PathBuf {
        self.chunks_directory.join("index.lock")
    }

    #[inline]
    pub fn archive_path(&self, name: &str) -> std::io::Result<PathBuf> {
        crate::archive::validate_name(name, 255)?;
        Ok(self
            .directory
            .join(".ddup-bak/archives")
            .join(format!("{name}.ddup")))
    }

    /// Lists all archives in the repository.
    /// Returns a vector of archive names without the ".ddup" extension.
    /// Example: "my_archive" instead of "my_archive.ddup".
    /// The archives are stored in the ".ddup-bak/archives" directory.
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

    /// Gets an archive by name.
    /// Do not use this method to extract data, the data is chunked and compressed.
    /// Use `restore_archive` or `entry_reader` instead.
    pub fn get_archive(&self, name: &str) -> std::io::Result<Archive> {
        let archive = Archive::open(self.archive_path(name)?)?;
        if archive.version() != FILE_VERSION {
            // Opening the repository migrates format 1 archives, so one here means its index was
            // unreadable.
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
        let _chunks_lock = Lock::shared(&self.chunks_lock_path())?;
        let _index_lock = Lock::exclusive(&self.index_lock_path())?;
        // Checked under the writer lock so concurrent creators see each other's archives. A
        // dangling symlink counts as taken.
        match archive_path.symlink_metadata() {
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("archive {name} already exists"),
                ));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        let index = ChunkIndex::load(&self.index_path())?;

        let root = root.unwrap_or(&self.directory);
        let walker = walker.unwrap_or_else(|| {
            ignore::WalkBuilder::new(root)
                .follow_links(false)
                .git_global(false)
                .build()
        });

        // Written under a hidden name and renamed when done, so a failed backup leaves no archive.
        let partial = archive_path.with_file_name(format!(
            ".partial-{}",
            &chunks::hex(blake3::hash(name.as_bytes()).as_bytes())[..16]
        ));
        let _ = std::fs::remove_file(&partial);
        let file = create_new_file(&partial)?;
        let handle = file.try_clone()?;
        let archive = match Archive::new(file) {
            Ok(archive) => archive,
            Err(err) => {
                let _ = std::fs::remove_file(&partial);
                return Err(err);
            }
        };
        let result = self
            .write_entries(
                archive,
                &index,
                walker,
                root,
                progress,
                compression,
                threads,
            )
            .and_then(|mut archive| {
                index.save(&self.index_path())?;
                archive.write_end_header()?;
                handle.sync_all()?;
                std::fs::rename(&partial, &archive_path)?;
                chunks::sync_dir(archive_path.parent().unwrap())?;
                Ok(archive)
            });

        if result.is_err() {
            let _ = std::fs::remove_file(&partial);
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn write_entries(
        &self,
        archive: Archive,
        index: &ChunkIndex,
        walker: ignore::Walk,
        root: &Path,
        progress: ProgressCallback,
        compression: CompressionFormatCallback,
        threads: usize,
    ) -> std::io::Result<Archive> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .map_err(std::io::Error::other)?;
        let job = Job {
            archive: Mutex::new(archive),
            index,
            storage: &self.storage,
            compression: &compression,
            progress: &progress,
            chunk_size: self.chunk_size,
            max_chunk_count: self.max_chunk_count,
            files: Inflight::new(pool.current_num_threads() * 4),
            chunks: Inflight::new(pool.current_num_threads() * 2),
            error: Mutex::new(None),
            children: Mutex::new(HashMap::new()),
        };

        pool.in_place_scope(|scope| {
            for item in walker {
                if job.error.lock().is_some() {
                    break;
                }

                let item = match item {
                    Ok(item) => item,
                    // Errors inside the repository's own directories are skipped along with them.
                    Err(err)
                        if walk_error_path(&err).is_some_and(|path| {
                            path.strip_prefix(root).is_ok_and(is_repository_internal)
                        }) =>
                    {
                        continue;
                    }
                    Err(err) => return Err(std::io::Error::other(err)),
                };
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
                // Checked per path: the walker still descends into directories we skip.
                if is_repository_internal(relative) {
                    continue;
                }
                // Refuse what the reader would reject, before publishing.
                let max_depth = crate::archive::DecodeLimits::default().max_depth;
                if relative.iter().count() > max_depth {
                    return Err(invalid(format!(
                        "{} is nested deeper than the {max_depth} levels an archive can hold",
                        path.display()
                    )));
                }
                let parent = relative.parent().map(Path::to_path_buf).unwrap_or_default();

                let metadata = path.symlink_metadata()?;
                if metadata.is_file() {
                    job.files.acquire();
                    let (job, path, relative) = (&job, path.to_path_buf(), relative.to_path_buf());
                    scope.spawn(move |scope| {
                        record(
                            job.write_file(scope, &path, &relative, name, parent),
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
                        owner: metadata_owner(&metadata),
                        mtime: modified(&metadata),
                        entries: Vec::new(),
                    }))
                } else if metadata.is_symlink() {
                    let target = std::fs::read_link(path)?;
                    Entry::Symlink(Box::new(SymlinkEntry {
                        name,
                        mode: metadata.permissions().into(),
                        owner: metadata_owner(&metadata),
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

        let mut archive = job.archive.into_inner();
        archive.entries = assemble(PathBuf::new(), &mut job.children.into_inner());
        Ok(archive)
    }

    /// Restores an archive into the repository's `.ddup-bak/archives-restored/<name>` directory,
    /// replacing whatever a previous restore left there, and returns that path.
    ///
    /// Each entry is reported with its final path, once to `progress` before it is created and
    /// once to `restored` after it is fully restored. A directory counts as restored after all its
    /// children and its own metadata, so children are reported to `restored` before their parent.
    /// Entries that fail are never reported to `restored`. Calls come from several threads, so
    /// the order between unrelated entries is unspecified.
    pub fn restore_archive(
        &self,
        name: &str,
        progress: ProgressCallback,
        restored: ProgressCallback,
        threads: usize,
    ) -> std::io::Result<PathBuf> {
        // Locked before reading the archive so a delete can't run in between.
        let _lock = Lock::shared(&self.chunks_lock_path())?;
        let archive = self.get_archive(name)?;
        self.restore_entries(name, archive.into_entries(), progress, restored, threads)
    }

    /// `restore_archive` for entries picked out of an archive.
    pub fn restore_entries(
        &self,
        name: &str,
        entries: Vec<Entry>,
        progress: ProgressCallback,
        restored: ProgressCallback,
        threads: usize,
    ) -> std::io::Result<PathBuf> {
        let destination = self.restored_path(name)?;
        self.restore_entries_replacing(entries, &destination, progress, restored, threads)?;
        Ok(destination)
    }

    /// Restores into staging, then swaps it into `destination`. Existing entries are kept until
    /// the swap succeeds and moved back if it fails. `.ddup-bak` and `.ddup-bak-restore*` entries
    /// are left alone. Not atomic against other writers or a crash.
    ///
    /// Callbacks work as in `restore_archive`. They report paths in `destination`, but entries
    /// are only moved there from staging once all of them are restored, before this returns.
    pub fn restore_entries_replacing(
        &self,
        entries: Vec<Entry>,
        destination: &Path,
        progress: ProgressCallback,
        restored: ProgressCallback,
        threads: usize,
    ) -> std::io::Result<()> {
        let _lock = Lock::shared(&self.chunks_lock_path())?;
        match destination.symlink_metadata() {
            Ok(metadata) if !metadata.is_dir() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "restore destination must be a directory, not a file or symlink",
                ));
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        std::fs::create_dir_all(destination)?;
        let reported = destination.to_path_buf();
        let destination = destination.canonicalize()?;
        let locks = self.directory.join(".ddup-bak/restore-locks");
        std::fs::create_dir_all(&locks)?;
        let key = chunks::hex(blake3::hash(destination.as_os_str().as_encoded_bytes()).as_bytes());
        let _turn = Lock::exclusive(&locks.join(format!("destination-{}", &key[..16])))?;
        let mut staging = crate::restore::StagedRestore::new(&destination)?;
        let staged = staging.path();
        // Report where entries end up, as the caller named it, not where they are staged.
        let published = |callback: ProgressCallback| {
            callback.map(|callback| {
                let (staged, reported) = (staged.clone(), reported.clone());
                Arc::new(move |path: &Path| {
                    callback(&reported.join(path.strip_prefix(&staged).unwrap_or(path)))
                }) as Arc<dyn Fn(&Path) + Send + Sync>
            })
        };
        self.restore_entries_to(
            entries,
            &staged,
            published(progress),
            published(restored),
            threads,
        )?;
        staging.publish(&destination)
    }

    /// Restores an archive into `destination`. Callbacks work as in `restore_archive`.
    pub fn restore_archive_to(
        &self,
        name: &str,
        destination: &Path,
        progress: ProgressCallback,
        restored: ProgressCallback,
        threads: usize,
    ) -> std::io::Result<()> {
        let _lock = Lock::shared(&self.chunks_lock_path())?;
        let archive = self.get_archive(name)?;
        self.restore_entries_to(
            archive.into_entries(),
            destination,
            progress,
            restored,
            threads,
        )
    }

    /// Restores entries into `destination`, creating it if missing. Never overwrites or follows
    /// existing paths. Callbacks work as in `restore_archive`.
    pub fn restore_entries_to(
        &self,
        entries: Vec<Entry>,
        destination: &Path,
        progress: ProgressCallback,
        restored: ProgressCallback,
        threads: usize,
    ) -> std::io::Result<()> {
        let _lock = Lock::shared(&self.chunks_lock_path())?;
        std::fs::create_dir_all(destination)?;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .map_err(std::io::Error::other)?;
        let error = Mutex::new(None);

        pool.in_place_scope(|scope| {
            for entry in entries {
                let (storage, progress, restored, error) =
                    (&self.storage, &progress, &restored, &error);
                let algorithm = self.hash_algorithm;
                scope.spawn(move |_| {
                    record(
                        restore_entry(
                            storage,
                            algorithm,
                            entry,
                            destination,
                            progress,
                            restored,
                            error,
                        ),
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
        // Held only for this call, so a concurrent delete waits instead of failing.
        let lock = Lock::shared(&self.chunks_lock_path())?;
        std::io::copy(&mut self.entry_reader_with(entry, lock)?, stream)?;
        Ok(())
    }

    /// Opens a streaming reader over a file entry's content. While the reader is open, `clean`,
    /// `delete_archive` and `rebuild` in this process fail with `WouldBlock`.
    pub fn entry_reader(&self, entry: Entry) -> std::io::Result<EntryReader> {
        let lock = Lock::reader(&self.chunks_lock_path())?;
        self.entry_reader_with(entry, lock)
    }

    fn entry_reader_with(&self, entry: Entry, lock: Lock) -> std::io::Result<EntryReader> {
        let Entry::File(mut file) = entry else {
            return Err(invalid("entry is not a file"));
        };
        Ok(EntryReader::new(
            chunks::entry_hashes(&mut file)?,
            file.size_real,
            Arc::clone(&self.storage),
            self.hash_algorithm,
            lock,
        ))
    }

    pub fn unreadable_archives(&self) -> std::io::Result<Vec<String>> {
        let mut unreadable = Vec::new();
        for name in self.list_archives()? {
            if self.readability(&name)? != Readability::Whole {
                unreadable.push(name);
            }
        }
        Ok(unreadable)
    }

    /// How much of an archive this build can read.
    fn readability(&self, name: &str) -> std::io::Result<Readability> {
        let archive = match self.get_archive(name) {
            Ok(archive) => archive,
            Err(err)
                if chunks::is_damage(&err) || err.kind() == std::io::ErrorKind::Unsupported =>
            {
                return Ok(Readability::Damaged);
            }
            Err(err) => return Err(err),
        };
        match collect_hashes(archive.into_entries(), &mut Vec::new()) {
            Ok(()) => Ok(Readability::Whole),
            Err(err) if chunks::is_damage(&err) => Ok(Readability::Damaged),
            Err(err) if err.kind() == std::io::ErrorKind::Unsupported => {
                Ok(Readability::Unsupported)
            }
            Err(err) => Err(err),
        }
    }

    fn refuse_deletion(unreadable: &[String]) -> std::io::Result<()> {
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

    /// Moves the archive to `.ddup-bak/deleting`, deletes the chunks only it used, then saves the
    /// index. Deleting first frees space even on a full disk. `settle_pending` finishes interrupted
    /// deletes.
    ///
    /// `progress` is called once per chunk the archive uses, with whether it was deleted, and once
    /// for each chunk deleted while finishing interrupted deletes. Calls come from several threads.
    pub fn delete_archive(
        &self,
        name: &str,
        progress: DeletionProgressCallback,
    ) -> std::io::Result<()> {
        let archive_path = self.archive_path(name)?;
        let marker = self.deleting_path(name);
        let _lock = Lock::exclusive(&self.chunks_lock_path())?;
        let live = archive_path.exists();
        if !live
            && !self
                .pending_deletions()?
                .iter()
                .any(|pending| pending == name)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("archive {name} does not exist"),
            ));
        }
        let unreadable = self.unreadable_archives()?;
        // Its references can't be read, so drop the file and recount everything.
        if live && unreadable.iter().any(|unreadable| unreadable == name) {
            std::fs::remove_file(&archive_path)?;
            self.sync_archive_dirs()?;
            return self.recount();
        }
        Self::refuse_deletion(&unreadable)?;
        let index = ChunkIndex::load(&self.index_path())?;
        let (index, mut markers) = self.settle_pending(index, &progress)?;

        if live {
            let mut hashes = Vec::new();
            collect_hashes(self.get_archive(name)?.into_entries(), &mut hashes)?;
            std::fs::create_dir_all(marker.parent().unwrap())?;
            let marker = self.free_marker(name, marker);
            std::fs::rename(&archive_path, &marker)?;
            self.sync_archive_dirs()?;
            markers.push(marker);

            let mut counts: HashMap<ChunkHash, u64> = HashMap::new();
            for hash in hashes {
                *counts.entry(hash).or_default() += 1;
            }
            let deletions: Vec<_> = counts
                .into_iter()
                .map(|(hash, count)| {
                    let mut remaining = 0;
                    for _ in 0..count {
                        remaining = index.dereference(&hash);
                    }
                    let deleted = remaining == 0;
                    if deleted {
                        index.remove(&hash);
                    }
                    (hash, deleted)
                })
                .collect();
            deletions.into_par_iter().try_for_each(|(hash, deleted)| {
                if deleted {
                    self.delete_chunk(&hash)?;
                }
                if let Some(progress) = &progress {
                    progress(&hash, deleted);
                }
                Ok::<(), std::io::Error>(())
            })?;
        }

        self.save_settled(&index, markers)
    }

    /// A free path in `deleting`: `marker`, else `<name>.ddup.1`, `.2` and so on.
    fn free_marker(&self, name: &str, marker: PathBuf) -> PathBuf {
        let dir = marker.parent().unwrap().to_path_buf();
        let mut candidate = marker;
        let mut n = 1;
        while candidate.exists() {
            // Drop the name rather than truncate it; a truncated name could match another archive.
            let suffix = format!(".ddup.{n}");
            let stem = if name.len() + suffix.len() <= 255 {
                name
            } else {
                ""
            };
            candidate = dir.join(format!("{stem}{suffix}"));
            n += 1;
        }
        candidate
    }

    fn deleting_path(&self, name: &str) -> PathBuf {
        self.directory
            .join(".ddup-bak/deleting")
            .join(format!("{name}.ddup"))
    }

    /// Archives whose deletion was interrupted.
    pub fn pending_deletions(&self) -> std::io::Result<Vec<String>> {
        let mut names: Vec<String> = self
            .pending_markers()?
            .iter()
            .filter_map(|marker| pending_name(marker))
            .filter(|name| !name.is_empty())
            .collect();
        names.sort();
        names.dedup();
        Ok(names)
    }

    /// Archive files in `deleting` awaiting settlement.
    fn pending_markers(&self) -> std::io::Result<Vec<PathBuf>> {
        let entries = match std::fs::read_dir(self.directory.join(".ddup-bak/deleting")) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            entries => entries?,
        };
        let mut markers = Vec::new();
        for entry in entries {
            let path = entry?.path();
            if pending_name(&path).is_some() {
                markers.push(path);
            }
        }
        Ok(markers)
    }

    /// Persists moves between `archives` and `deleting` before chunks or the index change.
    fn sync_archive_dirs(&self) -> std::io::Result<()> {
        chunks::sync_dir(&self.directory.join(".ddup-bak/archives"))?;
        let deleting = self.directory.join(".ddup-bak/deleting");
        if deleting.is_dir() {
            chunks::sync_dir(&deleting)?;
        }
        Ok(())
    }

    /// Finishes interrupted deletions: recounts their chunks from the remaining archives, deletes
    /// unreferenced ones, reporting each to `progress`, and returns the markers to remove after
    /// the caller saves the index. Needs the chunks lock held exclusively.
    fn settle_pending(
        &self,
        index: ChunkIndex,
        progress: &DeletionProgressCallback,
    ) -> std::io::Result<(ChunkIndex, Vec<PathBuf>)> {
        let markers = self.pending_markers()?;
        if markers.is_empty() {
            return Ok((index, Vec::new()));
        }

        let mut hashes = Vec::new();
        for path in &markers {
            // An unreadable marker hides which chunks it held, so recount everything.
            let unreadable = |err: &std::io::Error| {
                chunks::is_damage(err) || err.kind() == std::io::ErrorKind::Unsupported
            };
            let entries = match Archive::open(path) {
                Ok(archive) => archive.into_entries(),
                Err(err) if unreadable(&err) => return self.recounted(markers),
                Err(err) => return Err(named(path, err)),
            };
            match collect_hashes(entries, &mut hashes) {
                Ok(()) => {}
                Err(err) if unreadable(&err) => return self.recounted(markers),
                Err(err) => return Err(named(path, err)),
            }
        }
        let mut counts: HashMap<ChunkHash, u64> = hashes.into_iter().map(|h| (h, 0)).collect();
        for name in self.list_archives()? {
            let mut referenced = Vec::new();
            collect_hashes(self.get_archive(&name)?.into_entries(), &mut referenced)?;
            for hash in referenced {
                if let Some(count) = counts.get_mut(&hash) {
                    *count += 1;
                }
            }
        }
        // The interrupted attempt may not have synced these moves.
        self.sync_archive_dirs()?;
        for (hash, count) in counts {
            if count == 0 {
                self.delete_chunk(&hash)?;
                index.remove(&hash);
                if let Some(progress) = progress {
                    progress(&hash, true);
                }
            } else {
                index.set(&hash, count);
            }
        }
        Ok((index, markers))
    }

    /// A fully recounted index, with `markers` to remove once it is saved.
    fn recounted(&self, markers: Vec<PathBuf>) -> std::io::Result<(ChunkIndex, Vec<PathBuf>)> {
        let header = ChunkIndex::load_header(&self.index_path())?;
        self.sync_archive_dirs()?;
        let index = ChunkIndex::rebuild(
            header.chunk_size,
            header.max_chunk_count,
            self.hash_algorithm,
            &*self.storage,
            self.countable_archives()?,
            |_, _| {},
        )?;
        Ok((index, markers))
    }

    fn save_settled(&self, index: &ChunkIndex, mut markers: Vec<PathBuf>) -> std::io::Result<()> {
        index.save(&self.index_path())?;
        // Nameless markers go first, so a crash leaves named ones that can be retried.
        markers.sort_by_key(|marker| pending_name(marker).is_some_and(|name| !name.is_empty()));
        for marker in markers {
            std::fs::remove_file(marker)?;
        }
        Ok(())
    }

    /// Deletes unreferenced chunks, including ones left behind by interrupted backups and deletes.
    /// `progress` is called once per deleted chunk, always with `deleted` set.
    pub fn clean(&self, progress: DeletionProgressCallback) -> std::io::Result<()> {
        let _lock = Lock::exclusive(&self.chunks_lock_path())?;
        Self::refuse_deletion(&self.unreadable_archives()?)?;
        let index = ChunkIndex::load(&self.index_path())?;
        let (index, markers) = self.settle_pending(index, &progress)?;

        self.storage.remove_leftovers()?;
        self.remove_partial_archives()?;
        // Collected before index removals, which would make those chunks orphans too.
        let orphans: Vec<_> = self
            .storage
            .list_chunk_hashes()?
            .into_iter()
            .filter(|hash| !index.contains(hash))
            .collect();
        for hash in index.unreferenced().into_iter().chain(orphans) {
            self.delete_chunk(&hash)?;
            index.remove(&hash);
            if let Some(progress) = &progress {
                progress(&hash, true);
            }
        }

        self.save_settled(&index, markers)
    }

    /// Removes half-written archives left by interrupted backups and migrations.
    fn remove_partial_archives(&self) -> std::io::Result<()> {
        for entry in std::fs::read_dir(self.directory.join(".ddup-bak/archives"))? {
            let entry = entry?;
            let name = entry.file_name();
            let leftover = name
                .to_str()
                .is_some_and(|name| name.starts_with(".partial-") || name.starts_with(".migrate-"));
            if leftover && entry.file_type()?.is_file() {
                std::fs::remove_file(entry.path())?;
            }
        }
        Ok(())
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
    restored: &ProgressCallback,
    error: &Mutex<Option<std::io::Error>>,
) -> std::io::Result<()> {
    let path = directory.join(entry.name());
    if let Some(progress) = progress {
        progress(&path);
    }

    match entry {
        Entry::File(mut file_entry) => {
            let mut file = File::create_new(&path)?;
            let hashes = chunks::entry_hashes(&mut file_entry)?;
            if hashes.len() > 1 {
                file.set_len(file_entry.size_real)?;
            }
            let mut written = 0;
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
                    written += chunk.len() as u64;
                }
            }
            // Fewer bytes than recorded would leave zeros in the file.
            if written != file_entry.size_real {
                return Err(invalid(format!(
                    "{} restored {written} bytes of the {} recorded",
                    path.display(),
                    file_entry.size_real
                )));
            }

            // chown can clear setuid/setgid, so permissions go last.
            chown(&path, file_entry.owner)?;
            file.set_times(FileTimes::new().set_modified(file_entry.mtime))?;
            std::fs::set_permissions(&path, file_entry.mode.into())
        }
        Entry::Directory(dir_entry) => {
            let DirectoryEntry {
                entries,
                mode,
                mtime,
                owner,
                ..
            } = *dir_entry;
            std::fs::create_dir(&path)?;

            rayon::scope(|scope| {
                for child in entries {
                    let path = &path;
                    scope.spawn(move |_| {
                        record(
                            restore_entry(
                                storage, algorithm, child, path, progress, restored, error,
                            ),
                            error,
                        );
                    });
                }
            });
            if error.lock().is_some() {
                return Ok(());
            }

            // Open before applying the mode, which may deny opening it.
            let directory = open_dir(&path)?;
            chown(&path, owner)?;
            directory.set_times(FileTimes::new().set_modified(mtime))?;
            std::fs::set_permissions(&path, mode.into())
        }
        Entry::Symlink(link_entry) => {
            symlink(&link_entry, &path)?;
            chown(&path, link_entry.owner)
        }
    }?;
    if let Some(restored) = restored {
        restored(&path);
    }
    Ok(())
}

fn record(result: std::io::Result<()>, error: &Mutex<Option<std::io::Error>>) {
    if let Err(err) = result {
        error.lock().get_or_insert(err);
    }
}

#[cfg(unix)]
fn process_is_running(pid: u32) -> bool {
    // Signal 0 only checks existence; EPERM still means the process exists.
    let found = unsafe { libc::kill(pid as libc::pid_t, 0) } == 0;
    found || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn process_is_running(_pid: u32) -> bool {
    // Versions with the old lock only ran on Unix.
    false
}

/// Copies a format 1 entry tree into `archive`, replacing chunk id lists with hash lists.
fn migrate_v1_entries(
    entries: Vec<Entry>,
    archive: &mut Archive,
    ids: &HashMap<u64, ChunkHash>,
) -> std::io::Result<Vec<Entry>> {
    entries
        .into_iter()
        .map(|entry| {
            Ok(match entry {
                Entry::File(mut file) => {
                    let hashes = chunks::entry_hashes_v1(&mut file, ids)?;
                    Entry::File(archive.write_file_entry(
                        hashes.as_flattened(),
                        Some(file.size_real),
                        file.name,
                        file.mode,
                        file.mtime,
                        file.owner,
                        CompressionFormat::None,
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

/// The archive name in a marker file name: `<name>.ddup` or `<name>.ddup.<n>`.
fn pending_name(marker: &Path) -> Option<String> {
    let file = marker.file_name()?.to_str()?;
    if let Some(name) = file.strip_suffix(".ddup") {
        return Some(name.to_owned());
    }
    let (name, n) = file.rsplit_once(".ddup.")?;
    (!n.is_empty() && n.bytes().all(|b| b.is_ascii_digit())).then(|| name.to_owned())
}

/// Removes an earlier restore, including read-only directories. A missing path is fine.
pub fn remove_restored(path: &Path) -> std::io::Result<()> {
    let metadata = match path.symlink_metadata() {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    if !metadata.is_dir() {
        return std::fs::remove_file(path);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = metadata.permissions().mode();
        if mode & 0o700 != 0o700 {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode | 0o700))?;
        }
        for entry in std::fs::read_dir(path)? {
            let entry = entry?.path();
            if entry.symlink_metadata()?.is_dir() {
                remove_restored(&entry)?;
            }
        }
    }
    std::fs::remove_dir_all(path)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Readability {
    Whole,
    Damaged,
    Unsupported,
}

/// Whether a path relative to the backup root lies in a repository's own directory.
fn is_repository_internal(relative: &Path) -> bool {
    relative
        .iter()
        .any(|part| part == ".ddup-bak" || part == ".ddup-bak-restore")
}

/// Prefixes `err` with `path`.
fn named(path: &Path, err: std::io::Error) -> std::io::Error {
    std::io::Error::new(err.kind(), format!("{}: {err}", path.display()))
}

/// The path a walk error is about, if it names one.
fn walk_error_path(err: &ignore::Error) -> Option<&Path> {
    match err {
        ignore::Error::WithPath { path, .. } => Some(path),
        ignore::Error::WithDepth { err, .. } | ignore::Error::WithLineNumber { err, .. } => {
            walk_error_path(err)
        }
        ignore::Error::Loop { child, .. } => Some(child),
        _ => None,
    }
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

struct Job<'a> {
    archive: Mutex<Archive>,
    index: &'a ChunkIndex,
    storage: &'a Arc<dyn ChunkStorage>,
    compression: &'a CompressionFormatCallback,
    progress: &'a ProgressCallback,
    chunk_size: usize,
    max_chunk_count: usize,
    files: Inflight,
    chunks: Inflight,
    error: Mutex<Option<std::io::Error>>,
    children: Mutex<HashMap<PathBuf, Vec<Entry>>>,
}

impl Job<'_> {
    fn add(&self, path: &Path, parent: PathBuf, entry: Entry) {
        if let Some(progress) = self.progress {
            progress(path);
        }
        self.children.lock().entry(parent).or_default().push(entry);
    }

    fn write_file<'scope>(
        &'scope self,
        scope: &rayon::Scope<'scope>,
        path: &Path,
        relative: &Path,
        name: String,
        parent: PathBuf,
    ) -> std::io::Result<()> {
        if self.error.lock().is_some() {
            return Ok(());
        }

        let file = open_nofollow(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(std::io::Error::other(format!(
                "{} changed while being read",
                path.display()
            )));
        }
        let (hashes, size) = self.chunk_file(scope, file, &metadata, relative)?;

        let entry = self.archive.lock().write_file_entry(
            hashes.as_flattened(),
            Some(size),
            name,
            metadata.permissions().into(),
            modified(&metadata),
            metadata_owner(&metadata),
            CompressionFormat::None,
        )?;
        self.add(path, parent, Entry::File(entry));
        Ok(())
    }

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
            // `data` holds what was read before the file turned out larger than expected.
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
        // A count doesn't prove the chunk exists; a lost one is rewritten here.
        if self.index.reference(&hash) > 1 && self.storage.has_chunk(&hash)? {
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

/// Read-write, so an archive just written can be read back through the same handle.
fn create_new_file(path: &Path) -> std::io::Result<File> {
    File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
}

/// Opens a directory to set its times. Windows only opens directories with backup semantics.
fn open_dir(path: &Path) -> std::io::Result<File> {
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_WRITE_ATTRIBUTES: u32 = 0x100;
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
        File::options()
            .access_mode(FILE_WRITE_ATTRIBUTES)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)
    }
    #[cfg(not(windows))]
    File::open(path)
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

/// Restores ownership when permitted; unprivileged restores keep the current user.
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
