use crate::{
    archive::{
        Archive, CompressionFormat, CompressionFormatCallback, ProgressCallback, entries::Entry,
    },
    chunks::{ChunkIndex, lock::LockMode, reader::EntryReader, storage},
};
use std::{
    fs::{File, FileTimes},
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};

pub type DeletionProgressCallback = Option<Arc<dyn Fn(u64, bool) + Send + Sync + 'static>>;

pub struct Repository {
    pub directory: PathBuf,
    pub save_on_drop: bool,

    pub chunk_index: ChunkIndex,
}

impl Repository {
    /// Opens an existing repository.
    /// The repository must be initialized with `new` before use.
    /// The repository directory must contain a `.ddup-bak` directory.
    pub fn open(
        directory: &Path,
        chunks_directory: Option<&Path>,
        storage: Option<Arc<dyn storage::ChunkStorage>>,
    ) -> std::io::Result<Self> {
        let chunk_index = ChunkIndex::open(
            chunks_directory.map_or(directory.join(".ddup-bak/chunks"), |p| p.to_path_buf()),
            storage.map_or(
                Arc::new(storage::ChunkStorageLocal(
                    directory.join(".ddup-bak/chunks"),
                )),
                |s| s,
            ),
        )?;

        Ok(Self {
            directory: directory.to_path_buf(),
            save_on_drop: true,
            chunk_index,
        })
    }

    pub fn new(
        directory: &Path,
        chunk_size: usize,
        max_chunk_count: usize,
        storage: Option<Arc<dyn storage::ChunkStorage>>,
    ) -> Self {
        std::fs::create_dir_all(directory.join(".ddup-bak/archives")).unwrap();
        std::fs::create_dir_all(directory.join(".ddup-bak/archives-restored")).unwrap();
        std::fs::create_dir_all(directory.join(".ddup-bak/chunks")).unwrap();

        let chunk_index = ChunkIndex::new(
            directory.join(".ddup-bak/chunks"),
            chunk_size,
            max_chunk_count,
            storage.map_or(
                Arc::new(storage::ChunkStorageLocal(
                    directory.join(".ddup-bak/chunks"),
                )),
                |s| s,
            ),
        );

        Self {
            directory: directory.to_path_buf(),
            save_on_drop: true,
            chunk_index,
        }
    }

    pub fn save(&self) -> std::io::Result<()> {
        self.chunk_index.save()?;

        Ok(())
    }

    #[inline]
    pub fn archive_path(&self, name: &str) -> PathBuf {
        self.directory
            .join(".ddup-bak/archives")
            .join(format!("{}.ddup", name))
    }

    /// Sets the save_on_drop flag.
    /// If set to true, the repository will save all changes to disk when dropped.
    /// If set to false, the repository will not save changes when dropped.
    /// This is useful for testing purposes, where you may want to discard changes.
    /// By default, this flag is set to true and should NOT be changed.
    #[inline]
    pub const fn set_save_on_drop(&mut self, save_on_drop: bool) -> &mut Self {
        self.save_on_drop = save_on_drop;

        self
    }

    /// Lists all archives in the repository.
    /// Returns a vector of archive names without the ".ddup" extension.
    /// Example: "my_archive" instead of "my_archive.ddup".
    /// The archives are stored in the ".ddup-bak/archives" directory.
    pub fn list_archives(&self) -> std::io::Result<Vec<String>> {
        let mut archives = Vec::new();
        let archive_dir = self.directory.join(".ddup-bak/archives");

        for entry in std::fs::read_dir(archive_dir)?.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if let Some(stripped) = name.strip_suffix(".ddup") {
                    archives.push(stripped.to_string());
                }
            }
        }

        Ok(archives)
    }

    /// Gets an archive by name.
    /// Do not use this method to extract data, the data is chunked and compressed.
    /// Use `restore_archive` instead.
    pub fn get_archive(&self, name: &str) -> std::io::Result<Archive> {
        let archive_path = self.archive_path(name);

        Archive::open(archive_path.to_str().unwrap())
    }

    pub fn clean(&self, progress: DeletionProgressCallback) -> std::io::Result<()> {
        let mut w = self.chunk_index.lock.write_lock(LockMode::Destructive)?;
        self.chunk_index.clean(progress)?;

        w.unlock()?;

        Ok(())
    }

    pub fn entry_reader(&self, entry: Entry) -> std::io::Result<EntryReader> {
        match entry {
            Entry::File(file_entry) => Ok(EntryReader::new(file_entry, self.chunk_index.clone())),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Entry is not a file",
            )),
        }
    }

    #[inline]
    pub fn archive_path_parent<'a>(
        archive: &'a mut Archive,
        entry: &Path,
    ) -> Option<&'a mut Box<crate::archive::entries::DirectoryEntry>> {
        archive
            .find_archive_entry_mut(entry.parent()?)
            .ok()
            .flatten()
            .map(|e| match e {
                Entry::Directory(dir) => dir,
                _ => panic!("Parent entry is not a directory"),
            })
    }

    #[allow(clippy::too_many_arguments)]
    fn recursive_create_archive(
        archive: Arc<Mutex<Option<Archive>>>,
        chunk_index: &ChunkIndex,
        entry: ignore::DirEntry,
        root_path: &Path,
        progress_chunking: ProgressCallback,
        compression_callback: CompressionFormatCallback,
        scope: &rayon::Scope,
        error: Arc<RwLock<Option<std::io::Error>>>,
    ) -> std::io::Result<()> {
        let path = entry.path().strip_prefix(root_path).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Path is not a subpath of the root directory",
            )
        })?;
        let metadata = entry.path().symlink_metadata()?;

        if error.read().unwrap().is_some() {
            return Ok(());
        }

        if let Some(f) = &progress_chunking {
            f(entry.path())
        }

        if metadata.is_file() {
            let compression = compression_callback
                .as_ref()
                .map(|f| f(path, &metadata))
                .unwrap_or(CompressionFormat::Deflate);

            let chunks =
                chunk_index.chunk_file(&entry.path().to_path_buf(), compression, Some(scope))?;

            let mut chunk_content = Vec::new();
            for id in chunks {
                chunk_content.extend_from_slice(&crate::varint::encode_u64(id));
            }

            let mut archive = archive.lock().unwrap();
            let file_entry = archive.as_mut().unwrap().write_file_entry(
                Cursor::new(chunk_content),
                Some(metadata.len()),
                path.file_name().unwrap().to_string_lossy().into_owned(),
                metadata.permissions().into(),
                metadata.modified().unwrap_or(std::time::SystemTime::now()),
                {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::MetadataExt;
                        (metadata.uid(), metadata.gid())
                    }
                    #[cfg(windows)]
                    {
                        (0, 0)
                    }
                },
                compression,
            )?;

            if let Some(parent) = Self::archive_path_parent(archive.as_mut().unwrap(), path) {
                parent.entries.push(Entry::File(file_entry));
            } else {
                archive
                    .as_mut()
                    .unwrap()
                    .entries
                    .push(Entry::File(file_entry));
            }
        } else if metadata.is_dir() {
            if path.file_name().is_none() {
                return Ok(());
            }

            let mut archive = archive.lock().unwrap();

            let dir_entry = Entry::Directory(Box::new(crate::archive::entries::DirectoryEntry {
                name: path.file_name().unwrap().to_string_lossy().into_owned(),
                mode: metadata.permissions().into(),
                mtime: metadata.modified().unwrap_or(std::time::SystemTime::now()),
                owner: {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::MetadataExt;
                        (metadata.uid(), metadata.gid())
                    }
                    #[cfg(windows)]
                    {
                        (0, 0)
                    }
                },
                entries: Vec::new(),
            }));

            if let Some(parent) = Self::archive_path_parent(archive.as_mut().unwrap(), path) {
                parent.entries.push(dir_entry);
            } else {
                archive.as_mut().unwrap().entries.push(dir_entry);
            }
        } else if metadata.is_symlink() {
            if let Ok(target) = std::fs::read_link(path) {
                let mut archive = archive.lock().unwrap();

                let link_entry = Entry::Symlink(Box::new(crate::archive::entries::SymlinkEntry {
                    name: path.file_name().unwrap().to_string_lossy().into_owned(),
                    mode: metadata.permissions().into(),
                    mtime: metadata.modified().unwrap_or(std::time::SystemTime::now()),
                    owner: {
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::MetadataExt;
                            (metadata.uid(), metadata.gid())
                        }
                        #[cfg(windows)]
                        {
                            (0, 0)
                        }
                    },
                    target: target.to_string_lossy().into_owned(),
                    target_dir: target.is_dir(),
                }));

                if let Some(parent) = Self::archive_path_parent(archive.as_mut().unwrap(), path) {
                    parent.entries.push(link_entry);
                } else {
                    archive.as_mut().unwrap().entries.push(link_entry);
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_archive(
        &self,
        name: &str,
        directory: Option<ignore::Walk>,
        directory_root: Option<&Path>,
        progress_chunking: ProgressCallback,
        compression_callback: CompressionFormatCallback,
        threads: usize,
    ) -> std::io::Result<Archive> {
        if self.list_archives()?.contains(&name.to_string()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("Archive {} already exists", name),
            ));
        }

        let mut w = self.chunk_index.lock.write_lock(LockMode::NonDestructive)?;

        let archive_path = self.archive_path(name);

        let worker_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap(),
        );
        let error = Arc::new(RwLock::new(None));

        let walker = directory.unwrap_or_else(|| {
            ignore::WalkBuilder::new(&self.directory)
                .follow_links(false)
                .git_global(false)
                .build()
        });

        let archive = Arc::new(Mutex::new(Some(Archive::new(File::create(&archive_path)?))));

        worker_pool.in_place_scope(|scope| {
            for entry in walker.flatten() {
                let path = entry.path();
                if path.file_name() == Some(".ddup-bak".as_ref()) {
                    continue;
                }

                if error.read().unwrap().is_some() {
                    break;
                }

                scope.spawn({
                    let error = Arc::clone(&error);
                    let archive = Arc::clone(&archive);
                    let chunk_index = self.chunk_index.clone();
                    let directory_root = directory_root.unwrap_or(&self.directory);
                    let progress_chunking = progress_chunking.clone();
                    let compression_callback = compression_callback.clone();

                    move |scope| {
                        if let Err(err) = Self::recursive_create_archive(
                            archive,
                            &chunk_index,
                            entry,
                            directory_root,
                            progress_chunking,
                            compression_callback,
                            scope,
                            Arc::clone(&error),
                        ) {
                            let mut error = error.write().unwrap();
                            if error.is_none() {
                                *error = Some(err);
                            }
                        }
                    }
                });
            }
        });

        if let Some(err) = error.write().unwrap().take() {
            return Err(err);
        }

        let mut archive = archive.lock().unwrap().take().unwrap();
        archive.write_end_header()?;

        w.unlock()?;

        Ok(archive)
    }

    pub fn read_entry_content<S: Write>(
        &self,
        entry: Entry,
        stream: &mut S,
    ) -> std::io::Result<()> {
        match entry {
            Entry::File(mut file_entry) => {
                let mut buffer = [0; 4096];

                loop {
                    let chunk_id = crate::varint::decode_u64(&mut file_entry);
                    if chunk_id == 0 {
                        break;
                    }

                    let mut chunk = self.chunk_index.read_chunk_id_content(chunk_id)?;

                    loop {
                        let bytes_read = chunk.read(&mut buffer)?;
                        if bytes_read == 0 {
                            break;
                        }

                        stream.write_all(&buffer[..bytes_read])?;
                    }
                }

                Ok(())
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Entry is not a file",
            )),
        }
    }

    fn recursive_restore_archive(
        chunk_index: &ChunkIndex,
        entry: Entry,
        directory: &Path,
        progress: ProgressCallback,
        scope: &rayon::Scope,
        error: Arc<RwLock<Option<std::io::Error>>>,
    ) -> std::io::Result<()> {
        let path = directory.join(entry.name());

        if error.read().unwrap().is_some() {
            return Ok(());
        }

        if let Some(f) = &progress {
            f(&path)
        }

        match entry {
            Entry::File(mut file_entry) => {
                let mut file = File::create(&path)?;
                let mut buffer = [0; 4096];

                loop {
                    let chunk_id = crate::varint::decode_u64(&mut file_entry);
                    if chunk_id == 0 {
                        break;
                    }

                    let mut chunk = chunk_index.read_chunk_id_content(chunk_id)?;

                    loop {
                        let bytes_read = chunk.read(&mut buffer)?;
                        if bytes_read == 0 {
                            break;
                        }

                        file.write_all(&buffer[..bytes_read])?;
                    }
                }

                file.set_permissions(file_entry.mode.into())?;
                file.set_times(FileTimes::new().set_modified(file_entry.mtime))?;

                #[cfg(unix)]
                {
                    let (uid, gid) = file_entry.owner;

                    std::os::unix::fs::lchown(&path, Some(uid), Some(gid))?;
                }
            }
            Entry::Directory(dir_entry) => {
                std::fs::create_dir_all(&path)?;

                std::fs::set_permissions(&path, dir_entry.mode.into())?;

                #[cfg(unix)]
                {
                    let (uid, gid) = dir_entry.owner;
                    std::os::unix::fs::chown(&path, Some(uid), Some(gid))?;
                }

                for sub_entry in dir_entry.entries {
                    scope.spawn({
                        let error = Arc::clone(&error);
                        let chunk_index = chunk_index.clone();
                        let path = path.to_path_buf();
                        let progress = progress.clone();

                        move |scope| {
                            if let Err(err) = Self::recursive_restore_archive(
                                &chunk_index,
                                sub_entry,
                                &path,
                                progress,
                                scope,
                                Arc::clone(&error),
                            ) {
                                let mut error = error.write().unwrap();
                                if error.is_none() {
                                    *error = Some(err);
                                }
                            }
                        }
                    });
                }
            }
            #[cfg(unix)]
            Entry::Symlink(link_entry) => {
                std::os::unix::fs::symlink(link_entry.target, &path)?;
                std::fs::set_permissions(&path, link_entry.mode.into())?;

                let (uid, gid) = link_entry.owner;
                std::os::unix::fs::lchown(&path, Some(uid), Some(gid))?;
            }
            #[cfg(windows)]
            Entry::Symlink(link_entry) => {
                if link_entry.target_dir {
                    std::os::windows::fs::symlink_dir(link_entry.target, &path)?;
                } else {
                    std::os::windows::fs::symlink_file(link_entry.target, &path)?;
                }

                std::fs::set_permissions(&path, link_entry.mode.into())?;
            }
        }

        Ok(())
    }

    pub fn restore_archive(
        &self,
        name: &str,
        progress: ProgressCallback,
        threads: usize,
    ) -> std::io::Result<PathBuf> {
        if !self.list_archives()?.contains(&name.to_string()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Archive {} not found", name),
            ));
        }

        let mut r = self.chunk_index.lock.read_lock(LockMode::NonDestructive)?;

        let archive_path = self.archive_path(name);
        let archive = Archive::open(archive_path.to_str().unwrap())?;
        let destination = self
            .directory
            .join(".ddup-bak/archives-restored")
            .join(name);

        std::fs::create_dir_all(&destination)?;

        let worker_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap(),
        );
        let error = Arc::new(RwLock::new(None));

        worker_pool.in_place_scope(|scope| {
            for entry in archive.into_entries() {
                scope.spawn({
                    let error = Arc::clone(&error);
                    let chunk_index = self.chunk_index.clone();
                    let destination = destination.clone();
                    let progress = progress.clone();

                    move |scope| {
                        if let Err(err) = Self::recursive_restore_archive(
                            &chunk_index,
                            entry,
                            &destination,
                            progress,
                            scope,
                            Arc::clone(&error),
                        ) {
                            let mut error = error.write().unwrap();
                            if error.is_none() {
                                *error = Some(err);
                            }
                        }
                    }
                });
            }
        });

        if let Some(err) = error.write().unwrap().take() {
            return Err(err);
        }

        r.unlock()?;

        Ok(destination)
    }

    pub fn restore_entries(
        &self,
        name: &str,
        entries: Vec<Entry>,
        progress: ProgressCallback,
        threads: usize,
    ) -> std::io::Result<PathBuf> {
        if !self.list_archives()?.contains(&name.to_string()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Archive {} not found", name),
            ));
        }

        let mut r = self.chunk_index.lock.read_lock(LockMode::NonDestructive)?;

        let destination = self
            .directory
            .join(".ddup-bak/archives-restored")
            .join(name);

        std::fs::create_dir_all(&destination)?;

        let worker_pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap(),
        );
        let error = Arc::new(RwLock::new(None));

        worker_pool.in_place_scope(|scope| {
            for entry in entries {
                scope.spawn({
                    let error = Arc::clone(&error);
                    let chunk_index = self.chunk_index.clone();
                    let destination = destination.clone();
                    let progress = progress.clone();

                    move |scope| {
                        if let Err(err) = Self::recursive_restore_archive(
                            &chunk_index,
                            entry,
                            &destination,
                            progress,
                            scope,
                            Arc::clone(&error),
                        ) {
                            let mut error = error.write().unwrap();
                            if error.is_none() {
                                *error = Some(err);
                            }
                        }
                    }
                });
            }
        });

        if let Some(err) = error.write().unwrap().take() {
            return Err(err);
        }

        r.unlock()?;

        Ok(destination)
    }

    fn recursive_delete_archive(
        &self,
        entry: Entry,
        progress: DeletionProgressCallback,
    ) -> std::io::Result<()> {
        match entry {
            Entry::File(mut file_entry) => loop {
                let chunk_id = crate::varint::decode_u64(&mut file_entry);
                if chunk_id == 0 {
                    break;
                }

                if let Some(deleted) = self.chunk_index.dereference_chunk_id(chunk_id, true) {
                    if let Some(f) = &progress {
                        f(chunk_id, deleted)
                    }
                }
            },
            Entry::Directory(dir_entry) => {
                for sub_entry in dir_entry.entries {
                    self.recursive_delete_archive(sub_entry, progress.clone())?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    pub fn delete_archive(
        &self,
        name: &str,
        progress: DeletionProgressCallback,
    ) -> std::io::Result<()> {
        if !self.list_archives()?.contains(&name.to_string()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Archive {} not found", name),
            ));
        }

        let mut w = self.chunk_index.lock.write_lock(LockMode::Destructive)?;

        let archive_path = self.archive_path(name);
        let archive = Archive::open(archive_path.to_str().unwrap())?;

        for entry in archive.into_entries() {
            self.recursive_delete_archive(entry, progress.clone())?;
        }

        std::fs::remove_file(archive_path)?;

        w.unlock()?;

        Ok(())
    }
}

impl Drop for Repository {
    fn drop(&mut self) {
        if self.save_on_drop {
            if let Err(err) = self.save() {
                eprintln!("Failed to save repository: {}", err);
            }
        }
    }
}
