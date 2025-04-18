use crate::{
    archive::{Archive, CompressionFormat, ProgressCallback},
    chunks::ChunkIndex,
};
use std::{
    fs::{File, FileTimes},
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
};

pub type DeletionProgressCallback = Option<fn(u64, bool)>;

pub struct Repository {
    pub directory: PathBuf,
    pub save_on_drop: bool,

    chunk_index: ChunkIndex,
    ignored_files: Vec<String>,
}

impl Repository {
    pub fn new(directory: &Path) -> std::io::Result<Self> {
        let chunk_index = ChunkIndex::new(directory.join(".ddup-bak"))?;
        let mut ignored_files = Vec::new();

        let ignored_files_path = directory.join(".ddup-bak/ignored_files");
        if ignored_files_path.exists() {
            let text = std::fs::read_to_string(&ignored_files_path)?;
            for line in text.lines() {
                if !line.is_empty() {
                    ignored_files.push(line.to_string());
                }
            }
        }

        Ok(Self {
            directory: directory.to_path_buf(),
            save_on_drop: true,
            chunk_index,
            ignored_files,
        })
    }

    pub fn new_empty(directory: &Path, chunk_size: usize, ignored_files: Vec<String>) -> Self {
        let chunk_index = ChunkIndex::new_empty(directory.join(".ddup-bak"), chunk_size);

        std::fs::create_dir_all(directory.join(".ddup-bak/archives")).unwrap();
        std::fs::create_dir_all(directory.join(".ddup-bak/archives-tmp")).unwrap();
        std::fs::create_dir_all(directory.join(".ddup-bak/archives-restored")).unwrap();
        std::fs::create_dir_all(directory.join(".ddup-bak/chunks")).unwrap();
        std::fs::write(directory.join(".ddup-bak/ignored_files"), "").unwrap();

        Self {
            directory: directory.to_path_buf(),
            save_on_drop: true,
            chunk_index,
            ignored_files,
        }
    }

    fn archive_path(&self, name: &str) -> PathBuf {
        self.directory
            .join(".ddup-bak/archives")
            .join(format!("{}.ddup", name))
    }

    pub fn set_save_on_drop(&mut self, save_on_drop: bool) {
        self.save_on_drop = save_on_drop;
        self.chunk_index.set_save_on_drop(save_on_drop);
    }

    pub fn add_ignored_file(&mut self, file: &str) {
        if !self.ignored_files.contains(&file.to_string()) {
            self.ignored_files.push(file.to_string());
        }
    }

    pub fn remove_ignored_file(&mut self, file: &str) {
        if let Some(pos) = self.ignored_files.iter().position(|x| x == file) {
            self.ignored_files.remove(pos);
        }
    }

    pub fn is_ignored(&self, file: &str) -> bool {
        self.ignored_files.contains(&file.to_string())
    }

    pub fn get_ignored_files(&self) -> &[String] {
        &self.ignored_files
    }

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

    fn recursive_create_archive(
        &mut self,
        entry: std::fs::DirEntry,
        temp_path: &Path,
        progress_chunking: ProgressCallback,
    ) -> std::io::Result<()> {
        let path = entry.path();
        let destination = temp_path.join(path.file_name().unwrap());

        if let Some(f) = progress_chunking {
            f(&path)
        }

        if path.is_file() {
            let chunks = self
                .chunk_index
                .chunk_file(&path, CompressionFormat::Deflate)?;

            let file = File::create(&destination)?;
            let mut writer = BufWriter::new(&file);
            for chunk in chunks {
                let id = self.chunk_index.get_chunk_id(&chunk).map_or_else(
                    || {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("Chunk not found: {:?}", chunk),
                        ))
                    },
                    Ok,
                )?;

                writer.write_all(&crate::varint::encode_u64(id))?;
            }

            writer.flush()?;
            file.sync_all()?;
        } else if path.is_dir() {
            std::fs::create_dir_all(&destination)?;

            for sub_entry in std::fs::read_dir(&path)?.flatten() {
                self.recursive_create_archive(sub_entry, &destination, progress_chunking)?;
            }
        } else if path.is_symlink() {
            if let Ok(target) = std::fs::read_link(&path) {
                #[cfg(unix)]
                {
                    std::os::unix::fs::symlink(target, &destination)?;
                }
                #[cfg(windows)]
                {
                    if target.is_dir() {
                        std::os::windows::fs::symlink_dir(target, &destination)?;
                    } else {
                        std::os::windows::fs::symlink_file(target, &destination)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn create_archive(
        &mut self,
        name: &str,
        progress_chunking: ProgressCallback,
        progress_archiving: ProgressCallback,
    ) -> std::io::Result<Archive> {
        if self.list_archives()?.contains(&name.to_string()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("Archive {} already exists", name),
            ));
        }

        let archive_path = self.archive_path(name);
        let archive_tmp_path = self.directory.join(".ddup-bak/archives-tmp").join(name);
        let mut archive = Archive::new(File::create(&archive_path)?);

        std::fs::create_dir_all(&archive_tmp_path)?;

        for entry in std::fs::read_dir(&self.directory)?.flatten() {
            let path = entry.path();
            if self.is_ignored(path.to_str().unwrap())
                || path.file_name() == Some(".ddup-bak".as_ref())
            {
                continue;
            }

            self.recursive_create_archive(entry, &archive_tmp_path, progress_chunking)?;
        }

        for entry in std::fs::read_dir(&archive_tmp_path)?.flatten() {
            archive.add_entry(entry, progress_archiving)?;
        }

        std::fs::remove_dir_all(&archive_tmp_path)?;

        Ok(archive)
    }

    fn recursive_restore_archive(
        &mut self,
        entry: crate::archive::Entry,
        directory: &Path,
        progress: ProgressCallback,
    ) -> std::io::Result<()> {
        let path = directory.join(entry.name());

        if let Some(f) = progress {
            f(&path)
        }

        match entry {
            crate::archive::Entry::File(mut file_entry) => {
                let mut file = File::create(&path)?;
                let mut buffer = [0; 4096];

                loop {
                    let chunk_id = crate::varint::decode_u64(&mut file_entry);
                    if chunk_id == 0 {
                        break;
                    }

                    let mut chunk = self.chunk_index.get_chunk_id_file(chunk_id).map_or_else(
                        || {
                            Err(std::io::Error::new(
                                std::io::ErrorKind::NotFound,
                                format!("Chunk not found: {}", chunk_id),
                            ))
                        },
                        Ok,
                    )?;

                    loop {
                        let bytes_read = chunk.read(&mut buffer)?;
                        if bytes_read == 0 {
                            break;
                        }

                        file.write_all(&buffer[..bytes_read])?;
                    }
                }

                file.set_permissions(file_entry.mode)?;
                file.set_times(FileTimes::new().set_modified(file_entry.mtime))?;

                #[cfg(unix)]
                {
                    let (uid, gid) = file_entry.owner;

                    std::os::unix::fs::chown(&path, Some(uid), Some(gid))?;
                }
            }
            crate::archive::Entry::Directory(dir_entry) => {
                std::fs::create_dir_all(&path)?;

                for sub_entry in dir_entry.entries {
                    self.recursive_restore_archive(sub_entry, &path, progress)?;
                }
            }
            #[cfg(unix)]
            crate::archive::Entry::Symlink(link_entry) => {
                std::os::unix::fs::symlink(link_entry.target, &path)?;
            }
            #[cfg(windows)]
            crate::archive::Entry::Symlink(link_entry) => {
                if link_entry.target_dir {
                    std::os::windows::fs::symlink_dir(link_entry.target, &path)?;
                } else {
                    std::os::windows::fs::symlink_file(link_entry.target, &path)?;
                }
            }
        }

        Ok(())
    }

    pub fn restore_archive(
        &mut self,
        name: &str,
        progress: ProgressCallback,
    ) -> std::io::Result<()> {
        if !self.list_archives()?.contains(&name.to_string()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Archive {} not found", name),
            ));
        }

        let archive_path = self.archive_path(name);
        let archive = Archive::open(archive_path.to_str().unwrap())?;
        let destination = self
            .directory
            .join(".ddup-bak/archives-restored")
            .join(name);

        for entry in archive.into_entries() {
            self.recursive_restore_archive(entry, &destination, progress)?;
        }

        Ok(())
    }

    fn recursive_delete_archive(
        &mut self,
        entry: crate::archive::Entry,
        progress: DeletionProgressCallback,
    ) -> std::io::Result<()> {
        match entry {
            crate::archive::Entry::File(mut file_entry) => loop {
                let chunk_id = crate::varint::decode_u64(&mut file_entry);
                if chunk_id == 0 {
                    break;
                }

                if let Some(deleted) = self.chunk_index.dereference_chunk_id(chunk_id) {
                    if let Some(f) = progress {
                        f(chunk_id, deleted)
                    }
                }
            },
            crate::archive::Entry::Directory(dir_entry) => {
                for sub_entry in dir_entry.entries {
                    self.recursive_delete_archive(sub_entry, progress)?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    pub fn delete_archive(
        &mut self,
        name: &str,
        progress: DeletionProgressCallback,
    ) -> std::io::Result<()> {
        let archive_path = self.archive_path(name);
        let archive = Archive::open(archive_path.to_str().unwrap())?;

        for entry in archive.into_entries() {
            self.recursive_delete_archive(entry, progress)?;
        }

        std::fs::remove_file(archive_path)?;

        Ok(())
    }
}

impl Drop for Repository {
    fn drop(&mut self) {
        if !self.save_on_drop {
            return;
        }

        let ignored_files_path = self.directory.join(".ddup-bak/ignored_files");
        let mut file = File::create(&ignored_files_path).unwrap();

        for entry in &self.ignored_files {
            writeln!(file, "{}", entry).unwrap();
        }
    }
}
