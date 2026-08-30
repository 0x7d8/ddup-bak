//! Filesystem helpers that keep repository files private (0600/0700 on unix).

use std::{
    fs::{DirBuilder, File, OpenOptions},
    path::Path,
};

fn options() -> OpenOptions {
    #[allow(unused_mut)]
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options
}

pub fn create_file(path: &Path) -> std::io::Result<File> {
    options().write(true).create(true).truncate(true).open(path)
}

/// Read-write, so an archive just written can be read back through the same handle.
pub fn create_new_file(path: &Path) -> std::io::Result<File> {
    options().read(true).write(true).create_new(true).open(path)
}

pub fn open_or_create(path: &Path) -> std::io::Result<File> {
    options().read(true).write(true).create(true).open(path)
}

fn dir_builder(recursive: bool) -> DirBuilder {
    let mut builder = DirBuilder::new();
    builder.recursive(recursive);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder
}

pub fn create_dir(path: &Path) -> std::io::Result<()> {
    dir_builder(false).create(path)
}

pub fn create_dir_all(path: &Path) -> std::io::Result<()> {
    dir_builder(true).create(path)
}

/// Durably syncs a directory's entries (after renames into it).
pub fn sync_dir(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// Hands a file's data to the device without waiting for a cache flush; pair with
/// `sync_filesystem` to make it durable. Cheap where a full fsync is expensive.
pub fn flush_file(file: &File) -> std::io::Result<()> {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        use std::os::fd::AsRawFd;
        if unsafe { libc::fsync(file.as_raw_fd()) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(target_os = "linux")]
    {
        let _ = file;
        Ok(())
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "linux")))]
    file.sync_data()
}

/// `flush_file` for a directory (after renames into it). Nothing to do on Linux, where
/// `sync_filesystem` covers directories too.
pub fn flush_dir(path: &Path) -> std::io::Result<()> {
    #[cfg(all(unix, not(target_os = "linux")))]
    flush_file(&File::open(path)?)?;
    #[cfg(not(all(unix, not(target_os = "linux"))))]
    let _ = path;
    Ok(())
}

/// Writes `data` at `offset` without touching the file's cursor, so writers can share a file.
pub fn write_all_at(file: &File, offset: u64, data: &[u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::write_all_at(file, data, offset)
    }
    #[cfg(windows)]
    {
        let mut written = 0;
        while written < data.len() {
            let n = std::os::windows::fs::FileExt::seek_write(
                file,
                &data[written..],
                offset + written as u64,
            )?;
            if n == 0 {
                return Err(std::io::ErrorKind::WriteZero.into());
            }
            written += n;
        }
        Ok(())
    }
}

/// Durability barrier for everything previously flushed on the filesystem containing `path`.
pub fn sync_filesystem(path: &Path) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        if unsafe { libc::syncfs(File::open(path)?.as_raw_fd()) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        File::open(path)?.sync_all()
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "linux")))]
    {
        let _ = path;
        Ok(())
    }
}
