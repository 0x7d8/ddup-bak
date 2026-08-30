use crate::{entries::CFileEntry, repository::CRepository, set_error};
use ddup_bak::{
    archive::entries::{Entry, EntryMode, FileEntry},
    chunks::reader::EntryReader,
    repository::Repository,
};
use std::{ffi::*, fs::File, io::Read, sync::Arc, time::SystemTime};

/// Opaque streaming reader over a repository file entry. Holds a shared repository lock until
/// freed.
#[repr(C)]
pub struct CEntryReader {
    _private: [u8; 0],
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn repository_create_entry_reader(
    repo: *mut CRepository,
    entry: *const CFileEntry,
) -> *mut CEntryReader {
    let (Some(repo), Some(entry)) = (unsafe { (repo as *const Repository).as_ref() }, unsafe {
        entry.as_ref()
    }) else {
        return std::ptr::null_mut();
    };
    if entry.file.is_null() || entry.common.name.is_null() {
        return std::ptr::null_mut();
    }

    let file = unsafe {
        Arc::increment_strong_count(entry.file as *const File);
        Arc::from_raw(entry.file as *const File)
    };
    let compression = entry.compression.into();

    let file_entry = FileEntry {
        name: unsafe { CStr::from_ptr(entry.common.name) }
            .to_string_lossy()
            .into_owned(),
        mode: EntryMode::from(entry.common.mode),
        owner: (entry.common.uid, entry.common.gid),
        mtime: SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(entry.common.mtime),
        compression,
        size_compressed: match compression {
            ddup_bak::archive::CompressionFormat::None => None,
            _ => Some(entry.size_compressed),
        },
        size_real: entry.size_real,
        size: entry.size,
        file,
        offset: entry.offset,
        decoder: None,
        consumed: 0,
    };

    match repo.entry_reader(Entry::File(Box::new(file_entry))) {
        Ok(reader) => Box::into_raw(Box::new(reader)) as *mut CEntryReader,
        Err(err) => {
            set_error(&err);
            std::ptr::null_mut()
        }
    }
}

/// Reads up to `buffer_size` bytes. Returns the byte count, 0 at end of file, -1 on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn entry_reader_read(
    reader: *mut CEntryReader,
    buffer: *mut c_char,
    buffer_size: usize,
) -> c_int {
    let Some(reader) = (unsafe { (reader as *mut EntryReader).as_mut() }) else {
        return -1;
    };
    if buffer.is_null() {
        return -1;
    }

    let buffer = unsafe { std::slice::from_raw_parts_mut(buffer as *mut u8, buffer_size) };
    match reader.read(buffer) {
        Ok(bytes_read) => bytes_read as c_int,
        Err(err) => {
            set_error(&err);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_entry_reader(reader: *mut CEntryReader) {
    if !reader.is_null() {
        drop(unsafe { Box::from_raw(reader as *mut EntryReader) });
    }
}
