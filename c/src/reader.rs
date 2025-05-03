use crate::archive::CCompressionFormat;
use crate::entries::CFileEntry;
use ddup_bak::archive::entries::{Entry, EntryMode, FileEntry};
use ddup_bak::chunks::reader::EntryReader;
use std::ffi::*;
use std::io::Read;
use std::ops::{Deref, DerefMut};
use std::slice;
use std::sync::Arc;
use std::time::SystemTime;

#[repr(C)]
pub struct CEntryReader {
    _private: [u8; 0],
}

pub struct EntryReaderHandle {
    inner: Box<EntryReader>,
}

impl Deref for EntryReaderHandle {
    type Target = EntryReader;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for EntryReaderHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn repository_create_entry_reader(
    repo: *mut crate::repository::CRepository,
    entry: *const CFileEntry,
) -> *mut CEntryReader {
    if repo.is_null() || entry.is_null() {
        return std::ptr::null_mut();
    }

    let repo = &*repo;
    let entry = &*entry;

    let name = match CStr::from_ptr(entry.common.name).to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return std::ptr::null_mut(),
    };

    let file_arc = if !entry.file.is_null() {
        let file_ref = &*(entry.file as *const std::fs::File);
        Arc::new(
            file_ref
                .try_clone()
                .unwrap_or_else(|_| std::fs::File::open("/dev/null").unwrap()),
        )
    } else {
        return std::ptr::null_mut();
    };

    let file_entry = FileEntry {
        name,
        mode: EntryMode::from(entry.common.mode),
        owner: (entry.common.uid, entry.common.gid),
        mtime: SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(entry.common.mtime),
        compression: entry.compression.into(),
        size_compressed: if matches!(entry.compression, CCompressionFormat::None) {
            None
        } else {
            Some(entry.size_compressed)
        },
        size_real: entry.size_real,
        size: entry.size,
        file: file_arc,
        offset: entry.offset,
        decoder: None,
        consumed: 0,
    };

    match repo.entry_reader(Entry::File(Box::new(file_entry))) {
        Ok(reader) => {
            let handle = Box::new(EntryReaderHandle {
                inner: Box::new(reader),
            });

            Box::into_raw(handle) as *mut CEntryReader
        }
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn entry_reader_read(
    reader: *mut CEntryReader,
    buffer: *mut c_char,
    buffer_size: usize,
) -> c_int {
    if reader.is_null() || buffer.is_null() {
        return -1;
    }

    let reader_handle = &mut *(reader as *mut EntryReaderHandle);
    let buf_slice = slice::from_raw_parts_mut(buffer as *mut u8, buffer_size);

    match reader_handle.read(buf_slice) {
        Ok(bytes_read) => bytes_read as c_int,
        Err(_) => -1,
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn free_entry_reader(reader: *mut CEntryReader) {
    if !reader.is_null() {
        let _ = Box::from_raw(reader as *mut EntryReaderHandle);
    }
}
