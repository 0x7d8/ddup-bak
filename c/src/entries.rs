use crate::{archive::CCompressionFormat, c_string};
use ddup_bak::archive::entries::Entry;
use std::{ffi::*, fs::File, sync::Arc, time::SystemTime};

#[repr(C)]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum CEntryType {
    File = 0,
    Directory = 1,
    Symlink = 2,
}

/// Tagged pointer to a `CFileEntry`, `CDirectoryEntry` or `CSymlinkEntry`.
#[repr(C)]
pub struct CEntry {
    pub entry_type: CEntryType,
    pub entry: *mut c_void,
}

#[repr(C)]
pub struct CEntryCommon {
    pub name: *mut c_char,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime: u64,
    pub entry_type: CEntryType,
}

#[repr(C)]
pub struct CFileEntry {
    pub common: CEntryCommon,
    pub compression: CCompressionFormat,
    pub size: u64,
    pub size_real: u64,
    pub size_compressed: u64,

    pub file: *mut c_void,
    pub offset: u64,
}

#[repr(C)]
pub struct CDirectoryEntry {
    pub common: CEntryCommon,
    pub entries_count: c_uint,
    pub entries: *mut *mut CEntry,
}

#[repr(C)]
pub struct CSymlinkEntry {
    pub common: CEntryCommon,
    pub target: *mut c_char,
    pub target_dir: bool,
}

fn common(entry: &Entry, entry_type: CEntryType) -> CEntryCommon {
    let (uid, gid) = entry.owner();
    let mtime = entry
        .mtime()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    CEntryCommon {
        name: c_string(entry.name()).into_raw(),
        mode: entry.mode().bits(),
        uid,
        gid,
        mtime,
        entry_type,
    }
}

fn boxed<T>(value: T) -> *mut c_void {
    Box::into_raw(Box::new(value)) as *mut c_void
}

pub fn entry_to_c(entry: &Entry) -> *mut CEntry {
    let (entry_type, inner) = match entry {
        Entry::File(file) => (
            CEntryType::File,
            boxed(CFileEntry {
                common: common(entry, CEntryType::File),
                compression: file.compression.into(),
                size: file.size,
                size_real: file.size_real,
                size_compressed: file.size_compressed.unwrap_or(0),
                file: Arc::into_raw(Arc::clone(&file.file)) as *mut c_void,
                offset: file.offset,
            }),
        ),
        Entry::Directory(dir) => {
            let entries: Vec<*mut CEntry> = dir.entries.iter().map(entry_to_c).collect();
            (
                CEntryType::Directory,
                boxed(CDirectoryEntry {
                    common: common(entry, CEntryType::Directory),
                    entries_count: entries.len() as c_uint,
                    entries: Box::into_raw(entries.into_boxed_slice()) as *mut *mut CEntry,
                }),
            )
        }
        Entry::Symlink(link) => (
            CEntryType::Symlink,
            boxed(CSymlinkEntry {
                common: common(entry, CEntryType::Symlink),
                target: c_string(link.target.as_str()).into_raw(),
                target_dir: link.target_dir,
            }),
        ),
    };

    Box::into_raw(Box::new(CEntry {
        entry_type,
        entry: inner,
    }))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn get_entry_type(entry: *const CEntry) -> CEntryType {
    unsafe { entry.as_ref() }.map_or(CEntryType::File, |entry| entry.entry_type)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn entry_get_common(entry: *const CEntry) -> *const CEntryCommon {
    let Some(entry) = (unsafe { entry.as_ref() }) else {
        return std::ptr::null();
    };

    match entry.entry_type {
        CEntryType::File => unsafe { &(*(entry.entry as *const CFileEntry)).common },
        CEntryType::Directory => unsafe { &(*(entry.entry as *const CDirectoryEntry)).common },
        CEntryType::Symlink => unsafe { &(*(entry.entry as *const CSymlinkEntry)).common },
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn entry_name(entry: *const CEntry) -> *const c_char {
    let common = unsafe { entry_get_common(entry) };
    if common.is_null() {
        std::ptr::null()
    } else {
        unsafe { (*common).name }
    }
}

unsafe fn entry_as<T>(entry: *const CEntry, entry_type: CEntryType) -> *const T {
    match unsafe { entry.as_ref() } {
        Some(entry) if entry.entry_type == entry_type => entry.entry as *const T,
        _ => std::ptr::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn entry_as_file(entry: *const CEntry) -> *const CFileEntry {
    unsafe { entry_as(entry, CEntryType::File) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn entry_as_directory(entry: *const CEntry) -> *const CDirectoryEntry {
    unsafe { entry_as(entry, CEntryType::Directory) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn entry_as_symlink(entry: *const CEntry) -> *const CSymlinkEntry {
    unsafe { entry_as(entry, CEntryType::Symlink) }
}

/// Frees an entry and, for directories, all of its children.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_entry(entry: *mut CEntry) {
    if entry.is_null() {
        return;
    }

    let entry = unsafe { Box::from_raw(entry) };
    unsafe {
        match entry.entry_type {
            CEntryType::File => {
                let file = Box::from_raw(entry.entry as *mut CFileEntry);
                drop(CString::from_raw(file.common.name));
                drop(Arc::from_raw(file.file as *const File));
            }
            CEntryType::Directory => {
                let dir = Box::from_raw(entry.entry as *mut CDirectoryEntry);
                drop(CString::from_raw(dir.common.name));
                free_entry_array(dir.entries, dir.entries_count);
            }
            CEntryType::Symlink => {
                let link = Box::from_raw(entry.entry as *mut CSymlinkEntry);
                drop(CString::from_raw(link.common.name));
                drop(CString::from_raw(link.target));
            }
        }
    }
}

/// Frees an entry array returned by `archive_entries` together with its entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_entry_array(entries: *mut *mut CEntry, count: c_uint) {
    if entries.is_null() {
        return;
    }

    let entries =
        unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(entries, count as usize)) };
    for entry in entries {
        unsafe { free_entry(entry) };
    }
}
