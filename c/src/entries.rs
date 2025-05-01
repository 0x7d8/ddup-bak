use crate::archive::CCompressionFormat;
use ddup_bak::archive::entries::Entry;
use std::ffi::*;
use std::time::{Duration, SystemTime};

#[repr(C)]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum CEntryType {
    File = 0,
    Directory = 1,
    Symlink = 2,
}

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

fn create_c_entry_common(entry: &Entry) -> CEntryCommon {
    let name = CString::new(entry.name()).unwrap();
    let (uid, gid) = entry.owner();
    let mtime = entry
        .mtime()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs();

    let entry_type = match entry {
        Entry::File(_) => CEntryType::File,
        Entry::Directory(_) => CEntryType::Directory,
        Entry::Symlink(_) => CEntryType::Symlink,
    };

    #[cfg(unix)]
    let mode = {
        use std::os::unix::fs::PermissionsExt;
        entry.mode().mode()
    };

    #[cfg(windows)]
    let mode = {
        if entry.mode().readonly() {
            1
        } else {
            0
        }
    };

    CEntryCommon {
        name: name.into_raw(),
        mode,
        uid,
        gid,
        mtime,
        entry_type,
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn get_entry_type(entry: *const CEntry) -> CEntryType {
    if entry.is_null() {
        return CEntryType::File;
    }

    unsafe { (*entry).entry_type }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn entry_get_common(entry: *const CEntry) -> *const CEntryCommon {
    if entry.is_null() {
        return std::ptr::null();
    }

    let entry_type = unsafe { (*entry).entry_type };

    match entry_type {
        CEntryType::File => {
            let file_entry = unsafe { (*entry).entry as *const CFileEntry };
            unsafe { &(*file_entry).common }
        }
        CEntryType::Directory => {
            let dir_entry = unsafe { (*entry).entry as *const CDirectoryEntry };
            unsafe { &(*dir_entry).common }
        }
        CEntryType::Symlink => {
            let symlink_entry = unsafe { (*entry).entry as *const CSymlinkEntry };
            unsafe { &(*symlink_entry).common }
        }
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn entry_name(entry: *const CEntry) -> *const c_char {
    let common = entry_get_common(entry);

    if common.is_null() {
        return std::ptr::null();
    }

    unsafe { (*common).name }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn free_entry(entry: *mut CEntry) {
    if entry.is_null() {
        return;
    }

    let entry_type = unsafe { (*entry).entry_type };
    let entry_ptr = unsafe { (*entry).entry };

    match entry_type {
        CEntryType::File => {
            let file_entry = entry_ptr as *mut CFileEntry;
            unsafe {
                if !(*file_entry).common.name.is_null() {
                    let _ = CString::from_raw((*file_entry).common.name);
                }
                let _ = Box::from_raw(file_entry);
            }
        }
        CEntryType::Directory => {
            let dir_entry = entry_ptr as *mut CDirectoryEntry;
            unsafe {
                if !(*dir_entry).common.name.is_null() {
                    let _ = CString::from_raw((*dir_entry).common.name);
                }

                if !(*dir_entry).entries.is_null() {
                    for i in 0..(*dir_entry).entries_count {
                        let sub_entry = *(*dir_entry).entries.offset(i as isize);
                        if !sub_entry.is_null() {
                            free_entry(sub_entry);
                        }
                    }

                    let _ = Box::from_raw((*dir_entry).entries);
                }

                let _ = Box::from_raw(dir_entry);
            }
        }
        CEntryType::Symlink => {
            let symlink_entry = entry_ptr as *mut CSymlinkEntry;
            unsafe {
                if !(*symlink_entry).common.name.is_null() {
                    let _ = CString::from_raw((*symlink_entry).common.name);
                }

                if !(*symlink_entry).target.is_null() {
                    let _ = CString::from_raw((*symlink_entry).target);
                }

                let _ = Box::from_raw(symlink_entry);
            }
        }
    }

    unsafe {
        let _ = Box::from_raw(entry);
    }
}

pub fn entry_to_c(entry: &Entry) -> *mut CEntry {
    match entry {
        Entry::File(file_entry) => {
            let common = create_c_entry_common(entry);

            let file_entry_ptr = Box::into_raw(Box::new(CFileEntry {
                common,
                compression: CCompressionFormat::from(file_entry.compression),
                size: file_entry.size,
                size_real: file_entry.size_real,
                size_compressed: file_entry.size_compressed.unwrap_or(0),
            }));

            Box::into_raw(Box::new(CEntry {
                entry_type: CEntryType::File,
                entry: file_entry_ptr as *mut c_void,
            }))
        }
        Entry::Directory(dir_entry) => {
            let common = create_c_entry_common(entry);

            let entries_count = dir_entry.entries.len();
            let mut entries = Vec::with_capacity(entries_count);

            for sub_entry in &dir_entry.entries {
                entries.push(entry_to_c(sub_entry));
            }

            let entries_ptr = Box::into_raw(entries.into_boxed_slice()) as *mut *mut CEntry;

            let dir_entry_ptr = Box::into_raw(Box::new(CDirectoryEntry {
                common,
                entries_count: entries_count as c_uint,
                entries: entries_ptr,
            }));

            Box::into_raw(Box::new(CEntry {
                entry_type: CEntryType::Directory,
                entry: dir_entry_ptr as *mut c_void,
            }))
        }
        Entry::Symlink(symlink_entry) => {
            let common = create_c_entry_common(entry);

            let target = CString::new(&symlink_entry.target[..]).unwrap();

            let symlink_entry_ptr = Box::into_raw(Box::new(CSymlinkEntry {
                common,
                target: target.into_raw(),
                target_dir: symlink_entry.target_dir,
            }));

            Box::into_raw(Box::new(CEntry {
                entry_type: CEntryType::Symlink,
                entry: symlink_entry_ptr as *mut c_void,
            }))
        }
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn entry_as_file(entry: *const CEntry) -> *const CFileEntry {
    if entry.is_null() {
        return std::ptr::null();
    }

    let entry_type = unsafe { (*entry).entry_type };

    if entry_type != CEntryType::File {
        return std::ptr::null();
    }

    unsafe { (*entry).entry as *const CFileEntry }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn entry_as_directory(entry: *const CEntry) -> *const CDirectoryEntry {
    if entry.is_null() {
        return std::ptr::null();
    }

    let entry_type = unsafe { (*entry).entry_type };

    if entry_type != CEntryType::Directory {
        return std::ptr::null();
    }

    unsafe { (*entry).entry as *const CDirectoryEntry }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn entry_as_symlink(entry: *const CEntry) -> *const CSymlinkEntry {
    if entry.is_null() {
        return std::ptr::null();
    }

    let entry_type = unsafe { (*entry).entry_type };

    if entry_type != CEntryType::Symlink {
        return std::ptr::null();
    }

    unsafe { (*entry).entry as *const CSymlinkEntry }
}
