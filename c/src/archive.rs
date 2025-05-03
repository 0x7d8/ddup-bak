use crate::entries::CEntry;
use ddup_bak::archive::{Archive, CompressionFormat, ProgressCallback};
use std::ffi::*;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::Arc;

#[repr(C)]
pub struct CArchive {
    _private: [u8; 0],
}

pub struct ArchiveHandle {
    inner: Box<Archive>,
}

impl Deref for ArchiveHandle {
    type Target = Archive;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for ArchiveHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Deref for CArchive {
    type Target = ArchiveHandle;

    fn deref(&self) -> &Self::Target {
        unsafe { &*(self as *const CArchive as *const ArchiveHandle) }
    }
}

impl DerefMut for CArchive {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *(self as *mut CArchive as *mut ArchiveHandle) }
    }
}

impl CArchive {
    pub fn from_archive(archive: Archive) -> *mut CArchive {
        let handle = Box::new(ArchiveHandle {
            inner: Box::new(archive),
        });
        Box::into_raw(handle) as *mut CArchive
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn as_handle(ptr: *const CArchive) -> &'static ArchiveHandle {
        &*(ptr as *const ArchiveHandle)
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn as_handle_mut(ptr: *mut CArchive) -> &'static mut ArchiveHandle {
        &mut *(ptr as *mut ArchiveHandle)
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn into_archive(ptr: *mut CArchive) -> Archive {
        let handle = Box::from_raw(ptr as *mut ArchiveHandle);
        *handle.inner
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub enum CCompressionFormat {
    None = 0,
    Gzip = 1,
    Deflate = 2,
    Brotli = 3,
}

impl From<CCompressionFormat> for CompressionFormat {
    fn from(value: CCompressionFormat) -> Self {
        match value {
            CCompressionFormat::None => CompressionFormat::None,
            CCompressionFormat::Gzip => CompressionFormat::Gzip,
            CCompressionFormat::Deflate => CompressionFormat::Deflate,
            CCompressionFormat::Brotli => CompressionFormat::Brotli,
        }
    }
}

impl From<CompressionFormat> for CCompressionFormat {
    fn from(value: CompressionFormat) -> Self {
        match value {
            CompressionFormat::None => CCompressionFormat::None,
            CompressionFormat::Gzip => CCompressionFormat::Gzip,
            CompressionFormat::Deflate => CCompressionFormat::Deflate,
            CompressionFormat::Brotli => CCompressionFormat::Brotli,
        }
    }
}

pub type ProgressCallbackFn = extern "C" fn(path: *const c_char);

fn build_progress_callback(callback: Option<ProgressCallbackFn>) -> ProgressCallback {
    if let Some(callback_fn) = callback {
        Some(Arc::new(move |path: &Path| {
            if let Some(path_str) = path.to_str() {
                let c_path = CString::new(path_str).unwrap();
                callback_fn(c_path.as_ptr());
            }
        }))
    } else {
        None
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn new_archive(path: *const c_char) -> *mut CArchive {
    let path = unsafe { CStr::from_ptr(path).to_string_lossy().into_owned() };

    let file = match std::fs::File::create(&path) {
        Ok(file) => file,
        Err(_) => return std::ptr::null_mut(),
    };

    let archive = Archive::new(file);

    CArchive::from_archive(archive)
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn open_archive(path: *const c_char) -> *mut CArchive {
    let path = unsafe { CStr::from_ptr(path).to_string_lossy().into_owned() };

    match Archive::open(&path) {
        Ok(archive) => CArchive::from_archive(archive),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn free_archive(archive: *mut CArchive) {
    if archive.is_null() {
        return;
    }

    unsafe {
        let _ = CArchive::into_archive(archive);
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn archive_add_directory(
    archive: *mut CArchive,
    path: *const c_char,
    progress_callback: Option<ProgressCallbackFn>,
) -> c_int {
    if archive.is_null() || path.is_null() {
        return -1;
    }

    let archive = unsafe { &mut *archive };
    let path = unsafe { CStr::from_ptr(path).to_string_lossy().into_owned() };

    let callback = build_progress_callback(progress_callback);

    match archive.add_directory(&path, callback) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn archive_set_compression_callback(
    archive: *mut CArchive,
    callback: Option<extern "C" fn(path: *const c_char, size: u64) -> CCompressionFormat>,
) -> *mut CArchive {
    if archive.is_null() {
        return std::ptr::null_mut();
    }

    let archive = unsafe { &mut *archive };

    if let Some(callback_fn) = callback {
        archive.set_compression_callback(Some(Arc::new(
            move |path: &Path, metadata: &std::fs::Metadata| {
                if let Some(path_str) = path.to_str() {
                    let c_path = CString::new(path_str).unwrap();
                    let size = metadata.len();
                    let compression_format = callback_fn(c_path.as_ptr(), size);

                    CompressionFormat::from(compression_format)
                } else {
                    CompressionFormat::Deflate
                }
            },
        )));
    } else {
        archive.set_compression_callback(None);
    }

    archive
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn archive_set_real_size_callback(
    archive: *mut CArchive,
    callback: Option<extern "C" fn(path: *const c_char) -> u64>,
) -> *mut CArchive {
    if archive.is_null() {
        return std::ptr::null_mut();
    }

    let archive = unsafe { &mut *archive };

    if let Some(callback_fn) = callback {
        archive.set_real_size_callback(Some(Arc::new(move |path: &Path| {
            if let Some(path_str) = path.to_str() {
                let c_path = CString::new(path_str).unwrap();
                callback_fn(c_path.as_ptr())
            } else {
                0
            }
        })));
    } else {
        archive.set_real_size_callback(None);
    }

    archive
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn archive_entries_count(archive: *const CArchive) -> c_uint {
    if archive.is_null() {
        return 0;
    }

    let archive = unsafe { &*archive };

    archive.entries().len() as c_uint
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn archive_entries(archive: *const CArchive) -> *mut *const CEntry {
    if archive.is_null() {
        return std::ptr::null_mut();
    }

    let archive = unsafe { &*archive };

    let entries = archive.entries();

    let mut entry_ptrs: Vec<*const CEntry> = Vec::with_capacity(entries.len());

    for entry in entries.iter() {
        entry_ptrs.push(crate::entries::entry_to_c(entry));
    }

    Box::into_raw(entry_ptrs.into_boxed_slice()) as *mut *const CEntry
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn archive_find_entry(
    archive: *const CArchive,
    path: *const c_char,
) -> *mut CEntry {
    if archive.is_null() || path.is_null() {
        return std::ptr::null_mut();
    }

    let archive = unsafe { &*archive };
    let path_str = unsafe { CStr::from_ptr(path).to_string_lossy().into_owned() };

    match archive.find_archive_entry(Path::new(&path_str)) {
        Ok(Some(entry)) => crate::entries::entry_to_c(entry),
        _ => std::ptr::null_mut(),
    }
}
