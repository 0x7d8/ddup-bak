use crate::{
    CUserData, UserData, c_string,
    entries::{CEntry, entry_to_c},
    repository::{CProgressCallback, wrap_progress},
    set_error, str_arg,
};
use ddup_bak::archive::{Archive, CompressionFormat};
use std::{ffi::*, path::Path, sync::Arc};

/// Opaque archive handle.
#[repr(C)]
pub struct CArchive {
    _private: [u8; 0],
}

impl CArchive {
    pub fn from_archive(archive: Archive) -> *mut CArchive {
        Box::into_raw(Box::new(archive)) as *mut CArchive
    }
}

unsafe fn archive<'a>(ptr: *mut CArchive) -> Option<&'a mut Archive> {
    unsafe { (ptr as *mut Archive).as_mut() }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub enum CCompressionFormat {
    None = 0,
    Gzip = 1,
    Deflate = 2,
    Brotli = 3,
    Zstd = 4,
}

impl From<CCompressionFormat> for CompressionFormat {
    fn from(value: CCompressionFormat) -> Self {
        match value {
            CCompressionFormat::None => CompressionFormat::None,
            CCompressionFormat::Gzip => CompressionFormat::Gzip,
            CCompressionFormat::Deflate => CompressionFormat::Deflate,
            CCompressionFormat::Brotli => CompressionFormat::Brotli,
            CCompressionFormat::Zstd => CompressionFormat::Zstd,
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
            CompressionFormat::Zstd => CCompressionFormat::Zstd,
        }
    }
}

pub type CArchiveCompressionCallback = Option<
    extern "C" fn(path: *const c_char, size: u64, user_data: CUserData) -> CCompressionFormat,
>;
pub type CRealSizeCallback =
    Option<extern "C" fn(path: *const c_char, user_data: CUserData) -> u64>;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn new_archive(path: *const c_char) -> *mut CArchive {
    let Some(path) = (unsafe { str_arg(path) }) else {
        return std::ptr::null_mut();
    };

    match std::fs::File::create(&path).and_then(Archive::new) {
        Ok(archive) => CArchive::from_archive(archive),
        Err(err) => {
            set_error(&err);
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn open_archive(path: *const c_char) -> *mut CArchive {
    let Some(path) = (unsafe { str_arg(path) }) else {
        return std::ptr::null_mut();
    };

    match Archive::open(&path) {
        Ok(archive) => CArchive::from_archive(archive),
        Err(err) => {
            set_error(&err);
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_archive(archive: *mut CArchive) {
    if !archive.is_null() {
        drop(unsafe { Box::from_raw(archive as *mut Archive) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn archive_add_directory(
    archive: *mut CArchive,
    path: *const c_char,
    progress: CProgressCallback,
    user_data: CUserData,
) -> c_int {
    let (Some(archive), Some(path)) = (unsafe { self::archive(archive) }, unsafe { str_arg(path) })
    else {
        return -1;
    };

    match archive.add_directory(&path, wrap_progress(progress, UserData(user_data))) {
        Ok(_) => 0,
        Err(err) => {
            set_error(&err);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn archive_set_compression_callback(
    archive: *mut CArchive,
    callback: CArchiveCompressionCallback,
    user_data: CUserData,
) {
    let Some(archive) = (unsafe { self::archive(archive) }) else {
        return;
    };
    let user_data = UserData(user_data);

    archive.set_compression_callback(callback.map(|callback| {
        Arc::new(move |path: &Path, metadata: &std::fs::Metadata| {
            callback(
                c_string(path.to_string_lossy().into_owned()).as_ptr(),
                metadata.len(),
                user_data.get(),
            )
            .into()
        }) as Arc<dyn Fn(&Path, &std::fs::Metadata) -> CompressionFormat + Send + Sync>
    }));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn archive_set_real_size_callback(
    archive: *mut CArchive,
    callback: CRealSizeCallback,
    user_data: CUserData,
) {
    let Some(archive) = (unsafe { self::archive(archive) }) else {
        return;
    };
    let user_data = UserData(user_data);

    archive.set_real_size_callback(callback.map(|callback| {
        Arc::new(move |path: &Path| {
            callback(
                c_string(path.to_string_lossy().into_owned()).as_ptr(),
                user_data.get(),
            )
        }) as Arc<dyn Fn(&Path) -> u64 + Send + Sync>
    }));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn archive_entries_count(archive: *mut CArchive) -> c_uint {
    unsafe { self::archive(archive) }.map_or(0, |archive| archive.entries().len() as c_uint)
}

/// Top-level entries of the archive, free with `free_entry_array`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn archive_entries(archive: *mut CArchive) -> *mut *mut CEntry {
    let Some(archive) = (unsafe { self::archive(archive) }) else {
        return std::ptr::null_mut();
    };

    let entries: Vec<*mut CEntry> = archive.entries().iter().map(entry_to_c).collect();
    Box::into_raw(entries.into_boxed_slice()) as *mut *mut CEntry
}

/// Looks up an entry by path inside the archive, free with `free_entry`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn archive_find_entry(
    archive: *mut CArchive,
    path: *const c_char,
) -> *mut CEntry {
    let (Some(archive), Some(path)) = (unsafe { self::archive(archive) }, unsafe { str_arg(path) })
    else {
        return std::ptr::null_mut();
    };

    archive
        .find_archive_entry(Path::new(&path))
        .map_or(std::ptr::null_mut(), entry_to_c)
}
