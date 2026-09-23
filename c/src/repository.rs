use crate::{
    CUserData, UserData,
    archive::{CArchive, CCompressionFormat},
    c_string, set_error, str_arg,
};
use ddup_bak::{
    chunks::{ChunkHash, HashAlgorithm},
    repository::Repository,
};
use std::{ffi::*, path::Path, sync::Arc};

pub type CProgressCallback = Option<extern "C" fn(path: *const c_char, user_data: CUserData)>;
pub type CDeletionProgressCallback =
    Option<extern "C" fn(hash: *const u8, deleted: bool, user_data: CUserData)>;
pub type CRebuildProgressCallback =
    Option<extern "C" fn(hash: *const u8, references: u64, user_data: CUserData)>;
pub type CCompressionFormatCallback = Option<
    extern "C" fn(path: *const c_char, size: u64, user_data: CUserData) -> CCompressionFormat,
>;

/// Chunk hash algorithm of a repository; fixed at creation.
#[repr(C)]
#[derive(Copy, Clone)]
pub enum CHashAlgorithm {
    Blake2b256 = 0,
    Blake3 = 1,
}

impl From<CHashAlgorithm> for HashAlgorithm {
    fn from(value: CHashAlgorithm) -> Self {
        match value {
            CHashAlgorithm::Blake2b256 => HashAlgorithm::Blake2b256,
            CHashAlgorithm::Blake3 => HashAlgorithm::Blake3,
        }
    }
}

#[repr(C)]
pub struct CRepository {
    _private: [u8; 0],
}

fn into_c(repository: std::io::Result<Repository>) -> *mut CRepository {
    match repository {
        Ok(repository) => Box::into_raw(Box::new(repository)) as *mut CRepository,
        Err(err) => {
            set_error(&err);
            std::ptr::null_mut()
        }
    }
}

unsafe fn as_repository<'a>(ptr: *mut CRepository) -> Option<&'a Repository> {
    unsafe { (ptr as *mut Repository).as_ref() }
}

fn status(result: std::io::Result<()>) -> c_int {
    match result {
        Ok(()) => 0,
        Err(err) => {
            set_error(&err);
            -1
        }
    }
}

pub(crate) fn wrap_progress(
    callback: CProgressCallback,
    user_data: UserData,
) -> ddup_bak::archive::ProgressCallback {
    callback.map(|callback| {
        Arc::new(move |path: &Path| {
            callback(
                c_string(path.to_string_lossy().into_owned()).as_ptr(),
                user_data.get(),
            )
        }) as Arc<dyn Fn(&Path) + Send + Sync>
    })
}

fn deletion_callback(
    callback: CDeletionProgressCallback,
    user_data: UserData,
) -> ddup_bak::repository::DeletionProgressCallback {
    callback.map(|callback| {
        Arc::new(move |hash: &ChunkHash, deleted: bool| {
            callback(hash.as_ptr(), deleted, user_data.get())
        }) as Arc<dyn Fn(&ChunkHash, bool) + Send + Sync>
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn new_repository(
    directory: *const c_char,
    chunk_size: c_uint,
    max_chunk_count: c_uint,
) -> *mut CRepository {
    unsafe {
        new_repository_with_hash(
            directory,
            chunk_size,
            max_chunk_count,
            CHashAlgorithm::Blake2b256,
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn new_repository_with_hash(
    directory: *const c_char,
    chunk_size: c_uint,
    max_chunk_count: c_uint,
    hash_algorithm: CHashAlgorithm,
) -> *mut CRepository {
    let Some(directory) = (unsafe { str_arg(directory) }) else {
        return std::ptr::null_mut();
    };

    into_c(Repository::new_with_hash(
        Path::new(&directory),
        chunk_size as usize,
        max_chunk_count as usize,
        hash_algorithm.into(),
        None,
    ))
}

/// Kept for callers of older versions; the index is persisted by every operation.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn repository_save(repo: *mut CRepository) -> c_int {
    match unsafe { as_repository(repo) } {
        Some(repo) => status(repo.save()),
        None => -1,
    }
}

/// Kept for callers of older versions; has no effect.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn repository_set_save_on_drop(
    repo: *mut CRepository,
    save_on_drop: bool,
) -> *mut CRepository {
    let _ = save_on_drop;
    repo
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn open_repository(
    directory: *const c_char,
    chunks_directory: *const c_char,
) -> *mut CRepository {
    let Some(directory) = (unsafe { str_arg(directory) }) else {
        return std::ptr::null_mut();
    };
    let chunks_directory = unsafe { str_arg(chunks_directory) };

    into_c(Repository::open(
        Path::new(&directory),
        chunks_directory.as_deref().map(Path::new),
        None,
    ))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn rebuild_repository(
    directory: *const c_char,
    chunk_size: c_uint,
    max_chunk_count: c_uint,
    chunks_directory: *const c_char,
    progress_callback: CRebuildProgressCallback,
    user_data: CUserData,
) -> *mut CRepository {
    let Some(directory) = (unsafe { str_arg(directory) }) else {
        return std::ptr::null_mut();
    };
    let chunks_directory = unsafe { str_arg(chunks_directory) };
    let user_data = UserData(user_data);
    let progress = progress_callback.map(|callback| {
        Arc::new(move |hash: &ChunkHash, references: u64| {
            callback(hash.as_ptr(), references, user_data.get())
        }) as Arc<dyn Fn(&ChunkHash, u64) + Send + Sync>
    });

    into_c(Repository::rebuild(
        Path::new(&directory),
        chunk_size as usize,
        max_chunk_count as usize,
        chunks_directory.as_deref().map(Path::new),
        None,
        progress,
    ))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_repository(repo: *mut CRepository) {
    if !repo.is_null() {
        drop(unsafe { Box::from_raw(repo as *mut Repository) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn repository_clean(
    repo: *mut CRepository,
    progress_callback: CDeletionProgressCallback,
    user_data: CUserData,
) -> c_int {
    let Some(repo) = (unsafe { as_repository(repo) }) else {
        return -1;
    };

    status(repo.clean(deletion_callback(progress_callback, UserData(user_data))))
}

/// Backs up `directory` into a new archive. A null `directory` backs up the repository directory.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn repository_create_archive(
    repo: *mut CRepository,
    name: *const c_char,
    directory: *const c_char,
    progress_callback: CProgressCallback,
    compression_callback: CCompressionFormatCallback,
    user_data: CUserData,
    threads: c_uint,
) -> *mut CArchive {
    let (Some(repo), Some(name)) = (unsafe { as_repository(repo) }, unsafe { str_arg(name) })
    else {
        return std::ptr::null_mut();
    };
    let directory = unsafe { str_arg(directory) };
    let user_data = UserData(user_data);

    let walker = directory.as_deref().map(|directory| {
        ignore::WalkBuilder::new(directory)
            .follow_links(false)
            .git_global(false)
            .build()
    });
    let compression = compression_callback.map(|callback| {
        Arc::new(move |path: &Path, metadata: &std::fs::Metadata| {
            callback(
                c_string(path.to_string_lossy().into_owned()).as_ptr(),
                metadata.len(),
                user_data.get(),
            )
            .into()
        })
            as Arc<
                dyn Fn(&Path, &std::fs::Metadata) -> ddup_bak::archive::CompressionFormat
                    + Send
                    + Sync,
            >
    });

    let archive = repo.create_archive(
        &name,
        walker,
        directory.as_deref().map(Path::new),
        wrap_progress(progress_callback, user_data),
        compression,
        threads as usize,
    );

    match archive {
        Ok(archive) => CArchive::from_archive(archive),
        Err(err) => {
            set_error(&err);
            std::ptr::null_mut()
        }
    }
}

/// Null-terminated array of archive names, free with `free_string_array`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn repository_list_archives(
    repo: *mut CRepository,
    count: *mut c_uint,
) -> *mut *mut c_char {
    let Some(repo) = (unsafe { as_repository(repo) }) else {
        return std::ptr::null_mut();
    };

    let archives = match repo.list_archives() {
        Ok(archives) => archives,
        Err(err) => {
            set_error(&err);
            return std::ptr::null_mut();
        }
    };

    if !count.is_null() {
        unsafe { *count = archives.len() as c_uint };
    }

    let strings: Vec<*mut c_char> = archives
        .into_iter()
        .map(|name| c_string(name).into_raw())
        .chain(std::iter::once(std::ptr::null_mut()))
        .collect();

    Box::into_raw(strings.into_boxed_slice()) as *mut *mut c_char
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn repository_get_archive(
    repo: *mut CRepository,
    archive_name: *const c_char,
) -> *mut CArchive {
    let (Some(repo), Some(name)) = (unsafe { as_repository(repo) }, unsafe {
        str_arg(archive_name)
    }) else {
        return std::ptr::null_mut();
    };

    match repo.get_archive(&name) {
        Ok(archive) => CArchive::from_archive(archive),
        Err(err) => {
            set_error(&err);
            std::ptr::null_mut()
        }
    }
}

/// Restores an archive into `.ddup-bak/archives-restored/<name>`, replacing a previous restore,
/// and returns that path. Free it with `free_string`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn repository_restore_archive(
    repo: *mut CRepository,
    archive_name: *const c_char,
    progress_callback: CProgressCallback,
    user_data: CUserData,
    threads: c_uint,
) -> *mut c_char {
    let (Some(repo), Some(name)) = (unsafe { as_repository(repo) }, unsafe {
        str_arg(archive_name)
    }) else {
        return std::ptr::null_mut();
    };

    match repo.restore_archive(
        &name,
        wrap_progress(progress_callback, UserData(user_data)),
        threads as usize,
    ) {
        Ok(path) => c_string(path.to_string_lossy().into_owned()).into_raw(),
        Err(err) => {
            set_error(&err);
            std::ptr::null_mut()
        }
    }
}

/// Restores an archive into `destination`, which is created if missing.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn repository_restore_archive_to(
    repo: *mut CRepository,
    archive_name: *const c_char,
    destination: *const c_char,
    progress_callback: CProgressCallback,
    user_data: CUserData,
    threads: c_uint,
) -> c_int {
    let (Some(repo), Some(name), Some(destination)) = (
        unsafe { as_repository(repo) },
        unsafe { str_arg(archive_name) },
        unsafe { str_arg(destination) },
    ) else {
        return -1;
    };

    status(repo.restore_archive_to(
        &name,
        Path::new(&destination),
        wrap_progress(progress_callback, UserData(user_data)),
        threads as usize,
    ))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn repository_delete_archive(
    repo: *mut CRepository,
    archive_name: *const c_char,
    progress_callback: CDeletionProgressCallback,
    user_data: CUserData,
) -> c_int {
    let (Some(repo), Some(name)) = (unsafe { as_repository(repo) }, unsafe {
        str_arg(archive_name)
    }) else {
        return -1;
    };

    status(repo.delete_archive(
        &name,
        deletion_callback(progress_callback, UserData(user_data)),
    ))
}
