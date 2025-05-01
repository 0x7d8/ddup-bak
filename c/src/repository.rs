use crate::archive::CArchive;
use ddup_bak::repository::Repository;
use std::ffi::*;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::Arc;

pub type CProgressCallback = Option<extern "C" fn(*const c_char)>;
pub type CDeletionProgressCallback = Option<extern "C" fn(chunk_id: u64, deleted: bool)>;

#[repr(C)]
pub struct CRepository {
    _private: [u8; 0],
}

pub struct RepositoryHandle {
    inner: Box<Repository>,
}

impl Deref for RepositoryHandle {
    type Target = Repository;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for RepositoryHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Deref for CRepository {
    type Target = RepositoryHandle;

    fn deref(&self) -> &Self::Target {
        unsafe { &*(self as *const CRepository as *const RepositoryHandle) }
    }
}

impl DerefMut for CRepository {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *(self as *mut CRepository as *mut RepositoryHandle) }
    }
}

impl CRepository {
    pub fn from_repository(repository: Repository) -> *mut CRepository {
        let handle = Box::new(RepositoryHandle {
            inner: Box::new(repository),
        });
        Box::into_raw(handle) as *mut CRepository
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn as_handle(ptr: *const CRepository) -> &'static RepositoryHandle {
        &*(ptr as *const RepositoryHandle)
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn as_handle_mut(ptr: *mut CRepository) -> &'static mut RepositoryHandle {
        &mut *(ptr as *mut RepositoryHandle)
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn into_repository(ptr: *mut CRepository) -> Repository {
        let handle = Box::from_raw(ptr as *mut RepositoryHandle);
        *handle.inner
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn new_repository(
    directory: *const c_char,
    chunk_size: c_uint,
    max_chunk_count: c_uint,
    ignored_files: *const *const c_char,
) -> *mut CRepository {
    let directory = unsafe { CStr::from_ptr(directory).to_string_lossy().into_owned() };
    let ignored_files = unsafe {
        let mut files = Vec::new();
        let mut i = 0;
        while !ignored_files.add(i).is_null() {
            files.push(
                CStr::from_ptr(*ignored_files.add(i))
                    .to_string_lossy()
                    .into_owned(),
            );
            i += 1;
        }

        files
    };

    let repository = Repository::new(
        Path::new(&directory),
        chunk_size as usize,
        max_chunk_count as usize,
        ignored_files,
    );

    CRepository::from_repository(repository)
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn open_repository(
    directory: *const c_char,
    chunks_directory: *const c_char,
) -> *mut CRepository {
    let directory = unsafe { CStr::from_ptr(directory).to_string_lossy().into_owned() };
    let chunks_directory = if chunks_directory.is_null() {
        None
    } else {
        Some(unsafe {
            CStr::from_ptr(chunks_directory)
                .to_string_lossy()
                .into_owned()
        })
    };

    let repository = Repository::open(
        Path::new(&directory),
        chunks_directory.as_ref().map(Path::new),
    );

    match repository {
        Ok(repo) => CRepository::from_repository(repo),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn free_repository(repo: *mut CRepository) {
    if repo.is_null() {
        return;
    }

    unsafe {
        let _ = CRepository::into_repository(repo);
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn repository_save(repo: *mut CRepository) -> c_int {
    let repo = unsafe { &mut *repo };

    match repo.save() {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn repository_set_save_on_drop(
    repo: *mut CRepository,
    save_on_drop: bool,
) -> *mut CRepository {
    let repo = unsafe { &mut *repo };

    repo.set_save_on_drop(save_on_drop);

    repo
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn repository_add_ignored_file(
    repo: *mut CRepository,
    file: *const c_char,
) -> *mut CRepository {
    let repo = unsafe { &mut *repo };
    let file = unsafe { CStr::from_ptr(file).to_string_lossy().into_owned() };

    repo.add_ignored_file(&file);

    repo
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn repository_remove_ignored_file(
    repo: *mut CRepository,
    file: *const c_char,
) -> *mut CRepository {
    let repo = unsafe { &mut *repo };
    let file = unsafe { CStr::from_ptr(file).to_string_lossy().into_owned() };

    repo.remove_ignored_file(&file);

    repo
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn repository_is_ignored(
    repo: *mut CRepository,
    file: *const c_char,
) -> bool {
    let repo = unsafe { &mut *repo };
    let file = unsafe { CStr::from_ptr(file).to_string_lossy().into_owned() };

    repo.is_ignored(&file)
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn repository_get_ignored_files(repo: *mut CRepository) -> *mut *mut c_char {
    if repo.is_null() {
        return std::ptr::null_mut();
    }

    let repo = unsafe { &mut *repo };
    let ignored_files = repo.get_ignored_files();

    let mut files = Vec::with_capacity(ignored_files.len() + 1);

    for file in ignored_files {
        let c_file = CString::new(file.as_str()).unwrap();
        files.push(c_file.into_raw());
    }

    files.push(std::ptr::null_mut());

    let ptr = files.as_mut_ptr();
    std::mem::forget(files);

    ptr
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn repository_clean(
    repo: *mut CRepository,
    progress_callback: CDeletionProgressCallback,
) -> c_int {
    if repo.is_null() {
        return -1;
    }

    let repo = unsafe { &mut *repo };

    let progress_callback = progress_callback.map(|callback_fn| {
        Arc::new(move |chunk_id: u64, deleted: bool| {
            callback_fn(chunk_id, deleted);
        }) as Arc<dyn Fn(u64, bool) + Send + Sync>
    });

    match repo.clean(progress_callback) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn repository_create_archive(
    repo: *mut CRepository,
    name: *const c_char,
    directory: *const c_char,
    progress_chunking: CProgressCallback,
    progress_archiving: CProgressCallback,
    threads: c_uint,
) -> *mut CArchive {
    if repo.is_null() || name.is_null() {
        return std::ptr::null_mut();
    }

    let repo = unsafe { &mut *repo };
    let name = unsafe { CStr::from_ptr(name).to_string_lossy().into_owned() };

    let directory_str = if directory.is_null() {
        None
    } else {
        Some(unsafe { CStr::from_ptr(directory).to_string_lossy().into_owned() })
    };

    let directory_path = directory_str.as_ref().map(Path::new);

    let progress_chunking = progress_chunking.map(|callback_fn| {
        Arc::new(move |path: &std::path::Path| {
            if let Some(path_str) = path.to_str() {
                let c_path = CString::new(path_str).unwrap();
                callback_fn(c_path.as_ptr());
            }
        }) as Arc<dyn Fn(&std::path::Path) + Send + Sync>
    });

    let progress_archiving = progress_archiving.map(|callback_fn| {
        Arc::new(move |path: &std::path::Path| {
            if let Some(path_str) = path.to_str() {
                let c_path = CString::new(path_str).unwrap();
                callback_fn(c_path.as_ptr());
            }
        }) as Arc<dyn Fn(&std::path::Path) + Send + Sync>
    });

    match repo.create_archive(
        &name,
        directory_path,
        progress_chunking,
        progress_archiving,
        threads as usize,
    ) {
        Ok(archive) => CArchive::from_archive(archive),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn repository_list_archives(
    repo: *mut CRepository,
    count: *mut c_uint,
) -> *mut *mut c_char {
    if repo.is_null() || count.is_null() {
        return std::ptr::null_mut();
    }

    let repo = unsafe { &*repo };

    match repo.list_archives() {
        Ok(archives) => {
            unsafe { *count = archives.len() as c_uint };

            let mut c_archives = Vec::with_capacity(archives.len() + 1);

            for archive in archives {
                let c_archive = CString::new(archive).unwrap();
                c_archives.push(c_archive.into_raw());
            }

            c_archives.push(std::ptr::null_mut());

            let ptr = c_archives.as_mut_ptr();
            std::mem::forget(c_archives);

            ptr
        }
        Err(_) => {
            unsafe { *count = 0 };
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn repository_get_archive(
    repo: *mut CRepository,
    archive_name: *const c_char,
) -> *mut CArchive {
    if repo.is_null() || archive_name.is_null() {
        return std::ptr::null_mut();
    }

    let repo = unsafe { &*repo };
    let archive_name = unsafe { CStr::from_ptr(archive_name).to_string_lossy().into_owned() };

    match repo.get_archive(&archive_name) {
        Ok(archive) => CArchive::from_archive(archive),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn repository_restore_archive(
    repo: *mut CRepository,
    archive_name: *const c_char,
    progress_callback: CProgressCallback,
    threads: c_uint,
) -> *mut c_char {
    if repo.is_null() || archive_name.is_null() {
        return std::ptr::null_mut();
    }

    let repo = unsafe { &*repo };
    let archive_name = unsafe { CStr::from_ptr(archive_name).to_string_lossy().into_owned() };

    let progress_callback = progress_callback.map(|callback_fn| {
        Arc::new(move |path: &std::path::Path| {
            if let Some(path_str) = path.to_str() {
                let c_path = CString::new(path_str).unwrap();
                callback_fn(c_path.as_ptr());
            }
        }) as Arc<dyn Fn(&std::path::Path) + Send + Sync>
    });

    match repo.restore_archive(&archive_name, progress_callback, threads as usize) {
        Ok(path) => {
            if let Some(path_str) = path.to_str() {
                let c_path = CString::new(path_str).unwrap();
                c_path.into_raw()
            } else {
                std::ptr::null_mut()
            }
        }
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn repository_delete_archive(
    repo: *mut CRepository,
    archive_name: *const c_char,
    progress_callback: CDeletionProgressCallback,
) -> c_int {
    if repo.is_null() || archive_name.is_null() {
        return -1;
    }

    let repo = unsafe { &mut *repo };
    let archive_name = unsafe { CStr::from_ptr(archive_name).to_string_lossy().into_owned() };

    let progress_callback = progress_callback.map(|callback_fn| {
        Arc::new(move |chunk_id: u64, deleted: bool| {
            callback_fn(chunk_id, deleted);
        }) as Arc<dyn Fn(u64, bool) + Send + Sync>
    });

    match repo.delete_archive(&archive_name, progress_callback) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}
