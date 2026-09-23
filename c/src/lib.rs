#![allow(clippy::missing_safety_doc)]

use std::{cell::RefCell, ffi::*};

pub mod archive;
pub mod entries;
pub mod reader;
pub mod repository;

/// Opaque pointer handed back to every callback unchanged. Callbacks may run on several threads
/// at once.
pub type CUserData = *mut c_void;

#[derive(Clone, Copy)]
pub(crate) struct UserData(pub CUserData);

// SAFETY: the pointer is only handed back to the caller's callbacks, never dereferenced here;
// `CUserData` documents that those callbacks may run on several threads at once.
unsafe impl Send for UserData {}
unsafe impl Sync for UserData {}

impl UserData {
    pub fn get(self) -> CUserData {
        self.0
    }
}

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

pub(crate) fn c_string(value: impl Into<Vec<u8>>) -> CString {
    CString::new(value).unwrap_or_default()
}

pub(crate) fn set_error(err: &std::io::Error) {
    LAST_ERROR.with(|last| *last.borrow_mut() = c_string(err.to_string()));
}

pub(crate) unsafe fn str_arg(ptr: *const c_char) -> Option<String> {
    (!ptr.is_null()).then(|| {
        unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned()
    })
}

/// Message of the last error that happened on the calling thread. Valid until the next failing
/// call on the same thread.
#[unsafe(no_mangle)]
pub extern "C" fn last_error() -> *const c_char {
    LAST_ERROR.with(|last| last.borrow().as_ptr())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(unsafe { CString::from_raw(ptr) });
    }
}

/// Frees a null-terminated array of strings returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_string_array(ptr: *mut *mut c_char) {
    if ptr.is_null() {
        return;
    }

    let mut len = 0;
    unsafe {
        while !(*ptr.add(len)).is_null() {
            drop(CString::from_raw(*ptr.add(len)));
            len += 1;
        }
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
            ptr,
            len + 1,
        )));
    }
}
