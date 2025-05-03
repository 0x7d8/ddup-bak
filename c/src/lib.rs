use std::ffi::*;

pub mod archive;
pub mod entries;
pub mod reader;
pub mod repository;

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn free_string(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }

    unsafe {
        let _ = CString::from_raw(ptr);
    }
}

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn free_string_array(ptr: *mut *mut c_char) {
    if ptr.is_null() {
        return;
    }

    unsafe {
        let mut i = 0;
        while !(*ptr.add(i)).is_null() {
            let _ = CString::from_raw(*ptr.add(i));
            i += 1;
        }

        let _ = Box::from_raw(ptr);
    }
}
