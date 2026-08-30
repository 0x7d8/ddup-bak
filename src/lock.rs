use std::{fs::File, path::Path};

/// OS advisory file lock, released when dropped.
pub struct Lock {
    _file: File,
}

impl Lock {
    pub fn shared(path: &Path) -> std::io::Result<Self> {
        let file = crate::fs::open_or_create(path)?;
        file.lock_shared()?;
        Ok(Self { _file: file })
    }

    pub fn exclusive(path: &Path) -> std::io::Result<Self> {
        let file = crate::fs::open_or_create(path)?;
        file.lock()?;
        Ok(Self { _file: file })
    }
}
