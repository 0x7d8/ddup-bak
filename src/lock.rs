use parking_lot::Mutex;
use same_file::Handle;
use std::{
    fs::{File, TryLockError},
    path::Path,
    sync::{Arc, LazyLock},
    time::Duration,
};

const POLL: Duration = Duration::from_millis(10);

static READERS: LazyLock<Mutex<Vec<Arc<Handle>>>> = LazyLock::new(Default::default);

pub struct Lock {
    handle: Arc<Handle>,
    reader: bool,
}

impl Lock {
    pub fn shared(path: &Path) -> std::io::Result<Self> {
        let handle = open(path)?;
        handle.as_file().lock_shared()?;
        Ok(Self {
            handle: Arc::new(handle),
            reader: false,
        })
    }

    pub fn reader(path: &Path) -> std::io::Result<Self> {
        let handle = open(path)?;
        handle.as_file().lock_shared()?;
        let handle = Arc::new(handle);
        READERS.lock().push(Arc::clone(&handle));
        Ok(Self {
            handle,
            reader: true,
        })
    }

    /// Polls instead of blocking, so a reader opened in this process meanwhile makes it fail
    /// with `WouldBlock` instead of hanging.
    pub fn exclusive(path: &Path) -> std::io::Result<Self> {
        let handle = open(path)?;
        loop {
            if READERS.lock().iter().any(|reader| **reader == handle) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!(
                        "{} is held by an entry reader open in this process",
                        path.display()
                    ),
                ));
            }
            match handle.as_file().try_lock() {
                Ok(()) => break,
                Err(TryLockError::WouldBlock) => std::thread::sleep(POLL),
                Err(TryLockError::Error(err)) => return Err(err),
            }
        }
        Ok(Self {
            handle: Arc::new(handle),
            reader: false,
        })
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        if self.reader {
            READERS
                .lock()
                .retain(|reader| !Arc::ptr_eq(reader, &self.handle));
        }
    }
}

fn open(path: &Path) -> std::io::Result<Handle> {
    Handle::from_file(
        File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?,
    )
}
