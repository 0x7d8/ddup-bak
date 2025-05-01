use atomicwrites::{AllowOverwrite, AtomicFile};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    None = 0,
    Destructive = 1,
    NonDestructive = 2,
}

impl LockMode {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => LockMode::Destructive,
            2 => LockMode::NonDestructive,
            _ => LockMode::None,
        }
    }

    fn as_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone)]
pub struct RwLock {
    path: Arc<String>,
    writer_mode: Arc<AtomicU64>,
    writer_present: Arc<AtomicU64>,
    writer_pid: Arc<AtomicU64>,
    reader_counts: Arc<Vec<AtomicU64>>,
    refresh: Arc<Mutex<Option<JoinHandle<()>>>>,
    running: Arc<AtomicU64>,
    process_reader_counts: Arc<Vec<AtomicU64>>,
    process_has_writer: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
struct LockState {
    writer_mode: u8,
    writer_present: u8,
    writer_pid: u64,
    reader_counts: [u64; 3],
}

impl RwLock {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let path_arc = Arc::new(path_str.clone());

        let state = if !path.as_ref().exists() {
            let initial_state = LockState {
                writer_mode: LockMode::None.as_u8(),
                writer_present: 0,
                writer_pid: 0,
                reader_counts: [0; 3],
            };
            Self::write_state(&path_str, &initial_state)?;
            initial_state
        } else {
            Self::read_state(&path_str)?
        };

        let reader_counts = Arc::new(
            (0..3)
                .map(|i| AtomicU64::new(state.reader_counts[i]))
                .collect::<Vec<_>>(),
        );

        let writer_mode = Arc::new(AtomicU64::new(state.writer_mode as u64));
        let writer_present = Arc::new(AtomicU64::new(state.writer_present as u64));
        let writer_pid = Arc::new(AtomicU64::new(state.writer_pid));

        let process_reader_counts = Arc::new((0..3).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());
        let process_has_writer = Arc::new(AtomicU64::new(0));

        let running = Arc::new(AtomicU64::new(1));
        let running_clone = Arc::clone(&running);
        let path_clone = Arc::clone(&path_arc);
        let writer_mode_clone = Arc::clone(&writer_mode);
        let writer_present_clone = Arc::clone(&writer_present);
        let writer_pid_clone = Arc::clone(&writer_pid);
        let reader_counts_clone = Arc::clone(&reader_counts);

        let refresh = thread::spawn(move || {
            while running_clone.load(Ordering::SeqCst) == 1 {
                thread::sleep(Duration::from_millis(100));

                match Self::read_state(&path_clone) {
                    Ok(state) => {
                        writer_mode_clone.store(state.writer_mode as u64, Ordering::SeqCst);
                        writer_present_clone.store(state.writer_present as u64, Ordering::SeqCst);
                        writer_pid_clone.store(state.writer_pid, Ordering::SeqCst);

                        for (i, count) in state.reader_counts.iter().enumerate() {
                            if i < reader_counts_clone.len() {
                                reader_counts_clone[i].store(*count, Ordering::SeqCst);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error in refresh thread: {}", e);
                    }
                }
            }
        });

        Ok(Self {
            path: path_arc,
            writer_mode,
            writer_present,
            writer_pid,
            reader_counts,
            refresh: Arc::new(Mutex::new(Some(refresh))),
            running,
            process_reader_counts,
            process_has_writer,
        })
    }

    fn read_state(path: &str) -> std::io::Result<LockState> {
        let mut file = File::open(path)?;
        let mut reader_counts = [0u64; 3];

        file.seek(SeekFrom::Start(0))?;
        let mut writer_mode_buf = [0; 1];
        file.read_exact(&mut writer_mode_buf)?;
        let writer_mode = writer_mode_buf[0];

        file.seek(SeekFrom::Current(7))?;

        let mut writer_present_buf = [0; 1];
        file.read_exact(&mut writer_present_buf)?;
        let writer_present = writer_present_buf[0];

        file.seek(SeekFrom::Current(7))?;

        let mut writer_pid_buf = [0; 8];
        file.read_exact(&mut writer_pid_buf)?;
        let writer_pid = u64::from_le_bytes(writer_pid_buf);

        for reader_count in reader_counts.iter_mut() {
            let mut count_buf = [0; 8];
            if file.read_exact(&mut count_buf).is_ok() {
                *reader_count = u64::from_le_bytes(count_buf);
            } else {
                break;
            }
        }

        Ok(LockState {
            writer_mode,
            writer_present,
            writer_pid,
            reader_counts,
        })
    }

    fn write_state(path: &str, state: &LockState) -> std::io::Result<()> {
        let atomic_file = AtomicFile::new(path, AllowOverwrite);

        atomic_file.write(|f| {
            f.seek(SeekFrom::Start(0))?;

            f.write_all(&[state.writer_mode])?;
            f.write_all(&[0; 7])?; // Padding

            f.write_all(&[state.writer_present])?;
            f.write_all(&[0; 7])?; // Padding

            f.write_all(&state.writer_pid.to_le_bytes())?;

            for count in &state.reader_counts {
                f.write_all(&count.to_le_bytes())?;
            }

            Ok(())
        })?;

        Ok(())
    }

    fn update_state<F>(&self, update_fn: F) -> std::io::Result<()>
    where
        F: FnOnce(LockState) -> LockState,
    {
        let atomic_file = AtomicFile::new(&*self.path, AllowOverwrite);

        let current_state = Self::read_state(&self.path)?;
        let new_state = update_fn(current_state);

        self.writer_mode
            .store(new_state.writer_mode as u64, Ordering::SeqCst);
        self.writer_present
            .store(new_state.writer_present as u64, Ordering::SeqCst);
        self.writer_pid
            .store(new_state.writer_pid, Ordering::SeqCst);

        for (i, count) in new_state.reader_counts.iter().enumerate() {
            if i < self.reader_counts.len() {
                self.reader_counts[i].store(*count, Ordering::SeqCst);
            }
        }

        atomic_file.write(|f| {
            f.seek(SeekFrom::Start(0))?;

            f.write_all(&[new_state.writer_mode])?;
            f.write_all(&[0; 7])?;

            f.write_all(&[new_state.writer_present])?;
            f.write_all(&[0; 7])?;

            f.write_all(&new_state.writer_pid.to_le_bytes())?;

            for count in &new_state.reader_counts {
                f.write_all(&count.to_le_bytes())?;
            }

            Ok(())
        })?;

        Ok(())
    }

    fn current_pid() -> u64 {
        std::process::id() as u64
    }

    fn process_owns_writer(&self) -> bool {
        self.process_has_writer.load(Ordering::SeqCst) > 0
    }

    pub fn read_lock(&self, mode: LockMode) -> std::io::Result<ReadGuard> {
        if mode == LockMode::None {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot acquire read lock with None mode",
            ));
        }

        if self.process_owns_writer() {
            self.process_reader_counts[mode as usize].fetch_add(1, Ordering::SeqCst);

            return Ok(ReadGuard {
                lock: self.clone(),
                mode,
                active: true,
            });
        }

        let mut backoff = Duration::from_millis(1);
        let max_backoff = Duration::from_secs(1);

        loop {
            let current_writer_mode =
                LockMode::from_u8(self.writer_mode.load(Ordering::SeqCst) as u8);
            let writer_present = self.writer_present.load(Ordering::SeqCst) != 0;
            let writer_pid = self.writer_pid.load(Ordering::SeqCst);
            let current_pid = Self::current_pid();

            if !writer_present || current_writer_mode == mode || writer_pid == current_pid {
                match self.update_state(|mut state| {
                    if state.writer_present != 0
                        && LockMode::from_u8(state.writer_mode) != mode
                        && state.writer_pid != current_pid
                    {
                        return state;
                    }

                    state.reader_counts[mode as usize] += 1;
                    state
                }) {
                    Ok(()) => {
                        self.process_reader_counts[mode as usize].fetch_add(1, Ordering::SeqCst);

                        return Ok(ReadGuard {
                            lock: self.clone(),
                            mode,
                            active: true,
                        });
                    }
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            thread::sleep(backoff);
                            backoff = std::cmp::min(backoff * 2, max_backoff);
                            continue;
                        }
                        return Err(e);
                    }
                }
            }

            thread::sleep(backoff);
            backoff = std::cmp::min(backoff * 2, max_backoff);
        }
    }

    pub fn write_lock(&self, mode: LockMode) -> std::io::Result<WriteGuard> {
        if mode == LockMode::None {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot acquire write lock with None mode",
            ));
        }

        if self.process_owns_writer() {
            self.process_has_writer.fetch_add(1, Ordering::SeqCst);

            return Ok(WriteGuard {
                lock: self.clone(),
                mode,
                active: true,
            });
        }

        let mut backoff = Duration::from_millis(1);
        let max_backoff = Duration::from_secs(1);
        let current_pid = Self::current_pid();

        loop {
            let writer_present = self.writer_present.load(Ordering::SeqCst) != 0;
            let writer_pid = self.writer_pid.load(Ordering::SeqCst);

            let incompatible_readers = (0..3).any(|i| {
                if i == mode as usize {
                    false
                } else {
                    self.reader_counts[i].load(Ordering::SeqCst) > 0
                }
            });

            if (writer_present && writer_pid != current_pid) || incompatible_readers {
                thread::sleep(backoff);
                backoff = std::cmp::min(backoff * 2, max_backoff);
                continue;
            }

            match self.update_state(|mut state| {
                let incompatible_readers = (0..3).any(|i| {
                    if i == mode as usize {
                        false
                    } else {
                        state.reader_counts[i] > 0
                    }
                });

                if (state.writer_present != 0 && state.writer_pid != current_pid)
                    || incompatible_readers
                {
                    return state;
                }

                state.writer_mode = mode.as_u8();
                state.writer_present = 1;
                state.writer_pid = current_pid;
                state
            }) {
                Ok(()) => {
                    self.process_has_writer.store(1, Ordering::SeqCst);

                    return Ok(WriteGuard {
                        lock: self.clone(),
                        mode,
                        active: true,
                    });
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        thread::sleep(backoff);
                        backoff = std::cmp::min(backoff * 2, max_backoff);
                        continue;
                    }

                    return Err(e);
                }
            }
        }
    }

    pub fn try_read_lock(&self, mode: LockMode) -> std::io::Result<Option<ReadGuard>> {
        if mode == LockMode::None {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot acquire read lock with None mode",
            ));
        }

        if self.process_owns_writer() {
            self.process_reader_counts[mode as usize].fetch_add(1, Ordering::SeqCst);

            return Ok(Some(ReadGuard {
                lock: self.clone(),
                mode,
                active: true,
            }));
        }

        let current_writer_mode = LockMode::from_u8(self.writer_mode.load(Ordering::SeqCst) as u8);
        let writer_present = self.writer_present.load(Ordering::SeqCst) != 0;
        let writer_pid = self.writer_pid.load(Ordering::SeqCst);
        let current_pid = Self::current_pid();

        if !writer_present || current_writer_mode == mode || writer_pid == current_pid {
            match self.update_state(|mut state| {
                if state.writer_present != 0
                    && LockMode::from_u8(state.writer_mode) != mode
                    && state.writer_pid != current_pid
                {
                    return state;
                }

                state.reader_counts[mode as usize] += 1;
                state
            }) {
                Ok(()) => {
                    self.process_reader_counts[mode as usize].fetch_add(1, Ordering::SeqCst);

                    return Ok(Some(ReadGuard {
                        lock: self.clone(),
                        mode,
                        active: true,
                    }));
                }
                Err(e) => return Err(e),
            }
        }

        Ok(None)
    }

    pub fn try_write_lock(&self, mode: LockMode) -> std::io::Result<Option<WriteGuard>> {
        if mode == LockMode::None {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot acquire write lock with None mode",
            ));
        }

        if self.process_owns_writer() {
            self.process_has_writer.fetch_add(1, Ordering::SeqCst);

            return Ok(Some(WriteGuard {
                lock: self.clone(),
                mode,
                active: true,
            }));
        }

        let writer_present = self.writer_present.load(Ordering::SeqCst) != 0;
        let writer_pid = self.writer_pid.load(Ordering::SeqCst);
        let current_pid = Self::current_pid();

        let incompatible_readers = (0..3).any(|i| {
            if i == mode as usize {
                false
            } else {
                self.reader_counts[i].load(Ordering::SeqCst) > 0
            }
        });

        if (writer_present && writer_pid != current_pid) || incompatible_readers {
            return Ok(None);
        }

        match self.update_state(|mut state| {
            let incompatible_readers = (0..3).any(|i| {
                if i == mode as usize {
                    false
                } else {
                    state.reader_counts[i] > 0
                }
            });

            if (state.writer_present != 0 && state.writer_pid != current_pid)
                || incompatible_readers
            {
                return state;
            }

            state.writer_mode = mode.as_u8();
            state.writer_present = 1;
            state.writer_pid = current_pid;
            state
        }) {
            Ok(()) => {
                self.process_has_writer.store(1, Ordering::SeqCst);

                Ok(Some(WriteGuard {
                    lock: self.clone(),
                    mode,
                    active: true,
                }))
            }
            Err(e) => Err(e),
        }
    }

    pub fn reader_count(&self, mode: LockMode) -> u64 {
        self.reader_counts[mode as usize].load(Ordering::SeqCst)
    }

    pub fn total_reader_count(&self) -> u64 {
        (0..3)
            .map(|i| self.reader_counts[i].load(Ordering::SeqCst))
            .sum()
    }

    pub fn has_writer(&self) -> bool {
        self.writer_present.load(Ordering::SeqCst) != 0
    }

    pub fn writer_mode(&self) -> Option<LockMode> {
        if self.has_writer() {
            Some(LockMode::from_u8(
                self.writer_mode.load(Ordering::SeqCst) as u8
            ))
        } else {
            None
        }
    }

    pub fn writer_pid(&self) -> Option<u64> {
        if self.has_writer() {
            Some(self.writer_pid.load(Ordering::SeqCst))
        } else {
            None
        }
    }
}

pub struct ReadGuard {
    lock: RwLock,
    mode: LockMode,
    active: bool,
}

impl ReadGuard {
    pub fn mode(&self) -> LockMode {
        self.mode
    }

    pub fn unlock(&mut self) -> std::io::Result<()> {
        if self.active {
            let prev_count =
                self.lock.process_reader_counts[self.mode as usize].fetch_sub(1, Ordering::SeqCst);

            if prev_count == 1 && !self.lock.process_owns_writer() {
                self.lock.update_state(|mut state| {
                    if state.reader_counts[self.mode as usize] > 0 {
                        state.reader_counts[self.mode as usize] -= 1;
                    }
                    state
                })?;
            }

            self.active = false;
        }
        Ok(())
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        if self.active {
            if let Err(e) = self.unlock() {
                eprintln!("Error releasing read lock in drop: {}", e);
            }
        }
    }
}

pub struct WriteGuard {
    lock: RwLock,
    mode: LockMode,
    active: bool,
}

impl WriteGuard {
    pub fn mode(&self) -> LockMode {
        self.mode
    }

    pub fn unlock(&mut self) -> std::io::Result<()> {
        if self.active {
            let prev_count = self.lock.process_has_writer.fetch_sub(1, Ordering::SeqCst);

            if prev_count == 1 {
                self.lock.update_state(|mut state| {
                    let current_pid = RwLock::current_pid();
                    if state.writer_present != 0 && state.writer_pid == current_pid {
                        state.writer_present = 0;
                        state.writer_mode = LockMode::None.as_u8();
                        state.writer_pid = 0;
                    }
                    state
                })?;
            }

            self.active = false;
        }
        Ok(())
    }
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        if self.active {
            if let Err(e) = self.unlock() {
                eprintln!("Error releasing write lock in drop: {}", e);
            }
        }
    }
}

impl Drop for RwLock {
    fn drop(&mut self) {
        self.running.store(0, Ordering::SeqCst);

        if let Ok(mut refresh_guard) = self.refresh.lock() {
            if let Some(handle) = refresh_guard.take() {
                let _ = handle.join();
            }
        }
    }
}
