use colored::Colorize;
use ddup_bak::repository::Repository;
use std::{
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicUsize},
    },
};

pub mod backup;
pub mod init;

pub fn open_repository() -> Repository {
    if let Ok(repository) = Repository::open(Path::new(".")) {
        repository
    } else {
        println!("{}", "repository is not initialized!".red());
        println!(
            "{} {} {}",
            "Run".red(),
            "ddup-bak init .".cyan(),
            "to initialize a new repository.".red()
        );

        std::process::exit(1);
    }
}

const SPINNER: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

pub struct Progress {
    pub total: usize,

    pub text: Arc<RwLock<String>>,
    finished: Arc<AtomicBool>,
    progress: Arc<AtomicUsize>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Clone for Progress {
    fn clone(&self) -> Self {
        Self {
            total: self.total,
            text: Arc::clone(&self.text),
            finished: Arc::clone(&self.finished),
            progress: Arc::clone(&self.progress),
            thread: None,
        }
    }
}

impl Progress {
    pub fn new(total: usize) -> Self {
        Self {
            total,
            text: Arc::new(RwLock::new(String::new())),
            finished: Arc::new(AtomicBool::new(false)),
            progress: Arc::new(AtomicUsize::new(0)),
            thread: None,
        }
    }

    /*#[inline]
    pub fn incr<N: Into<usize>>(&self, n: N) {
        self.progress
            .fetch_add(n.into(), std::sync::atomic::Ordering::SeqCst);
    }*/

    #[inline]
    pub fn set_text<T: Into<String>>(&self, text: T) {
        let mut guard = self.text.write().unwrap();
        *guard = text.into();
    }

    /*#[inline]
    pub fn progress(&self) -> usize {
        self.progress.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[inline]
    pub fn percent(&self) -> f64 {
        (self.progress() as f64 / self.total as f64) * 100.0
    }*/

    pub fn spinner<F>(&mut self, fmt: F)
    where
        F: Fn(&Progress, &str) -> String + Send + Sync + 'static,
    {
        let total = self.total;
        let text = Arc::clone(&self.text);
        let finished = Arc::clone(&self.finished);
        let progress = Arc::clone(&self.progress);

        let thread = std::thread::spawn(move || {
            let mut i = 0;

            loop {
                eprint!(
                    "{}",
                    fmt(
                        &Progress {
                            total,
                            text: Arc::clone(&text),
                            finished: Arc::clone(&finished),
                            progress: Arc::clone(&progress),
                            thread: None
                        },
                        &SPINNER[i].to_string()
                    )
                );

                i = (i + 1) % SPINNER.len();
                std::thread::sleep(std::time::Duration::from_millis(50));

                if finished.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
            }
        });

        self.thread = Some(thread);
    }

    pub fn finish(&mut self) {
        self.finished
            .store(true, std::sync::atomic::Ordering::SeqCst);

        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }

        println!();
    }
}
