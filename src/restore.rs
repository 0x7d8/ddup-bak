//! Staging and rollback for restores which replace existing destination contents.

use std::{
    ffi::{OsStr, OsString},
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const PREFIX: &str = ".ddup-bak-restore-";

fn reserved(name: &OsStr) -> bool {
    name == ".ddup-bak"
        || name == ".ddup-bak-restore"
        || name.to_str().is_some_and(|name| name.starts_with(PREFIX))
}

pub(crate) struct StagedRestore {
    root: PathBuf,
    // Once original entries start moving, even a panic must leave them recoverable.
    retain: bool,
}

impl StagedRestore {
    pub(crate) fn new(destination: &Path) -> io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = loop {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let root = destination.join(format!("{PREFIX}{}-{id}", std::process::id()));
            match std::fs::create_dir(&root) {
                Ok(()) => break root,
                // Never reuse or remove a directory just because its name looks like ours.
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err),
            }
        };
        let staging = Self {
            root,
            retain: false,
        };
        std::fs::create_dir(staging.path())?;
        std::fs::create_dir(staging.previous())?;
        Ok(staging)
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.root.join("new")
    }

    fn previous(&self) -> PathBuf {
        self.root.join("previous")
    }

    pub(crate) fn publish(&mut self, destination: &Path) -> io::Result<()> {
        self.publish_with(destination, |from, to| std::fs::rename(from, to))
    }

    fn publish_with(
        &mut self,
        destination: &Path,
        mut rename: impl FnMut(&Path, &Path) -> io::Result<()>,
    ) -> io::Result<()> {
        // Finish enumeration and validation before modifying a single destination entry.
        let old = names(destination)?
            .into_iter()
            .filter(|name| !reserved(name))
            .collect::<Vec<_>>();
        let new = names(&self.path())?;
        if let Some(name) = new.iter().find(|name| reserved(name)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot replace reserved restore entry {}",
                    name.to_string_lossy()
                ),
            ));
        }

        let mut saved = Vec::new();
        let mut installed = Vec::new();
        self.retain = true;
        let result = (|| {
            for name in &old {
                move_entry(
                    &destination.join(name),
                    &self.previous().join(name),
                    &mut rename,
                )?;
                saved.push(name);
            }
            for name in &new {
                move_entry(
                    &self.path().join(name),
                    &destination.join(name),
                    &mut rename,
                )?;
                installed.push(name);
            }
            crate::chunks::sync_dir(destination)
        })();

        if let Err(original) = result {
            let mut rollback_error = None;
            for name in installed.into_iter().rev() {
                if let Err(err) = move_entry(
                    &destination.join(name),
                    &self.path().join(name),
                    &mut rename,
                ) {
                    rollback_error.get_or_insert(err);
                }
            }
            for name in saved.into_iter().rev() {
                if let Err(err) = move_entry(
                    &self.previous().join(name),
                    &destination.join(name),
                    &mut rename,
                ) {
                    rollback_error.get_or_insert(err);
                }
            }
            if let Some(rollback) = rollback_error {
                // Do not let Drop destroy the only remaining copies of original data.
                return Err(io::Error::new(
                    original.kind(),
                    format!(
                        "restore failed: {original}; rollback failed: {rollback}; original entries \
                         remain in the destination or {}; keep {} for recovery",
                        self.previous().display(),
                        self.root.display(),
                    ),
                ));
            }
            self.retain = false;
            return Err(original);
        }

        // Only now may cleanup remove the originals. A cleanup error cannot turn an already
        // committed replacement into a reported restore failure; Drop reports any leftovers.
        self.retain = false;
        Ok(())
    }
}

fn names(directory: &Path) -> io::Result<Vec<OsString>> {
    let mut names = std::fs::read_dir(directory)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<io::Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

fn move_entry(
    from: &Path,
    to: &Path,
    rename: &mut impl FnMut(&Path, &Path) -> io::Result<()>,
) -> io::Result<()> {
    // In particular, never overwrite an entry if a rollback finds an unexpected occupant.
    match to.symlink_metadata() {
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                to.display().to_string(),
            ));
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    rename(from, to)
}

impl Drop for StagedRestore {
    fn drop(&mut self) {
        if !self.retain
            && let Err(err) = crate::repository::remove_restored(&self.root)
        {
            eprintln!(
                "could not remove restore staging {}: {err}",
                self.root.display()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn destination() -> tempfile::TempDir {
        let t = tempfile::tempdir().unwrap();
        fs::write(t.path().join("a"), b"original a").unwrap();
        fs::create_dir(t.path().join("b")).unwrap();
        fs::write(t.path().join("b/file"), b"original b").unwrap();
        t
    }

    fn staged(destination: &Path) -> StagedRestore {
        let staging = StagedRestore::new(destination).unwrap();
        fs::write(staging.path().join("a"), b"replacement a").unwrap();
        fs::write(staging.path().join("c"), b"new c").unwrap();
        staging
    }

    fn assert_original(destination: &Path) {
        assert_eq!(fs::read(destination.join("a")).unwrap(), b"original a");
        assert_eq!(fs::read(destination.join("b/file")).unwrap(), b"original b");
        assert!(!destination.join("c").exists());
        assert_eq!(
            names(destination).unwrap(),
            [OsString::from("a"), OsString::from("b")]
        );
    }

    #[test]
    fn every_forward_rename_failure_rolls_back_original_contents() {
        // Fail both old-entry moves and both publication moves, including after a new
        // entry has already become visible. Rollback uses the actual filesystem.
        for fail_at in 0..4 {
            let destination = destination();
            let mut staging = staged(destination.path());
            let mut calls = 0;
            let error = staging
                .publish_with(destination.path(), |from, to| {
                    let fail = calls == fail_at;
                    calls += 1;
                    if fail {
                        Err(io::Error::other("injected rename failure"))
                    } else {
                        fs::rename(from, to)
                    }
                })
                .unwrap_err();
            assert!(error.to_string().contains("injected rename failure"));
            drop(staging);
            assert_original(destination.path());
        }
    }

    #[test]
    fn rollback_failure_keeps_originals_and_reports_the_recovery_path() {
        let destination = destination();
        let mut staging = staged(destination.path());
        let root = staging.root.clone();
        let old = staging.previous();
        let new = staging.path();
        let error = staging
            .publish_with(destination.path(), |from, to| {
                if from == new.join("c") || from.starts_with(&old) {
                    Err(io::Error::other("persistent I/O failure"))
                } else {
                    fs::rename(from, to)
                }
            })
            .unwrap_err();
        assert!(error.to_string().contains(&old.display().to_string()));
        drop(staging);
        assert_eq!(fs::read(old.join("a")).unwrap(), b"original a");
        assert_eq!(fs::read(old.join("b/file")).unwrap(), b"original b");
        // A later successful restore also leaves the recovery directory intact.
        let mut next = staged(destination.path());
        next.publish(destination.path()).unwrap();
        drop(next);
        assert!(root.exists());
        assert_eq!(fs::read(old.join("a")).unwrap(), b"original a");
    }

    #[test]
    fn an_unexpected_occupant_is_not_overwritten_during_rollback() {
        let destination = destination();
        let mut staging = staged(destination.path());
        let old = staging.previous();
        let new = staging.path();
        let error = staging
            .publish_with(destination.path(), |from, to| {
                if from == new.join("a") {
                    fs::write(to, b"concurrent writer").unwrap();
                    Err(io::Error::other("publication failed"))
                } else {
                    fs::rename(from, to)
                }
            })
            .unwrap_err();
        assert!(error.to_string().contains("rollback failed"));
        drop(staging);
        assert_eq!(
            fs::read(destination.path().join("a")).unwrap(),
            b"concurrent writer"
        );
        assert_eq!(fs::read(old.join("a")).unwrap(), b"original a");
        assert_eq!(
            fs::read(destination.path().join("b/file")).unwrap(),
            b"original b"
        );
    }

    #[test]
    fn successful_publication_preserves_reserved_entries() {
        let destination = destination();
        for name in [".ddup-bak", ".ddup-bak-restore", ".ddup-bak-restore-user"] {
            fs::create_dir(destination.path().join(name)).unwrap();
            fs::write(destination.path().join(name).join("sentinel"), b"keep").unwrap();
        }
        let mut staging = staged(destination.path());
        let root = staging.root.clone();
        staging.publish(destination.path()).unwrap();
        drop(staging);
        assert!(!root.exists());
        assert_eq!(
            fs::read(destination.path().join("a")).unwrap(),
            b"replacement a"
        );
        assert_eq!(fs::read(destination.path().join("c")).unwrap(), b"new c");
        assert!(!destination.path().join("b").exists());
        for name in [".ddup-bak", ".ddup-bak-restore", ".ddup-bak-restore-user"] {
            assert_eq!(
                fs::read(destination.path().join(name).join("sentinel")).unwrap(),
                b"keep"
            );
        }
    }

    #[test]
    fn a_reserved_archive_entry_is_rejected_before_any_destination_moves() {
        let destination = destination();
        let mut staging = staged(destination.path());
        fs::write(staging.path().join(".ddup-bak-restore"), b"reserved").unwrap();
        assert_eq!(
            staging.publish(destination.path()).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        drop(staging);
        assert_original(destination.path());
    }

    #[cfg(unix)]
    #[test]
    fn rollback_preserves_symlinks_and_read_only_directory_modes() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let destination = destination();
        fs::remove_file(destination.path().join("a")).unwrap();
        symlink("missing", destination.path().join("a")).unwrap();
        fs::set_permissions(
            destination.path().join("b"),
            fs::Permissions::from_mode(0o500),
        )
        .unwrap();
        let mut staging = staged(destination.path());
        let new = staging.path();
        assert!(
            staging
                .publish_with(destination.path(), |from, to| {
                    if from == new.join("c") {
                        Err(io::Error::other("injected"))
                    } else {
                        fs::rename(from, to)
                    }
                })
                .is_err()
        );
        drop(staging);
        assert_eq!(
            fs::read_link(destination.path().join("a")).unwrap(),
            Path::new("missing")
        );
        assert_eq!(
            fs::metadata(destination.path().join("b"))
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o500
        );
        fs::set_permissions(
            destination.path().join("b"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
    }
}
