use ddup_bak::{
    chunks::{ChunkIndex, HashAlgorithm},
    lock::Lock,
    repository::Repository,
};
use std::{fs, sync::Arc, time::Duration};

#[test]
fn duplicate_archive_creation_is_atomic() {
    let t = tempfile::tempdir().unwrap();
    let root = t.path().join("repo");
    let repo = Arc::new(Repository::new(&root, 4096, 0, None).unwrap());
    let held = Lock::exclusive(&root.join(".ddup-bak/chunks/index.lock")).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut handles = vec![];
    for i in 0..2 {
        let repo = Arc::clone(&repo);
        let source = t.path().join(format!("source-{i}"));
        fs::create_dir(&source).unwrap();
        fs::write(source.join("file"), format!("creator-{i}")).unwrap();
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            repo.create_archive("same-name", None, Some(&source), None, None, 2)
                .map(|_| ())
        }));
    }
    barrier.wait();
    std::thread::sleep(Duration::from_millis(150));
    drop(held);
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let successes = results.iter().filter(|r| r.is_ok()).count();
    eprintln!("duplicate archive create results: {:?}", results);
    assert_eq!(
        successes, 1,
        "exactly one creator should succeed for the same archive name"
    );
    let winner = results.iter().position(|r| r.is_ok()).unwrap();
    assert_eq!(
        results[1 - winner].as_ref().unwrap_err().kind(),
        std::io::ErrorKind::AlreadyExists
    );
    repo.clean(None).unwrap();
    let restored = repo.restore_archive("same-name", None, None, 2).unwrap();
    let expected = format!("creator-{winner}");
    assert_eq!(
        fs::read(restored.join("file")).unwrap(),
        expected.as_bytes()
    );
    let index = ChunkIndex::load(&root.join(".ddup-bak/chunks/index")).unwrap();
    assert_eq!(
        index.references(&HashAlgorithm::default().hash(expected.as_bytes())),
        1
    );
}

#[cfg(unix)]
#[test]
fn restoring_preserves_setuid_and_setgid_bits() {
    use std::os::unix::fs::PermissionsExt;
    let t = tempfile::tempdir().unwrap();
    let root = t.path().join("repo");
    let src = t.path().join("src");
    fs::create_dir(&src).unwrap();
    fs::write(src.join("executable"), b"test").unwrap();
    fs::set_permissions(src.join("executable"), fs::Permissions::from_mode(0o6755)).unwrap();
    fs::create_dir(src.join("shared")).unwrap();
    fs::set_permissions(src.join("shared"), fs::Permissions::from_mode(0o2770)).unwrap();
    let repo = Repository::new(&root, 4096, 0, None).unwrap();
    repo.create_archive("one", None, Some(&src), None, None, 2)
        .unwrap();
    let dst = repo.restore_archive("one", None, None, 2).unwrap();
    let actual = fs::metadata(dst.join("executable"))
        .unwrap()
        .permissions()
        .mode()
        & 0o7777;
    assert_eq!(
        actual, 0o6755,
        "ownership application after chmod must not clear special bits"
    );
    assert_eq!(
        fs::metadata(dst.join("shared"))
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o2770
    );
}

#[cfg(unix)]
#[test]
fn a_dangling_symlink_reserves_an_archive_name() {
    let t = tempfile::tempdir().unwrap();
    let repo = Repository::new(&t.path().join("repo"), 4096, 0, None).unwrap();
    let source = t.path().join("source");
    fs::create_dir(&source).unwrap();
    let archive = repo.archive_path("reserved").unwrap();
    std::os::unix::fs::symlink("absent", &archive).unwrap();
    let err = repo
        .create_archive("reserved", None, Some(&source), None, None, 2)
        .err()
        .unwrap();
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(
        fs::read_link(archive).unwrap(),
        std::path::Path::new("absent")
    );
}

fn corrupt_chunk(root: &std::path::Path) {
    for a in fs::read_dir(root.join(".ddup-bak/chunks"))
        .unwrap()
        .flatten()
    {
        if !a.path().is_dir() {
            continue;
        }
        for b in fs::read_dir(a.path()).unwrap().flatten() {
            for file in fs::read_dir(b.path()).unwrap().flatten() {
                if file
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "chunk")
                {
                    fs::write(file.path(), b"corrupt").unwrap();
                    return;
                }
            }
        }
    }
    panic!("fixture has no chunks");
}

#[test]
fn a_failed_repeated_restore_preserves_the_previous_output() {
    let t = tempfile::tempdir().unwrap();
    let root = t.path().join("repo");
    let source = t.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("file"), b"previous restore").unwrap();
    let repo = Repository::new(&root, 4096, 0, None).unwrap();
    repo.create_archive("one", None, Some(&source), None, None, 2)
        .unwrap();
    let restored = repo.restore_archive("one", None, None, 2).unwrap();
    corrupt_chunk(&root);
    assert!(repo.restore_archive("one", None, None, 2).is_err());
    assert_eq!(
        fs::read(restored.join("file")).unwrap(),
        b"previous restore"
    );
    assert_eq!(fs::read_dir(restored).unwrap().count(), 1);
}

#[cfg(feature = "cli")]
#[test]
fn cli_preserves_a_preexisting_staging_directory_on_failure_and_success() {
    let t = tempfile::tempdir().unwrap();
    let root = t.path().join("repo");
    let source = t.path().join("source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("file"), b"new data").unwrap();
    let repo = Repository::new(&root, 4096, 0, None).unwrap();
    repo.create_archive("one", None, Some(&source), None, None, 2)
        .unwrap();
    let destination = t.path().join("destination");
    let reserved = destination.join(".ddup-bak-restore");
    fs::create_dir_all(&reserved).unwrap();
    fs::write(reserved.join("user-file"), b"keep me").unwrap();
    let restore = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_ddup-bak"))
            .current_dir(&root)
            .args(["backup", "restore", "one"])
            .arg(&destination)
            .output()
            .unwrap()
    };
    let output = restore();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fs::read(reserved.join("user-file")).unwrap(), b"keep me");
    corrupt_chunk(&root);
    assert!(!restore().status.success());
    assert_eq!(fs::read(reserved.join("user-file")).unwrap(), b"keep me");
    assert_eq!(fs::read(destination.join("file")).unwrap(), b"new data");
    assert_eq!(fs::read_dir(destination).unwrap().count(), 2);
}
