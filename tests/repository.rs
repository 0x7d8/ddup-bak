use ddup_bak::{
    archive::{
        Archive, CompressionFormat, CompressionFormatCallback,
        entries::{Entry, EntryMode, FileEntry},
    },
    chunks::{
        ChunkIndex, HashAlgorithm,
        storage::{ChunkStorage, ChunkStorageLocal},
    },
    lock::Lock,
    repository::Repository,
};
use std::{
    fs::{self, File},
    io::{Cursor, Read},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};
use tempfile::TempDir;

const CHUNK_SIZE: usize = 4096;

struct Fixture {
    _dir: TempDir,
    root: PathBuf,
    repository: Repository,
}

impl Fixture {
    fn new() -> Self {
        Self::with_storage(|_| None)
    }

    fn with_storage(storage: impl FnOnce(PathBuf) -> Option<Arc<dyn ChunkStorage>>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let chunks = root.join("repo/.ddup-bak/chunks");
        let repository =
            Repository::new(&root.join("repo"), CHUNK_SIZE, 0, storage(chunks.clone())).unwrap();
        fs::create_dir_all(chunks).unwrap();
        Self {
            _dir: dir,
            root,
            repository,
        }
    }

    fn source(&self, name: &str, files: &[(&str, &[u8])]) -> PathBuf {
        let source = self.root.join(name);
        for (path, content) in files {
            let path = source.join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, content).unwrap();
        }
        source
    }

    fn backup(&self, name: &str, source: &Path) -> std::io::Result<()> {
        let walker = ignore::WalkBuilder::new(source)
            .standard_filters(false)
            .build();
        let compression: CompressionFormatCallback =
            Some(Arc::new(|_, _| CompressionFormat::Deflate));
        self.repository
            .create_archive(name, Some(walker), Some(source), None, compression, 4)?;
        Ok(())
    }

    fn restore(&self, name: &str) -> std::io::Result<PathBuf> {
        let destination = self.root.join(format!("restored-{name}"));
        self.repository
            .restore_archive_to(name, &destination, None, 4)?;
        Ok(destination)
    }

    fn chunks_dir(&self) -> PathBuf {
        self.root.join("repo/.ddup-bak/chunks")
    }

    fn chunk_path(&self, content: &[u8]) -> PathBuf {
        let storage = ChunkStorageLocal(self.chunks_dir());
        self.chunks_dir()
            .join(storage.path_from_chunk(&HashAlgorithm::default().hash(content)))
    }

    fn stored_chunks(&self) -> usize {
        ChunkStorageLocal(self.chunks_dir())
            .list_chunk_hashes()
            .unwrap()
            .len()
    }
}

fn random(len: usize) -> Vec<u8> {
    seeded(0x9E3779B97F4A7C15, len)
}

fn seeded(mut state: u64, len: usize) -> Vec<u8> {
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn assert_same_files(source: &Path, restored: &Path, paths: &[&str]) {
    for path in paths {
        assert_eq!(
            fs::read(source.join(path)).unwrap(),
            fs::read(restored.join(path)).unwrap(),
            "{path}"
        );
    }
}

#[test]
fn roundtrip_restores_every_file_and_metadata() {
    let fixture = Fixture::new();
    let big = random(300 * 1024);
    let source = fixture.source(
        "src",
        &[
            ("big.bin", &big),
            (".hidden", b"hidden"),
            ("sub/.ignore", b"ignored.txt"),
            ("sub/ignored.txt", b"still backed up"),
            ("ro/file", b"read only dir"),
            ("empty", b""),
        ],
    );
    let old = SystemTime::UNIX_EPOCH + Duration::from_secs(1_500_000_000);
    File::open(source.join("sub"))
        .unwrap()
        .set_times(fs::FileTimes::new().set_modified(old))
        .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::os::unix::fs::symlink("big.bin", source.join("link")).unwrap();
        fs::set_permissions(source.join("ro"), fs::Permissions::from_mode(0o555)).unwrap();
    }

    fixture.backup("b", &source).unwrap();
    let restored = fixture.restore("b").unwrap();

    assert_same_files(
        &source,
        &restored,
        &[
            "big.bin",
            ".hidden",
            "sub/.ignore",
            "sub/ignored.txt",
            "ro/file",
            "empty",
        ],
    );
    assert_eq!(
        fs::metadata(restored.join("sub"))
            .unwrap()
            .modified()
            .unwrap(),
        old
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::read_link(restored.join("link")).unwrap(),
            Path::new("big.bin")
        );
        assert_eq!(
            fs::metadata(restored.join("ro"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o555
        );
        fs::set_permissions(source.join("ro"), fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(restored.join("ro"), fs::Permissions::from_mode(0o755)).unwrap();
    }
}

#[test]
fn rebuild_recreates_the_index_exactly() {
    let fixture = Fixture::new();
    let files: Vec<(String, Vec<u8>)> = (0..5)
        .map(|i| (format!("f{i}"), random(10_000 + i)))
        .collect();
    let refs: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(n, c)| (n.as_str(), c.as_slice()))
        .collect();
    let source = fixture.source("src", &refs);
    fixture.backup("b", &source).unwrap();
    let before = fixture.stored_chunks();

    fs::remove_file(fixture.chunks_dir().join("index")).unwrap();
    assert!(Repository::open(&fixture.root.join("repo"), None, None).is_err());
    let repository =
        Repository::rebuild(&fixture.root.join("repo"), CHUNK_SIZE, 0, None, None, None).unwrap();
    repository.clean(None).unwrap();

    let names: Vec<&str> = refs.iter().map(|(n, _)| *n).collect();
    assert_same_files(&source, &fixture.restore("b").unwrap(), &names);
    assert_eq!(fixture.stored_chunks(), before);

    repository.delete_archive("b", None).unwrap();
    assert_eq!(fixture.stored_chunks(), 0);
}

#[test]
fn delete_keeps_chunks_shared_with_other_archives() {
    let fixture = Fixture::new();
    let shared = random(50_000);
    let first = fixture.source("first", &[("shared", &shared), ("only-first", b"1")]);
    let second = fixture.source("second", &[("shared", &shared), ("only-second", b"2")]);
    fixture.backup("first", &first).unwrap();
    fixture.backup("second", &second).unwrap();

    fixture.repository.delete_archive("first", None).unwrap();
    assert_same_files(
        &second,
        &fixture.restore("second").unwrap(),
        &["shared", "only-second"],
    );
}

struct FailingDelete {
    inner: ChunkStorageLocal,
    deletes: AtomicUsize,
    fail_at: usize,
}

impl ChunkStorage for FailingDelete {
    fn read_chunk_content(
        &self,
        chunk: &ddup_bak::chunks::ChunkHash,
    ) -> std::io::Result<Box<dyn Read + Send + Sync>> {
        self.inner.read_chunk_content(chunk)
    }

    fn write_chunk_content(
        &self,
        chunk: &ddup_bak::chunks::ChunkHash,
        content: &[u8],
    ) -> std::io::Result<()> {
        self.inner.write_chunk_content(chunk, content)
    }

    fn delete_chunk_content(&self, chunk: &ddup_bak::chunks::ChunkHash) -> std::io::Result<()> {
        if self.deletes.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_at {
            return Err(std::io::Error::other("disk gave up"));
        }
        self.inner.delete_chunk_content(chunk)
    }

    fn list_chunk_hashes(&self) -> std::io::Result<Vec<ddup_bak::chunks::ChunkHash>> {
        self.inner.list_chunk_hashes()
    }
}

#[test]
fn interrupted_delete_does_not_leave_the_index_pointing_at_missing_chunks() {
    let fixture = Fixture::with_storage(|chunks| {
        Some(Arc::new(FailingDelete {
            inner: ChunkStorageLocal(chunks),
            deletes: AtomicUsize::new(0),
            fail_at: 2,
        }))
    });
    let source = fixture.source("src", &[("f", &random(50_000))]);
    fixture.backup("a", &source).unwrap();

    assert!(fixture.repository.delete_archive("a", None).is_err());

    fixture.backup("b", &source).unwrap();
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["f"]);
}

#[cfg(unix)]
#[test]
fn a_delete_that_fails_midway_is_finished_by_the_next_deletion() {
    use std::os::unix::fs::PermissionsExt;

    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipped: root ignores directory permissions");
        return;
    }

    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(20_000))]);
    fixture.backup("a", &source).unwrap();
    let chunks = fixture.chunks_dir();

    // Read-only, so no chunk can be removed and the index cannot be written.
    fs::set_permissions(&chunks, fs::Permissions::from_mode(0o555)).unwrap();
    let result = fixture.repository.delete_archive("a", None);
    fs::set_permissions(&chunks, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(result.is_err());
    assert_eq!(
        fixture.repository.list_archives().unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(fixture.repository.pending_deletions().unwrap(), ["a"]);

    fixture.repository.delete_archive("a", None).unwrap();
    assert!(fixture.repository.pending_deletions().unwrap().is_empty());
    assert_eq!(fixture.stored_chunks(), 0);
}

#[test]
fn a_crash_between_chunk_deletion_and_the_index_save_is_recovered() {
    let fixture = Fixture::new();
    let shared = random(50_000);
    let a = fixture.source("a", &[("shared", &shared), ("only-a", &seeded(1, 50_000))]);
    let b = fixture.source("b", &[("shared", &shared), ("only-b", &seeded(2, 50_000))]);
    let storage = ChunkStorageLocal(fixture.chunks_dir());
    fixture.backup("b", &b).unwrap();
    let of_b = storage.list_chunk_hashes().unwrap();
    fixture.backup("a", &a).unwrap();
    let all = fixture.stored_chunks();
    let only_in_a = storage
        .list_chunk_hashes()
        .unwrap()
        .into_iter()
        .find(|hash| !of_b.contains(hash))
        .unwrap();

    // A crash after moving the archive and removing one chunk, before the index was saved.
    let ddup_bak = fixture.root.join("repo/.ddup-bak");
    fs::rename(
        ddup_bak.join("archives/a.ddup"),
        ddup_bak.join("deleting/a.ddup"),
    )
    .unwrap();
    fs::remove_file(
        fixture
            .chunks_dir()
            .join(storage.path_from_chunk(&only_in_a)),
    )
    .unwrap();

    fixture.backup("c", &a).unwrap();
    assert_eq!(fixture.repository.pending_deletions().unwrap(), ["a"]);
    assert_same_files(&a, &fixture.restore("c").unwrap(), &["shared", "only-a"]);
    fixture.repository.clean(None).unwrap();
    assert!(fixture.repository.pending_deletions().unwrap().is_empty());

    fixture.repository.delete_archive("c", None).unwrap();
    fixture.repository.clean(None).unwrap();
    assert!(fixture.stored_chunks() < all);
    assert_same_files(&b, &fixture.restore("b").unwrap(), &["shared", "only-b"]);
}

#[test]
fn a_pending_deletion_does_not_hold_up_the_next_one() {
    let fixture = Fixture::new();
    let shared = random(50_000);
    let x = fixture.source("x", &[("shared", &shared), ("only-x", &seeded(1, 50_000))]);
    let y = fixture.source("y", &[("shared", &shared), ("only-y", &seeded(2, 50_000))]);
    let z = fixture.source("z", &[("shared", &shared)]);
    for (name, source) in [("x", &x), ("y", &y), ("z", &z)] {
        fixture.backup(name, source).unwrap();
    }
    let all = fixture.stored_chunks();

    let ddup_bak = fixture.root.join("repo/.ddup-bak");
    fs::rename(
        ddup_bak.join("archives/x.ddup"),
        ddup_bak.join("deleting/x.ddup"),
    )
    .unwrap();

    fixture.repository.delete_archive("y", None).unwrap();
    assert!(fixture.repository.pending_deletions().unwrap().is_empty());
    assert_eq!(fixture.repository.list_archives().unwrap(), ["z"]);
    assert!(fixture.stored_chunks() < all);
    assert_same_files(&z, &fixture.restore("z").unwrap(), &["shared"]);
}

#[test]
fn a_live_archive_named_like_a_pending_deletion_is_deleted_too() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(20_000))]);
    fixture.backup("a", &source).unwrap();

    // An older version unaware of pending deletions could leave this state.
    let ddup_bak = fixture.root.join("repo/.ddup-bak");
    fs::rename(
        ddup_bak.join("archives/a.ddup"),
        ddup_bak.join("deleting/a.ddup"),
    )
    .unwrap();
    fixture
        .backup("b", &fixture.source("other", &[("g", &seeded(3, 20_000))]))
        .unwrap();
    fs::rename(
        ddup_bak.join("archives/b.ddup"),
        ddup_bak.join("archives/a.ddup"),
    )
    .unwrap();

    fixture.repository.delete_archive("a", None).unwrap();
    assert!(fixture.repository.list_archives().unwrap().is_empty());
    assert!(fixture.repository.pending_deletions().unwrap().is_empty());
    assert_eq!(fixture.stored_chunks(), 0);
}

#[test]
fn a_live_archive_whose_name_the_file_system_equates_with_a_pending_one_is_deleted_too() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(20_000))]);
    fixture.backup("a", &source).unwrap();

    let ddup_bak = fixture.root.join("repo/.ddup-bak");
    fs::rename(
        ddup_bak.join("archives/a.ddup"),
        ddup_bak.join("deleting/a.ddup"),
    )
    .unwrap();
    fixture
        .backup("b", &fixture.source("other", &[("g", &seeded(3, 20_000))]))
        .unwrap();
    fs::rename(
        ddup_bak.join("archives/b.ddup"),
        ddup_bak.join("archives/A.ddup"),
    )
    .unwrap();

    // On a case-insensitive file system the marker for `A` is the pending `a`.
    fixture.repository.delete_archive("A", None).unwrap();
    assert!(fixture.repository.list_archives().unwrap().is_empty());
    assert!(fixture.repository.pending_deletions().unwrap().is_empty());
    assert_eq!(fixture.stored_chunks(), 0);
}

#[test]
fn chunks_an_interrupted_clean_deleted_are_written_again() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(30_000))]);
    fixture.backup("a", &source).unwrap();

    // An interrupted clean: counts at zero, files already gone.
    let index_path = fixture.chunks_dir().join("index");
    let index = ChunkIndex::load(&index_path).unwrap();
    let storage = ChunkStorageLocal(fixture.chunks_dir());
    for (hash, _) in index.iter().collect::<Vec<_>>() {
        index.set(&hash, 0);
        storage.delete_chunk_content(&hash).unwrap();
    }
    index.save(&index_path).unwrap();

    fixture.backup("b", &source).unwrap();
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["f"]);
}

/// Removes `blocker` after a chunk delete, like a full disk that a delete frees space on.
struct UnlockOnDelete {
    inner: ChunkStorageLocal,
    blocker: PathBuf,
}

impl ChunkStorage for UnlockOnDelete {
    fn read_chunk_content(
        &self,
        chunk: &ddup_bak::chunks::ChunkHash,
    ) -> std::io::Result<Box<dyn Read + Send + Sync>> {
        self.inner.read_chunk_content(chunk)
    }

    fn write_chunk_content(
        &self,
        chunk: &ddup_bak::chunks::ChunkHash,
        content: &[u8],
    ) -> std::io::Result<()> {
        self.inner.write_chunk_content(chunk, content)
    }

    fn delete_chunk_content(&self, chunk: &ddup_bak::chunks::ChunkHash) -> std::io::Result<()> {
        self.inner.delete_chunk_content(chunk)?;
        let _ = fs::remove_dir(&self.blocker);
        Ok(())
    }

    fn list_chunk_hashes(&self) -> std::io::Result<Vec<ddup_bak::chunks::ChunkHash>> {
        self.inner.list_chunk_hashes()
    }
}

#[test]
fn a_delete_sharing_a_pending_name_frees_space_before_it_needs_any() {
    let fixture = Fixture::with_storage(|chunks| {
        Some(Arc::new(UnlockOnDelete {
            inner: ChunkStorageLocal(chunks.clone()),
            blocker: chunks.join("index.tmp"),
        }))
    });
    let shared = random(50_000);
    fixture
        .backup("a", &fixture.source("a", &[("shared", &shared)]))
        .unwrap();
    let ddup_bak = fixture.root.join("repo/.ddup-bak");
    fs::rename(
        ddup_bak.join("archives/a.ddup"),
        ddup_bak.join("deleting/a.ddup"),
    )
    .unwrap();
    let b = fixture.source("b", &[("shared", &shared), ("only-b", &seeded(3, 50_000))]);
    fixture.backup("b", &b).unwrap();
    fs::rename(
        ddup_bak.join("archives/b.ddup"),
        ddup_bak.join("archives/a.ddup"),
    )
    .unwrap();

    // The index cannot be saved while `index.tmp` is a directory. The pending `a` shares every
    // chunk with the live one, so only deleting the live one's chunks clears it.
    let blocker = fixture.chunks_dir().join("index.tmp");
    fs::create_dir(&blocker).unwrap();
    let result = fixture.repository.delete_archive("a", None);
    let _ = fs::remove_dir(&blocker);
    result.unwrap();
    assert!(fixture.repository.list_archives().unwrap().is_empty());
    assert!(fixture.repository.pending_deletions().unwrap().is_empty());
    assert_eq!(fixture.stored_chunks(), 0);
}

#[cfg(unix)]
#[test]
fn rebuild_fails_rather_than_skip_an_archive_it_is_not_allowed_to_read() {
    use std::os::unix::fs::PermissionsExt;

    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipped: root ignores file permissions");
        return;
    }

    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(20_000))]);
    fixture.backup("a", &source).unwrap();
    fixture.backup("b", &source).unwrap();
    let repo = fixture.root.join("repo");
    let b = repo.join(".ddup-bak/archives/b.ddup");

    fs::set_permissions(&b, fs::Permissions::from_mode(0o000)).unwrap();
    let result = Repository::rebuild(&repo, CHUNK_SIZE, 0, None, None, None);
    fs::set_permissions(&b, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        result.err().map(|err| err.kind()),
        Some(std::io::ErrorKind::PermissionDenied)
    );

    let repository = Repository::rebuild(&repo, CHUNK_SIZE, 0, None, None, None).unwrap();
    repository.delete_archive("a", None).unwrap();
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["f"]);
}

#[test]
fn opening_an_older_repository_adds_the_deleting_directory() {
    let fixture = Fixture::new();
    let deleting = fixture.root.join("repo/.ddup-bak/deleting");
    fs::remove_dir(&deleting).unwrap();

    Repository::open(&fixture.root.join("repo"), None, None).unwrap();
    assert!(deleting.is_dir());
}

#[test]
fn a_chunk_lost_from_storage_is_written_back_by_the_next_backup_after_rebuild() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(30_000))]);
    fixture.backup("a", &source).unwrap();
    let storage = ChunkStorageLocal(fixture.chunks_dir());
    let lost = storage.list_chunk_hashes().unwrap()[0];
    storage.delete_chunk_content(&lost).unwrap();

    let repo = fixture.root.join("repo");
    Repository::rebuild(&repo, CHUNK_SIZE, 0, None, None, None).unwrap();
    fixture.backup("b", &source).unwrap();
    assert_same_files(&source, &fixture.restore("a").unwrap(), &["f"]);

    fixture.repository.delete_archive("a", None).unwrap();
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["f"]);
}

#[test]
fn a_chunk_lost_from_storage_is_written_back_by_the_next_backup_after_recovery() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(30_000))]);
    fixture.backup("a", &source).unwrap();
    fixture.backup("b", &source).unwrap();
    let ddup_bak = fixture.root.join("repo/.ddup-bak");
    fs::rename(
        ddup_bak.join("archives/a.ddup"),
        ddup_bak.join("deleting/a.ddup"),
    )
    .unwrap();
    let storage = ChunkStorageLocal(fixture.chunks_dir());
    let lost = storage.list_chunk_hashes().unwrap()[0];
    storage.delete_chunk_content(&lost).unwrap();

    fixture.repository.clean(None).unwrap();
    fixture.backup("c", &source).unwrap();
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["f"]);

    fixture.repository.delete_archive("b", None).unwrap();
    assert_same_files(&source, &fixture.restore("c").unwrap(), &["f"]);
}

#[test]
fn a_numbered_marker_is_reported_under_the_archive_name_and_settled_by_it() {
    let fixture = Fixture::new();
    fixture
        .backup("a", &fixture.source("first", &[("f", &random(20_000))]))
        .unwrap();
    fixture
        .backup("b", &fixture.source("second", &[("g", &seeded(3, 20_000))]))
        .unwrap();
    // An older version unaware of pending deletions could leave `a` both pending and live.
    let ddup_bak = fixture.root.join("repo/.ddup-bak");
    fs::rename(
        ddup_bak.join("archives/a.ddup"),
        ddup_bak.join("deleting/a.ddup"),
    )
    .unwrap();
    fs::rename(
        ddup_bak.join("archives/b.ddup"),
        ddup_bak.join("archives/a.ddup"),
    )
    .unwrap();

    let blocker = fixture.chunks_dir().join("index.tmp");
    fs::create_dir(&blocker).unwrap();
    assert!(fixture.repository.delete_archive("a", None).is_err());
    fs::remove_dir(&blocker).unwrap();
    assert!(ddup_bak.join("deleting/a.ddup.1").is_file());
    assert_eq!(fixture.repository.pending_deletions().unwrap(), ["a"]);

    fixture.repository.delete_archive("a", None).unwrap();
    assert!(fixture.repository.pending_deletions().unwrap().is_empty());
    assert_eq!(fixture.stored_chunks(), 0);
}

#[test]
fn a_directory_where_a_chunk_should_be_fails_the_backup() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(30_000))]);
    fixture.backup("a", &source).unwrap();
    let storage = ChunkStorageLocal(fixture.chunks_dir());
    let hash = storage.list_chunk_hashes().unwrap()[0];
    let path = fixture.chunks_dir().join(storage.path_from_chunk(&hash));
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();

    assert!(fixture.backup("b", &source).is_err());
}

#[test]
fn rebuild_keeps_the_recorded_hash_algorithm_when_storage_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    let repository =
        Repository::new_with_hash(&repo, CHUNK_SIZE, 0, HashAlgorithm::Blake3, None).unwrap();
    fs::create_dir_all(repo.join("src")).unwrap();
    fs::write(repo.join("src/f"), random(20_000)).unwrap();
    let walker = ignore::WalkBuilder::new(repo.join("src"))
        .standard_filters(false)
        .build();
    let compression: CompressionFormatCallback = Some(Arc::new(|_, _| CompressionFormat::Deflate));
    repository
        .create_archive(
            "a",
            Some(walker),
            Some(&repo.join("src")),
            None,
            compression,
            4,
        )
        .unwrap();
    let storage = ChunkStorageLocal(repo.join(".ddup-bak/chunks"));
    for hash in storage.list_chunk_hashes().unwrap() {
        storage.delete_chunk_content(&hash).unwrap();
    }

    let rebuilt = Repository::rebuild(&repo, CHUNK_SIZE, 0, None, None, None).unwrap();
    assert_eq!(rebuilt.hash_algorithm(), HashAlgorithm::Blake3);
}

#[test]
fn a_live_archive_with_the_longest_name_sharing_a_pending_one_is_deleted_too() {
    let fixture = Fixture::new();
    let name = "n".repeat(250);
    let source = fixture.source("src", &[("f", &random(20_000))]);
    fixture.backup(&name, &source).unwrap();
    let ddup_bak = fixture.root.join("repo/.ddup-bak");
    fs::rename(
        ddup_bak.join(format!("archives/{name}.ddup")),
        ddup_bak.join(format!("deleting/{name}.ddup")),
    )
    .unwrap();
    fixture
        .backup("b", &fixture.source("other", &[("g", &seeded(3, 20_000))]))
        .unwrap();
    fs::rename(
        ddup_bak.join("archives/b.ddup"),
        ddup_bak.join(format!("archives/{name}.ddup")),
    )
    .unwrap();

    fixture.repository.delete_archive(&name, None).unwrap();
    assert!(fixture.repository.list_archives().unwrap().is_empty());
    assert!(fixture.repository.pending_deletions().unwrap().is_empty());
    assert_eq!(fixture.stored_chunks(), 0);
}

struct InterruptOnce {
    inner: ChunkStorageLocal,
    done: std::sync::atomic::AtomicBool,
}

impl ChunkStorage for InterruptOnce {
    fn read_chunk_content(
        &self,
        chunk: &ddup_bak::chunks::ChunkHash,
    ) -> std::io::Result<Box<dyn Read + Send + Sync>> {
        if !self.done.swap(true, Ordering::SeqCst) {
            return Err(std::io::Error::from(std::io::ErrorKind::Interrupted));
        }
        self.inner.read_chunk_content(chunk)
    }

    fn write_chunk_content(
        &self,
        chunk: &ddup_bak::chunks::ChunkHash,
        content: &[u8],
    ) -> std::io::Result<()> {
        self.inner.write_chunk_content(chunk, content)
    }

    fn delete_chunk_content(&self, chunk: &ddup_bak::chunks::ChunkHash) -> std::io::Result<()> {
        self.inner.delete_chunk_content(chunk)
    }

    fn list_chunk_hashes(&self) -> std::io::Result<Vec<ddup_bak::chunks::ChunkHash>> {
        self.inner.list_chunk_hashes()
    }
}

#[test]
fn an_interrupted_chunk_read_is_retried_not_skipped() {
    let fixture = Fixture::with_storage(|chunks| {
        Some(Arc::new(InterruptOnce {
            inner: ChunkStorageLocal(chunks),
            done: std::sync::atomic::AtomicBool::new(false),
        }))
    });
    let content = random(3_000);
    let source = fixture.source("src", &[("f", &content)]);
    fixture.backup("a", &source).unwrap();

    let entry = fixture
        .repository
        .get_archive("a")
        .unwrap()
        .into_entries()
        .into_iter()
        .find(|entry| matches!(entry, Entry::File(_)))
        .unwrap();
    let mut read = Vec::new();
    fixture
        .repository
        .read_entry_content(entry, &mut read)
        .unwrap();
    assert_eq!(read, content);
}

#[test]
fn a_storage_with_the_default_existence_check_rejects_a_directory_in_a_chunks_place() {
    let fixture = Fixture::with_storage(|chunks| {
        Some(Arc::new(FailingDelete {
            inner: ChunkStorageLocal(chunks),
            deletes: AtomicUsize::new(0),
            fail_at: usize::MAX,
        }))
    });
    let source = fixture.source("src", &[("f", &random(30_000))]);
    fixture.backup("a", &source).unwrap();
    let storage = ChunkStorageLocal(fixture.chunks_dir());
    let hash = storage.list_chunk_hashes().unwrap()[0];
    let path = fixture.chunks_dir().join(storage.path_from_chunk(&hash));
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();

    assert!(fixture.backup("b", &source).is_err());
    let leftovers = walk(&fixture.chunks_dir())
        .into_iter()
        .filter(|path| path.extension().is_some_and(|ext| ext == "tmp"))
        .count();
    assert_eq!(leftovers, 0);
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            paths.extend(walk(&path));
        } else {
            paths.push(path);
        }
    }
    paths
}

#[test]
fn a_numbered_marker_for_the_longest_name_carries_no_name_and_is_still_settled() {
    let fixture = Fixture::new();
    let name = "n".repeat(250);
    fixture
        .backup(&name, &fixture.source("first", &[("f", &random(20_000))]))
        .unwrap();
    fixture
        .backup("b", &fixture.source("second", &[("g", &seeded(3, 20_000))]))
        .unwrap();
    let ddup_bak = fixture.root.join("repo/.ddup-bak");
    fs::rename(
        ddup_bak.join(format!("archives/{name}.ddup")),
        ddup_bak.join(format!("deleting/{name}.ddup")),
    )
    .unwrap();
    fs::rename(
        ddup_bak.join("archives/b.ddup"),
        ddup_bak.join(format!("archives/{name}.ddup")),
    )
    .unwrap();

    let blocker = fixture.chunks_dir().join("index.tmp");
    fs::create_dir(&blocker).unwrap();
    assert!(fixture.repository.delete_archive(&name, None).is_err());
    fs::remove_dir(&blocker).unwrap();
    assert!(ddup_bak.join("deleting/.ddup.1").is_file());
    assert_eq!(
        fixture.repository.pending_deletions().unwrap(),
        [name.as_str()]
    );

    fixture.repository.clean(None).unwrap();
    assert!(fixture.repository.pending_deletions().unwrap().is_empty());
    assert!(
        fs::read_dir(ddup_bak.join("deleting"))
            .unwrap()
            .next()
            .is_none()
    );
    assert_eq!(fixture.stored_chunks(), 0);
}

#[cfg(unix)]
#[test]
fn rebuild_fails_rather_than_replace_an_index_it_is_not_allowed_to_read() {
    use std::os::unix::fs::PermissionsExt;

    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipped: root ignores file permissions");
        return;
    }

    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(20_000))]);
    fixture.backup("a", &source).unwrap();
    let index = fixture.chunks_dir().join("index");
    let before = fs::read(&index).unwrap();

    fs::set_permissions(&index, fs::Permissions::from_mode(0o000)).unwrap();
    let result = Repository::rebuild(&fixture.root.join("repo"), CHUNK_SIZE, 0, None, None, None);
    fs::set_permissions(&index, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        result.err().map(|err| err.kind()),
        Some(std::io::ErrorKind::PermissionDenied)
    );
    assert_eq!(fs::read(&index).unwrap(), before);
}

#[test]
fn an_empty_chunk_file_is_written_over_by_the_next_backup() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(30_000))]);
    fixture.backup("a", &source).unwrap();
    let storage = ChunkStorageLocal(fixture.chunks_dir());
    let hash = storage.list_chunk_hashes().unwrap()[0];
    fs::write(
        fixture.chunks_dir().join(storage.path_from_chunk(&hash)),
        b"",
    )
    .unwrap();

    fixture.backup("b", &source).unwrap();
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["f"]);
}

#[test]
fn a_nameless_marker_neither_stops_the_next_backup_nor_survives_clean() {
    let fixture = Fixture::new();
    fixture
        .backup("a", &fixture.source("first", &[("f", &random(20_000))]))
        .unwrap();
    let ddup_bak = fixture.root.join("repo/.ddup-bak");
    fs::rename(
        ddup_bak.join("archives/a.ddup"),
        ddup_bak.join("deleting/.ddup.1"),
    )
    .unwrap();

    fixture
        .backup("b", &fixture.source("second", &[("g", &seeded(3, 20_000))]))
        .unwrap();
    fixture.repository.clean(None).unwrap();
    assert!(
        fs::read_dir(ddup_bak.join("deleting"))
            .unwrap()
            .next()
            .is_none()
    );
    assert_same_files(
        &fixture.root.join("second"),
        &fixture.restore("b").unwrap(),
        &["g"],
    );
    fixture.repository.delete_archive("b", None).unwrap();
    assert_eq!(fixture.stored_chunks(), 0);
}

#[test]
fn a_storage_with_the_default_existence_check_writes_over_an_empty_chunk() {
    let fixture = Fixture::with_storage(|chunks| {
        Some(Arc::new(FailingDelete {
            inner: ChunkStorageLocal(chunks),
            deletes: AtomicUsize::new(0),
            fail_at: usize::MAX,
        }))
    });
    let source = fixture.source("src", &[("f", &random(30_000))]);
    fixture.backup("a", &source).unwrap();
    let storage = ChunkStorageLocal(fixture.chunks_dir());
    let hash = storage.list_chunk_hashes().unwrap()[0];
    fs::write(
        fixture.chunks_dir().join(storage.path_from_chunk(&hash)),
        b"",
    )
    .unwrap();

    fixture.backup("b", &source).unwrap();
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["f"]);
}

#[test]
fn clean_removes_temporary_chunk_files_left_by_interrupted_writes() {
    let fixture = Fixture::new();
    let leftover = fixture
        .chunks_dir()
        .join(format!("ab/cd/{}.1234.5.tmp", "e".repeat(60)));
    fs::create_dir_all(leftover.parent().unwrap()).unwrap();
    fs::write(&leftover, b"partial").unwrap();
    // Kept: a `.tmp` named unlike a chunk, and one reached through a symlink.
    let other = fixture.chunks_dir().join("ab/cd/notes.tmp");
    fs::write(&other, b"kept").unwrap();
    let outside = fixture.root.join("outside/cd");
    fs::create_dir_all(&outside).unwrap();
    let document = outside.join(format!("{}.1.2.tmp", "f".repeat(60)));
    fs::write(&document, b"kept").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(
        fixture.root.join("outside"),
        fixture.chunks_dir().join("zz"),
    )
    .unwrap();

    // Kept: a chunk-like `.tmp` outside the two levels of hex directories.
    let elsewhere = fixture
        .chunks_dir()
        .join(format!("notes/saved/{}.123.4.tmp", "a".repeat(60)));
    fs::create_dir_all(elsewhere.parent().unwrap()).unwrap();
    fs::write(&elsewhere, b"kept").unwrap();

    fixture.repository.clean(None).unwrap();
    assert!(!leftover.exists());
    assert!(other.exists());
    assert!(document.exists());
    assert!(elsewhere.exists());
}

#[cfg(unix)]
#[test]
fn an_earlier_restore_with_read_only_directories_can_be_removed() {
    use std::os::unix::fs::PermissionsExt;

    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipped: root ignores directory permissions");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let tree = dir.path().join("restored");
    fs::create_dir_all(tree.join("d")).unwrap();
    fs::write(tree.join("d/f"), b"x").unwrap();
    fs::set_permissions(tree.join("d"), fs::Permissions::from_mode(0o555)).unwrap();

    ddup_bak::repository::remove_restored(&tree).unwrap();
    assert!(!tree.exists());
    ddup_bak::repository::remove_restored(&tree).unwrap();
}

#[test]
fn a_footer_counting_fewer_entries_than_the_table_holds_is_damage() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(20_000)), ("g", &seeded(3, 20_000))]);
    fixture.backup("a", &source).unwrap();
    let before = fixture.stored_chunks();

    let path = fixture.repository.archive_path("a").unwrap();
    let mut bytes = fs::read(&path).unwrap();
    let footer = bytes.len() - 16;
    assert_eq!(
        u64::from_le_bytes(bytes[footer..footer + 8].try_into().unwrap()),
        2
    );
    bytes[footer..footer + 8].copy_from_slice(&1u64.to_le_bytes());
    fs::write(&path, bytes).unwrap();

    assert_eq!(fixture.repository.unreadable_archives().unwrap(), ["a"]);
    Repository::rebuild(&fixture.root.join("repo"), CHUNK_SIZE, 0, None, None, None).unwrap();
    assert_eq!(
        fixture.repository.clean(None).unwrap_err().kind(),
        std::io::ErrorKind::Unsupported
    );
    assert_eq!(fixture.stored_chunks(), before);
}

#[test]
fn an_index_with_an_impossible_reference_count_is_rejected() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(3_000))]);
    fixture.backup("a", &source).unwrap();

    let index_path = fixture.chunks_dir().join("index");
    let index = ChunkIndex::load(&index_path).unwrap();
    for (hash, _) in index.iter().collect::<Vec<_>>() {
        index.set(&hash, u64::MAX);
    }
    index.save(&index_path).unwrap();

    assert_eq!(
        ChunkIndex::load(&index_path).err().map(|err| err.kind()),
        Some(std::io::ErrorKind::InvalidData)
    );
    assert!(fixture.backup("b", &source).is_err());
    Repository::rebuild(&fixture.root.join("repo"), CHUNK_SIZE, 0, None, None, None).unwrap();
    fixture.backup("b", &source).unwrap();
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["f"]);
}

#[test]
fn an_entry_with_content_but_no_chunks_is_damage() {
    let fixture = Fixture::new();
    let path = fixture.repository.archive_path("bad").unwrap();
    let mut archive = Archive::new(File::create(&path).unwrap()).unwrap();
    let entry = archive
        .write_file_entry(
            std::io::empty(),
            Some(4),
            "f",
            EntryMode::new(0o600),
            SystemTime::UNIX_EPOCH,
            (0, 0),
            CompressionFormat::None,
        )
        .unwrap();
    archive.entries.push(Entry::File(entry));
    archive.write_end_header().unwrap();
    drop(archive);

    assert_eq!(fixture.repository.unreadable_archives().unwrap(), ["bad"]);
    assert_eq!(
        fixture.repository.clean(None).unwrap_err().kind(),
        std::io::ErrorKind::Unsupported
    );
}

#[test]
fn restore_recreates_the_directory_it_restores_into() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", b"back")]);
    fixture.backup("a", &source).unwrap();
    fs::remove_dir_all(fixture.root.join("repo/.ddup-bak/archives-restored")).unwrap();

    let restored = fixture.repository.restore_archive("a", None, 2).unwrap();
    assert_eq!(fs::read(restored.join("f")).unwrap(), b"back");
}

#[test]
fn a_reference_count_at_the_accepted_limit_stays_loadable_after_a_backup() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(3_000))]);
    fixture.backup("a", &source).unwrap();

    let index_path = fixture.chunks_dir().join("index");
    let index = ChunkIndex::load(&index_path).unwrap();
    for (hash, _) in index.iter().collect::<Vec<_>>() {
        index.set(&hash, 1 << 62);
    }
    index.save(&index_path).unwrap();

    fixture.backup("b", &source).unwrap();
    assert!(ChunkIndex::load(&index_path).is_ok());
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["f"]);
}

#[test]
fn a_damaged_reference_count_is_caught_before_it_deletes_a_shared_chunk() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(3_000))]);
    fixture.backup("a", &source).unwrap();
    fixture.backup("b", &source).unwrap();

    // The only entry's count sits after the 8-byte magic, 17 header bytes and 32-byte hash.
    let index_path = fixture.chunks_dir().join("index");
    let mut bytes = fs::read(&index_path).unwrap();
    assert_eq!(bytes[8 + 17 + 32], 2);
    bytes[8 + 17 + 32] = 0;
    fs::write(&index_path, bytes).unwrap();

    assert_eq!(
        ChunkIndex::load(&index_path).err().map(|err| err.kind()),
        Some(std::io::ErrorKind::InvalidData)
    );
    assert!(fixture.repository.delete_archive("a", None).is_err());
    Repository::rebuild(&fixture.root.join("repo"), CHUNK_SIZE, 0, None, None, None).unwrap();
    fixture.repository.delete_archive("a", None).unwrap();
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["f"]);
}

#[test]
fn an_index_written_without_a_checksum_still_loads() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(3_000))]);
    fixture.backup("a", &source).unwrap();

    let index_path = fixture.chunks_dir().join("index");
    let mut bytes = fs::read(&index_path).unwrap();
    bytes[..8].copy_from_slice(b"DDUPIDX3");
    bytes.truncate(bytes.len() - 32);
    fs::write(&index_path, bytes).unwrap();

    assert_eq!(ChunkIndex::load(&index_path).unwrap().len(), 1);
    fixture.backup("b", &source).unwrap();
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["f"]);
}

#[test]
fn a_compressed_chunk_list_cut_short_by_its_declared_size_is_damage() {
    let fixture = Fixture::new();
    let path = fixture.repository.archive_path("bad").unwrap();
    let mut archive = Archive::new(File::create(&path).unwrap()).unwrap();
    let mut entry = archive
        .write_file_entry(
            Cursor::new(vec![1; 64]),
            Some(9),
            "f",
            EntryMode::new(0o600),
            SystemTime::UNIX_EPOCH,
            (0, 0),
            CompressionFormat::Deflate,
        )
        .unwrap();
    entry.size = 32;
    archive.entries.push(Entry::File(entry));
    archive.write_end_header().unwrap();
    drop(archive);

    assert_eq!(fixture.repository.unreadable_archives().unwrap(), ["bad"]);
}

#[test]
fn a_file_whose_chunks_fall_short_of_its_recorded_size_does_not_restore_padded() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", b"12345678")]);
    fixture.backup("a", &source).unwrap();
    let Entry::File(mut file) = fixture
        .repository
        .get_archive("a")
        .unwrap()
        .into_entries()
        .into_iter()
        .find(|entry| matches!(entry, Entry::File(_)))
        .unwrap()
    else {
        unreachable!()
    };
    let hashes = ddup_bak::chunks::entry_hashes(&mut file).unwrap();

    let path = fixture.repository.archive_path("bad").unwrap();
    let mut archive = Archive::new(File::create(&path).unwrap()).unwrap();
    let entry = archive
        .write_file_entry(
            hashes.as_flattened(),
            Some(12),
            "f",
            EntryMode::new(0o600),
            SystemTime::UNIX_EPOCH,
            (0, 0),
            CompressionFormat::None,
        )
        .unwrap();
    archive.entries.push(Entry::File(entry));
    archive.write_end_header().unwrap();
    drop(archive);

    assert!(fixture.restore("bad").is_err());
    let entry = fixture
        .repository
        .get_archive("bad")
        .unwrap()
        .into_entries()
        .into_iter()
        .find(|entry| matches!(entry, Entry::File(_)))
        .unwrap();
    let mut read = Vec::new();
    assert!(
        fixture
            .repository
            .read_entry_content(entry, &mut read)
            .is_err()
    );
}

#[test]
fn open_or_rebuild_rebuilds_an_index_cut_short_after_its_header() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(20_000))]);
    fixture.backup("a", &source).unwrap();
    let index_path = fixture.chunks_dir().join("index");
    let bytes = fs::read(&index_path).unwrap();
    fs::write(&index_path, &bytes[..25]).unwrap();

    let repo = fixture.root.join("repo");
    let repository = Repository::open_or_rebuild(&repo, CHUNK_SIZE, 0, None, None, None).unwrap();
    assert!(ChunkIndex::load(&index_path).is_ok());
    let restored = repository.restore_archive("a", None, 2).unwrap();
    assert_same_files(&source, &restored, &["f"]);
}

#[test]
fn restores_of_one_archive_into_its_own_place_take_turns() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(200_000))]);
    fixture.backup("a", &source).unwrap();

    let restored = std::thread::scope(|scope| {
        let first = scope.spawn(|| fixture.repository.restore_archive("a", None, 2));
        let second = scope.spawn(|| fixture.repository.restore_archive("a", None, 2));
        let first = first.join().unwrap().unwrap();
        assert_eq!(second.join().unwrap().unwrap(), first);
        first
    });
    assert_same_files(&source, &restored, &["f"]);
}

#[cfg(unix)]
#[test]
fn an_archive_whose_compressed_hash_list_is_garbage_counts_as_damaged() {
    use std::os::unix::fs::FileExt;

    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", b"good")]);
    fixture.backup("good", &source).unwrap();

    let path = fixture.repository.archive_path("broken").unwrap();
    let mut archive = Archive::new(File::create(&path).unwrap()).unwrap();
    let entry = archive
        .write_file_entry(
            Cursor::new(vec![0; 32]),
            Some(4),
            "f",
            EntryMode::new(0o600),
            SystemTime::UNIX_EPOCH,
            (0, 0),
            CompressionFormat::Zstd,
        )
        .unwrap();
    entry.file.write_all_at(&[255; 4], entry.offset).unwrap();
    archive.entries.push(Entry::File(entry));
    archive.write_end_header().unwrap();
    drop(archive);

    let repo = fixture.root.join("repo");
    Repository::rebuild(&repo, CHUNK_SIZE, 0, None, None, None).unwrap();
    assert_eq!(
        fixture.repository.unreadable_archives().unwrap(),
        ["broken"]
    );
    fixture.repository.delete_archive("broken", None).unwrap();
    assert_same_files(&source, &fixture.restore("good").unwrap(), &["f"]);
}

#[test]
fn a_tree_nested_deeper_than_an_archive_can_hold_is_refused_before_publication() {
    let fixture = Fixture::new();
    let mut deep = fixture.root.join("src");
    for _ in 0..258 {
        deep.push("d");
    }
    fs::create_dir_all(&deep).unwrap();
    fs::write(deep.join("f"), b"deep").unwrap();

    let err = fixture.backup("a", &fixture.root.join("src")).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(fixture.repository.list_archives().unwrap().is_empty());
}

#[cfg(unix)]
#[test]
fn restore_replaces_its_earlier_output_even_where_that_is_read_only() {
    use std::os::unix::fs::PermissionsExt;

    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipped: root ignores directory permissions");
        return;
    }

    let fixture = Fixture::new();
    let source = fixture.source("src", &[("ro/f", b"kept")]);
    fs::set_permissions(source.join("ro"), fs::Permissions::from_mode(0o500)).unwrap();
    fixture.backup("a", &source).unwrap();

    let first = fixture.repository.restore_archive("a", None, 2).unwrap();
    assert_eq!(fs::read(first.join("ro/f")).unwrap(), b"kept");
    let second = fixture.repository.restore_archive("a", None, 2).unwrap();
    assert_eq!(fs::read(second.join("ro/f")).unwrap(), b"kept");
    fs::set_permissions(source.join("ro"), fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(second.join("ro"), fs::Permissions::from_mode(0o700)).unwrap();
}

/// Lists `first` ahead of the stored chunks. The all-zero hash reads as an empty, unstored chunk.
struct ListFirst {
    first: ddup_bak::chunks::ChunkHash,
    inner: ChunkStorageLocal,
}

impl ChunkStorage for ListFirst {
    fn read_chunk_content(
        &self,
        chunk: &ddup_bak::chunks::ChunkHash,
    ) -> std::io::Result<Box<dyn Read + Send + Sync>> {
        if *chunk == [0; 32] {
            return Ok(Box::new(Cursor::new(Vec::new())));
        }
        self.inner.read_chunk_content(chunk)
    }

    fn write_chunk_content(
        &self,
        chunk: &ddup_bak::chunks::ChunkHash,
        content: &[u8],
    ) -> std::io::Result<()> {
        self.inner.write_chunk_content(chunk, content)
    }

    fn delete_chunk_content(&self, chunk: &ddup_bak::chunks::ChunkHash) -> std::io::Result<()> {
        if *chunk == [0; 32] {
            return Ok(());
        }
        self.inner.delete_chunk_content(chunk)
    }

    fn list_chunk_hashes(&self) -> std::io::Result<Vec<ddup_bak::chunks::ChunkHash>> {
        let mut hashes = vec![self.first];
        hashes.extend(
            self.inner
                .list_chunk_hashes()?
                .into_iter()
                .filter(|hash| *hash != self.first),
        );
        Ok(hashes)
    }
}

#[test]
fn rebuild_tells_the_hash_algorithm_from_a_whole_chunk_not_the_first_listed() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(20_000))]);
    fixture.backup("a", &source).unwrap();
    let repo = fixture.root.join("repo");
    fs::remove_file(fixture.chunks_dir().join("index")).unwrap();

    let storage = Arc::new(ListFirst {
        first: [0; 32],
        inner: ChunkStorageLocal(fixture.chunks_dir()),
    });
    let rebuilt = Repository::rebuild(&repo, CHUNK_SIZE, 0, None, Some(storage), None).unwrap();
    let restored = rebuilt.restore_archive("a", None, 2).unwrap();
    assert_eq!(
        fs::read(restored.join("f")).unwrap(),
        fs::read(source.join("f")).unwrap()
    );
}

#[test]
fn rebuild_moves_past_a_truncated_zstd_chunk_when_telling_the_hash_algorithm() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(40_000))]);
    let walker = ignore::WalkBuilder::new(&source)
        .standard_filters(false)
        .build();
    let compression: CompressionFormatCallback = Some(Arc::new(|_, _| CompressionFormat::Zstd));
    fixture
        .repository
        .create_archive("a", Some(walker), Some(&source), None, compression, 4)
        .unwrap();

    let local = ChunkStorageLocal(fixture.chunks_dir());
    let first = local.list_chunk_hashes().unwrap()[0];
    let path = fixture.chunks_dir().join(local.path_from_chunk(&first));
    let bytes = fs::read(&path).unwrap();
    fs::write(&path, &bytes[..bytes.len() - 1]).unwrap();
    fs::remove_file(fixture.chunks_dir().join("index")).unwrap();

    let storage = Arc::new(ListFirst {
        first,
        inner: local,
    });
    let repo = fixture.root.join("repo");
    let rebuilt = Repository::rebuild(&repo, CHUNK_SIZE, 0, None, Some(storage), None).unwrap();
    assert_eq!(rebuilt.hash_algorithm(), HashAlgorithm::default());
}

#[cfg(unix)]
#[test]
fn a_directory_without_read_permission_restores_with_its_mode() {
    use std::os::unix::fs::PermissionsExt;

    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipped: root ignores directory permissions");
        return;
    }

    let fixture = Fixture::new();
    let entry = ddup_bak::archive::entries::DirectoryEntry {
        name: "x".into(),
        mode: EntryMode::new(0o100),
        owner: (0, 0),
        mtime: SystemTime::UNIX_EPOCH + Duration::from_secs(1_500_000_000),
        entries: Vec::new(),
    };
    let restored = fixture.root.join("restored");
    fixture
        .repository
        .restore_entries_to(vec![Entry::Directory(Box::new(entry))], &restored, None, 2)
        .unwrap();

    let metadata = fs::metadata(restored.join("x")).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o100);
    assert_eq!(
        metadata.modified().unwrap(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_500_000_000)
    );
    fs::set_permissions(restored.join("x"), fs::Permissions::from_mode(0o700)).unwrap();
}

#[test]
fn backing_up_the_repository_directory_leaves_its_own_files_out_entirely() {
    let fixture = Fixture::new();
    let repo = fixture.root.join("repo");
    fs::create_dir_all(repo.join("data")).unwrap();
    fs::write(repo.join("data/f"), random(20_000)).unwrap();
    // An unreadable directory inside `.ddup-bak` must not fail the backup.
    let locked = repo.join(".ddup-bak/archives-restored/locked");
    fs::create_dir(&locked).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
    }

    let result = fixture.backup("a", &repo);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o700)).unwrap();
    }
    result.unwrap();
    let restored = fixture.restore("a").unwrap();
    assert!(restored.join("data/f").is_file());
    assert!(!restored.join(".ddup-bak").exists());

    fixture.repository.delete_archive("a", None).unwrap();
    fixture.repository.clean(None).unwrap();
    assert_eq!(fixture.stored_chunks(), 0);
}

#[test]
fn deleting_an_unreadable_archive_frees_its_chunks_through_a_recount() {
    let fixture = Fixture::new();
    fixture
        .backup("a", &fixture.source("first", &[("f", &random(50_000))]))
        .unwrap();
    fixture
        .backup("b", &fixture.source("second", &[("g", &seeded(3, 50_000))]))
        .unwrap();
    let path = fixture.repository.archive_path("b").unwrap();
    let bytes = fs::read(&path).unwrap();
    fs::write(&path, &bytes[..bytes.len() - 8]).unwrap();

    // Deleting `a` is refused while `b` cannot be read.
    assert!(fixture.repository.delete_archive("a", None).is_err());
    fixture.repository.delete_archive("b", None).unwrap();
    fixture.repository.delete_archive("a", None).unwrap();
    fixture.repository.clean(None).unwrap();
    assert!(fixture.repository.list_archives().unwrap().is_empty());
    assert_eq!(fixture.stored_chunks(), 0);
}

#[test]
fn junk_in_the_deleting_directory_is_cleared_by_a_recount() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(20_000))]);
    fixture.backup("a", &source).unwrap();
    let ddup_bak = fixture.root.join("repo/.ddup-bak");
    fs::write(ddup_bak.join("deleting/junk.ddup"), b"not an archive").unwrap();

    fixture.backup("b", &source).unwrap();
    fixture.repository.clean(None).unwrap();
    assert!(
        fs::read_dir(ddup_bak.join("deleting"))
            .unwrap()
            .next()
            .is_none()
    );
    assert_same_files(&source, &fixture.restore("a").unwrap(), &["f"]);
    fixture.repository.delete_archive("a", None).unwrap();
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["f"]);
}

#[test]
fn an_unreadable_archive_beside_a_pending_deletion_does_not_stop_backups() {
    let fixture = Fixture::new();
    fixture
        .backup("a", &fixture.source("first", &[("f", &random(20_000))]))
        .unwrap();
    fixture
        .backup("b", &fixture.source("second", &[("g", &seeded(3, 20_000))]))
        .unwrap();
    let path = fixture.repository.archive_path("a").unwrap();
    let bytes = fs::read(&path).unwrap();
    fs::write(&path, &bytes[..bytes.len() - 8]).unwrap();
    let ddup_bak = fixture.root.join("repo/.ddup-bak");
    fs::rename(
        ddup_bak.join("archives/b.ddup"),
        ddup_bak.join("deleting/b.ddup"),
    )
    .unwrap();

    let source = fixture.source("third", &[("h", &seeded(4, 20_000))]);
    fixture.backup("c", &source).unwrap();
    assert_same_files(&source, &fixture.restore("c").unwrap(), &["h"]);
}

#[test]
fn clean_removes_archives_left_half_written() {
    let fixture = Fixture::new();
    let archives = fixture.root.join("repo/.ddup-bak/archives");
    fs::write(archives.join(".partial-0123456789abcdef"), b"cut short").unwrap();
    fs::write(archives.join(".migrate-0123456789abcdef"), b"cut short").unwrap();

    fixture.repository.clean(None).unwrap();
    assert!(fs::read_dir(&archives).unwrap().next().is_none());
}

#[test]
fn a_directory_whose_name_merely_starts_like_the_repository_is_backed_up() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[(".ddup-bakup/notes.txt", b"kept")]);
    fixture.backup("a", &source).unwrap();
    assert_same_files(
        &source,
        &fixture.restore("a").unwrap(),
        &[".ddup-bakup/notes.txt"],
    );
}

#[test]
fn clean_reports_each_unreferenced_chunk_once() {
    let fixture = Fixture::new();
    fixture
        .backup("a", &fixture.source("src", &[("f", &random(200_000))]))
        .unwrap();
    let index_path = fixture.chunks_dir().join("index");
    let index = ChunkIndex::load(&index_path).unwrap();
    let chunks = index.len();
    for (hash, _) in index.iter().collect::<Vec<_>>() {
        index.set(&hash, 0);
    }
    index.save(&index_path).unwrap();
    fs::remove_file(fixture.repository.archive_path("a").unwrap()).unwrap();

    let reported = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&reported);
    fixture
        .repository
        .clean(Some(Arc::new(move |_, _| {
            counter.fetch_add(1, Ordering::SeqCst);
        })))
        .unwrap();
    assert!(chunks > 1);
    assert_eq!(reported.load(Ordering::SeqCst), chunks);
    assert_eq!(fixture.stored_chunks(), 0);
}

struct SlowRead(ChunkStorageLocal);

impl ChunkStorage for SlowRead {
    fn read_chunk_content(
        &self,
        chunk: &ddup_bak::chunks::ChunkHash,
    ) -> std::io::Result<Box<dyn Read + Send + Sync>> {
        std::thread::sleep(Duration::from_millis(300));
        self.0.read_chunk_content(chunk)
    }

    fn write_chunk_content(
        &self,
        chunk: &ddup_bak::chunks::ChunkHash,
        content: &[u8],
    ) -> std::io::Result<()> {
        self.0.write_chunk_content(chunk, content)
    }

    fn delete_chunk_content(&self, chunk: &ddup_bak::chunks::ChunkHash) -> std::io::Result<()> {
        self.0.delete_chunk_content(chunk)
    }

    fn list_chunk_hashes(&self) -> std::io::Result<Vec<ddup_bak::chunks::ChunkHash>> {
        self.0.list_chunk_hashes()
    }
}

#[test]
fn a_read_in_flight_makes_deletion_wait_not_fail() {
    let fixture =
        Fixture::with_storage(|chunks| Some(Arc::new(SlowRead(ChunkStorageLocal(chunks)))));
    let content = random(3_000);
    let source = fixture.source("src", &[("f", &content)]);
    fixture.backup("a", &source).unwrap();
    let entry = fixture
        .repository
        .get_archive("a")
        .unwrap()
        .into_entries()
        .into_iter()
        .find(|entry| matches!(entry, Entry::File(_)))
        .unwrap();

    std::thread::scope(|scope| {
        let read = scope.spawn(|| {
            let mut out = Vec::new();
            fixture
                .repository
                .read_entry_content(entry, &mut out)
                .map(|()| out)
        });
        std::thread::sleep(Duration::from_millis(100));
        fixture.repository.delete_archive("a", None).unwrap();
        assert_eq!(read.join().unwrap().unwrap(), content);
    });
}

#[test]
fn archives_with_the_longest_allowed_names_can_be_deleted() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(20_000))]);
    let name = "n".repeat(250);
    fixture.backup(&name, &source).unwrap();

    fixture.repository.delete_archive(&name, None).unwrap();
    assert!(fixture.repository.list_archives().unwrap().is_empty());
    assert_eq!(fixture.stored_chunks(), 0);
}

#[test]
fn concurrent_backups_wait_for_each_other() {
    let fixture = Fixture::new();
    let first = fixture.source("first", &[("f", &random(30_000))]);
    let second = fixture.source("second", &[("g", &random(30_000))]);

    std::thread::scope(|scope| {
        let a = scope.spawn(|| fixture.backup("first", &first));
        let b = scope.spawn(|| fixture.backup("second", &second));
        a.join().unwrap().unwrap();
        b.join().unwrap().unwrap();
    });

    assert_same_files(&first, &fixture.restore("first").unwrap(), &["f"]);
    assert_same_files(&second, &fixture.restore("second").unwrap(), &["g"]);
}

#[test]
fn deletion_waits_for_a_backup_or_restore_in_another_thread() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(20_000))]);
    fixture.backup("a", &source).unwrap();

    // The lock a running backup or restore holds.
    let held = Lock::shared(&fixture.chunks_dir().join("chunks.lock")).unwrap();
    std::thread::scope(|scope| {
        let clean = scope.spawn(|| fixture.repository.clean(None));
        std::thread::sleep(Duration::from_millis(100));
        assert!(!clean.is_finished());

        drop(held);
        clean.join().unwrap().unwrap();
    });
}

#[test]
fn a_reader_opened_while_deletion_waits_makes_it_fail_instead_of_hanging() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(20_000))]);
    fixture.backup("a", &source).unwrap();

    let held = Lock::shared(&fixture.chunks_dir().join("chunks.lock")).unwrap();
    std::thread::scope(|scope| {
        let clean = scope.spawn(|| fixture.repository.clean(None));
        std::thread::sleep(Duration::from_millis(100));
        assert!(!clean.is_finished());

        let entry = fixture
            .repository
            .get_archive("a")
            .unwrap()
            .into_entries()
            .into_iter()
            .find(|entry| matches!(entry, Entry::File(_)))
            .unwrap();
        let _reader = fixture.repository.entry_reader(entry).unwrap();
        drop(held);

        let err = clean.join().unwrap().unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    });
}

#[cfg(unix)]
#[test]
fn a_hard_link_to_the_lock_file_is_the_same_lock() {
    let fixture = Fixture::new();
    let lock = fixture.chunks_dir().join("chunks.lock");
    let link = fixture.root.join("linked.lock");
    let _reader = Lock::reader(&lock).unwrap();
    fs::hard_link(&lock, &link).unwrap();

    let err = Lock::exclusive(&link)
        .err()
        .expect("the link is the same locked file");
    assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
}

#[test]
fn an_open_entry_reader_makes_deletion_fail_instead_of_hanging() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(20_000))]);
    fixture.backup("a", &source).unwrap();

    let entry = fixture
        .repository
        .get_archive("a")
        .unwrap()
        .into_entries()
        .into_iter()
        .find(|entry| matches!(entry, Entry::File(_)))
        .unwrap();
    let reader = fixture.repository.entry_reader(entry).unwrap();

    let err = fixture.repository.clean(None).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(
        fixture
            .repository
            .delete_archive("a", None)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::WouldBlock
    );

    drop(reader);
    fixture.repository.delete_archive("a", None).unwrap();
    assert_eq!(fixture.stored_chunks(), 0);
}

#[test]
fn corrupted_chunks_fail_verification() {
    let fixture = Fixture::new();
    let (a, b) = (vec![b'A'; 500], vec![b'B'; 500]);
    let source = fixture.source("src", &[("a", &a), ("b", &b)]);
    fixture.backup("b", &source).unwrap();

    let (path_a, path_b) = (fixture.chunk_path(&a), fixture.chunk_path(&b));
    let swap = fixture.root.join("swap");
    fs::rename(&path_a, &swap).unwrap();
    fs::rename(&path_b, &path_a).unwrap();
    fs::rename(&swap, &path_b).unwrap();

    assert!(fixture.restore("b").is_err());
}

#[test]
fn truncated_index_is_rejected() {
    let fixture = Fixture::new();
    let source = fixture.source("src", &[("f", &random(20_000))]);
    fixture.backup("b", &source).unwrap();

    let index = fixture.chunks_dir().join("index");
    let bytes = fs::read(&index).unwrap();
    fs::write(&index, &bytes[..bytes.len() * 3 / 4]).unwrap();

    assert!(ChunkIndex::load(&index).is_err());
    assert!(fixture.backup("c", &source).is_err());
}

#[cfg(unix)]
#[test]
fn failed_backup_leaves_nothing_behind() {
    use std::os::unix::fs::PermissionsExt;

    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipped: root ignores directory permissions");
        return;
    }

    let fixture = Fixture::new();
    let content = random(3000);
    let source = fixture.source("src", &[("f", &content)]);
    let blocked = fixture.chunk_path(&content).parent().unwrap().to_path_buf();
    fs::create_dir_all(&blocked).unwrap();
    fs::set_permissions(&blocked, fs::Permissions::from_mode(0o555)).unwrap();

    assert!(fixture.backup("b", &source).is_err());
    assert!(fixture.repository.list_archives().unwrap().is_empty());
    assert!(
        ChunkIndex::load(&fixture.chunks_dir().join("index"))
            .unwrap()
            .is_empty()
    );

    fs::set_permissions(&blocked, fs::Permissions::from_mode(0o755)).unwrap();
    fixture.backup("b", &source).unwrap();
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["f"]);
}

#[test]
fn names_that_escape_their_directory_are_rejected() {
    let fixture = Fixture::new();
    assert!(fixture.repository.get_archive("../etc").is_err());

    let path = fixture.root.join("evil.ddup");
    let mut archive = Archive::new(File::create(&path).unwrap()).unwrap();
    for name in ["../evil", "a/b", "", "."] {
        let result = archive.write_file_entry(
            Cursor::new(Vec::new()),
            None,
            name,
            EntryMode::default(),
            SystemTime::now(),
            (0, 0),
            CompressionFormat::None,
        );
        assert!(result.is_err(), "{name:?} accepted");
    }
}

#[test]
fn the_default_walk_skips_what_it_always_skipped() {
    let fixture = Fixture::new();
    let source = fixture.root.join("source");
    fs::create_dir_all(source.join(".hidden")).unwrap();
    fs::write(source.join(".hidden/inside"), b"hidden").unwrap();
    fs::write(source.join(".env"), b"secret").unwrap();
    fs::write(source.join(".ignore"), b"ignored\n").unwrap();
    fs::write(source.join("ignored"), b"ignored").unwrap();
    fs::write(source.join("kept"), b"kept").unwrap();

    fixture
        .repository
        .create_archive("a", None, Some(&source), None, None, 2)
        .unwrap();
    let restored = fixture.restore("a").unwrap();
    let mut names: Vec<_> = fs::read_dir(&restored)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(names, ["kept"]);

    let walker = ignore::WalkBuilder::new(&source)
        .standard_filters(false)
        .build();
    fixture
        .repository
        .create_archive("b", Some(walker), Some(&source), None, None, 2)
        .unwrap();
    let restored = fixture.restore("b").unwrap();
    assert_same_files(
        &source,
        &restored,
        &[".hidden/inside", ".env", ".ignore", "ignored", "kept"],
    );
}

/// Backslashes are ordinary filename bytes on Unix, e.g. in systemd's escaped unit names.
#[cfg(unix)]
#[test]
fn backslashes_in_names_roundtrip() {
    let fixture = Fixture::new();
    let source = fixture.root.join("source");
    fs::create_dir_all(source.join("dev-disk-by\\x2duuid")).unwrap();
    fs::write(source.join("system-systemd\\x2dveritysetup.slice"), b"unit").unwrap();
    fs::write(source.join("dev-disk-by\\x2duuid/entry"), b"entry").unwrap();

    fixture.backup("a", &source).unwrap();
    assert_same_files(
        &source,
        &fixture.restore("a").unwrap(),
        &[
            "system-systemd\\x2dveritysetup.slice",
            "dev-disk-by\\x2duuid/entry",
        ],
    );
}

#[test]
fn long_names_roundtrip() {
    let fixture = Fixture::new();
    let name = "文".repeat(100);
    let path = fixture.root.join("long.ddup");
    let mut archive = Archive::new(File::create(&path).unwrap()).unwrap();
    let entry = archive
        .write_file_entry(
            Cursor::new(b"x".to_vec()),
            None,
            name.clone(),
            EntryMode::default(),
            SystemTime::now(),
            (0, 0),
            CompressionFormat::None,
        )
        .unwrap();
    archive.entries.push(Entry::File(entry));
    archive.write_end_header().unwrap();

    let reopened = Archive::open(&path).unwrap();
    assert_eq!(reopened.entries()[0].name(), name);
}

#[test]
fn truncated_entry_data_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short");
    fs::write(&path, b"only ten b").unwrap();

    let mut entry = FileEntry {
        name: "x".into(),
        mode: EntryMode::default(),
        owner: (0, 0),
        mtime: SystemTime::now(),
        compression: CompressionFormat::None,
        size_compressed: None,
        size_real: 100,
        size: 100,
        file: Arc::new(File::open(&path).unwrap()),
        offset: 0,
        decoder: None,
        consumed: 0,
    };

    assert!(entry.read_to_end(&mut Vec::new()).is_err());
}

#[test]
fn short_and_unsupported_archives_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short.ddup");
    fs::write(&path, b"DDUPBAK\x02").unwrap();
    assert!(Archive::open(&path).is_err());

    let fixture = Fixture::new();
    let path = fixture.root.join("repo/.ddup-bak/archives/v1.ddup");
    Archive::new(File::create(&path).unwrap())
        .unwrap()
        .write_end_header()
        .unwrap();
    let mut bytes = fs::read(&path).unwrap();
    bytes[7] = 1;
    fs::write(&path, bytes).unwrap();

    assert!(Archive::open(&path).is_ok());
    assert_eq!(
        fixture.repository.get_archive("v1").unwrap_err().kind(),
        std::io::ErrorKind::Unsupported
    );
}

#[test]
fn deflate_compressed_indexes_from_older_versions_load() {
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index");
    let mut encoder = flate2::write::DeflateEncoder::new(
        File::create(&path).unwrap(),
        flate2::Compression::default(),
    );
    encoder.write_all(b"DDUPIDX2").unwrap();
    encoder.write_all(&4096u32.to_le_bytes()).unwrap();
    encoder.write_all(&7u32.to_le_bytes()).unwrap();
    encoder.write_all(&1u64.to_le_bytes()).unwrap();
    encoder.write_all(&[0xAB; 32]).unwrap();
    encoder.write_all(&[3]).unwrap();
    encoder.finish().unwrap();

    let index = ChunkIndex::load(&path).unwrap();
    assert_eq!((index.chunk_size, index.max_chunk_count), (4096, 7));
    assert_eq!(index.references(&[0xAB; 32]), 3);
}

#[test]
fn chunks_that_do_not_compress_are_stored_raw() {
    let fixture = Fixture::new();
    let (noise, text) = (random(3000), vec![b'A'; 3000]);
    let source = fixture.source("src", &[("noise", &noise), ("text", &text)]);
    fixture.backup("b", &source).unwrap();

    let format = |content: &[u8]| fs::read(fixture.chunk_path(content)).unwrap()[0];
    assert_eq!(format(&noise), CompressionFormat::None.encode());
    assert_eq!(format(&text), CompressionFormat::Deflate.encode());
    assert!(fs::metadata(fixture.chunk_path(&text)).unwrap().len() < 100);
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["noise", "text"]);
}

fn leb128(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while value > 0x7F {
        out.push((value & 0x7F) as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
    out
}

/// Writes a repository as versions before archive format 2 did: BLAKE2b chunk names,
/// a Deflate-compressed index keyed by chunk id, and archives listing chunk ids as varints.
fn write_format_1_repository(repo: &Path, files: &[(&str, &[Vec<u8>], CompressionFormat)]) {
    use std::io::Write;

    for sub in ["archives", "archives-restored", "chunks"] {
        fs::create_dir_all(repo.join(".ddup-bak").join(sub)).unwrap();
    }
    let chunks_dir = repo.join(".ddup-bak/chunks");
    let storage = ChunkStorageLocal(chunks_dir.clone());

    let mut records = Vec::new();
    let mut archive =
        Archive::new(File::create(repo.join(".ddup-bak/archives/old.ddup")).unwrap()).unwrap();
    let mut sub = ddup_bak::archive::entries::DirectoryEntry {
        name: "sub".into(),
        mode: EntryMode::new(0o755),
        owner: (0, 0),
        mtime: SystemTime::UNIX_EPOCH,
        entries: Vec::new(),
    };
    for (name, chunks, compression) in files {
        let mut ids = Vec::new();
        for chunk in chunks.iter() {
            let hash = HashAlgorithm::Blake2b256.hash(chunk);
            let id = records.len() as u64 + 1;
            records.push((hash, id));
            ids.extend(leb128(id));

            let mut body = vec![compression.encode()];
            match compression {
                CompressionFormat::None => body.extend_from_slice(chunk),
                CompressionFormat::Deflate => {
                    let mut e = flate2::write::DeflateEncoder::new(
                        &mut body,
                        flate2::Compression::default(),
                    );
                    e.write_all(chunk).unwrap();
                    e.finish().unwrap();
                }
                #[cfg(feature = "brotli")]
                CompressionFormat::Brotli => {
                    let mut e = brotli::CompressorWriter::new(&mut body, 4096, 11, 22);
                    e.write_all(chunk).unwrap();
                }
                other => panic!("{other:?} not used by the fixture"),
            }
            let path = chunks_dir.join(storage.path_from_chunk(&hash));
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, body).unwrap();
        }

        // Format 1 compressed the id list with the file's own format, recorded in the entry header.
        let size_real = chunks.iter().map(|c| c.len() as u64).sum();
        let entry = archive
            .write_file_entry(
                Cursor::new(ids),
                Some(size_real),
                *name,
                EntryMode::new(0o644),
                SystemTime::UNIX_EPOCH,
                (0, 0),
                *compression,
            )
            .unwrap();
        if name.starts_with("in-sub-") {
            sub.entries.push(Entry::File(entry));
        } else {
            archive.entries.push(Entry::File(entry));
        }
    }
    archive.entries.push(Entry::Directory(Box::new(sub)));
    archive.write_end_header().unwrap();
    drop(archive);

    // Byte 7 is the format version. Format 1 differs from 2 only in what file bodies hold.
    let path = repo.join(".ddup-bak/archives/old.ddup");
    let mut bytes = fs::read(&path).unwrap();
    bytes[7] = 1;
    fs::write(&path, bytes).unwrap();

    let mut index = flate2::write::DeflateEncoder::new(
        File::create(chunks_dir.join("index")).unwrap(),
        flate2::Compression::default(),
    );
    index.write_all(&0u64.to_le_bytes()).unwrap();
    index.write_all(&(CHUNK_SIZE as u32).to_le_bytes()).unwrap();
    index.write_all(&0u32.to_le_bytes()).unwrap();
    index
        .write_all(&(records.len() as u64).to_le_bytes())
        .unwrap();
    index
        .write_all(&(records.len() as u64 + 1).to_le_bytes())
        .unwrap();
    for (hash, id) in &records {
        index.write_all(hash).unwrap();
        index.write_all(&leb128(*id)).unwrap();
        index.write_all(&leb128(1)).unwrap();
    }
    index.finish().unwrap();
}

#[test]
fn format_1_repositories_migrate_on_open_and_keep_deduplicating() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    let (a, b, mut c) = (random(1000), vec![b'B'; 900], random(2000));
    c.reverse(); // `random` is deterministic, so this keeps c distinct from a
    #[cfg(feature = "brotli")]
    let b_format = CompressionFormat::Brotli;
    #[cfg(not(feature = "brotli"))]
    let b_format = CompressionFormat::Deflate;
    write_format_1_repository(
        &repo,
        &[
            ("a", std::slice::from_ref(&a), CompressionFormat::Deflate),
            ("in-sub-b", std::slice::from_ref(&b), b_format),
            (
                "c",
                &[c[..1000].to_vec(), c[1000..].to_vec()],
                CompressionFormat::None,
            ),
        ],
    );
    let stored = || {
        ChunkStorageLocal(repo.join(".ddup-bak/chunks"))
            .list_chunk_hashes()
            .unwrap()
            .len()
    };
    assert_eq!(stored(), 4);

    let repository = Repository::open(&repo, None, None).unwrap();
    assert_eq!(repository.hash_algorithm(), HashAlgorithm::Blake2b256);
    assert_eq!(
        Archive::open(repo.join(".ddup-bak/archives/old.ddup"))
            .unwrap()
            .version(),
        2
    );
    assert_eq!(
        ChunkIndex::load_header(&repo.join(".ddup-bak/chunks/index"))
            .unwrap()
            .version,
        4
    );

    for entry in Archive::open(repo.join(".ddup-bak/archives/old.ddup"))
        .unwrap()
        .entries()
    {
        let expected = if entry.name() == "sub" { 0o755 } else { 0o644 };
        assert_eq!(entry.mode().bits(), expected, "{}", entry.name());
    }
    let restored = repository.restore_archive("old", None, 2).unwrap();
    assert_eq!(restored, repo.join(".ddup-bak/archives-restored/old"));
    assert_eq!(fs::read(restored.join("a")).unwrap(), a);
    assert_eq!(fs::read(restored.join("sub/in-sub-b")).unwrap(), b);
    assert_eq!(fs::read(restored.join("c")).unwrap(), c);

    // New backups must dedup against the migrated BLAKE2b chunks.
    let repository = Repository::open(&repo, None, None).unwrap();
    let source = dir.path().join("src");
    fs::create_dir_all(source.join("sub")).unwrap();
    fs::write(source.join("a"), &a).unwrap();
    fs::write(source.join("sub/in-sub-b"), &b).unwrap();
    let walker = ignore::WalkBuilder::new(&source)
        .standard_filters(false)
        .build();
    repository
        .create_archive("new", Some(walker), Some(&source), None, None, 2)
        .unwrap();
    assert_eq!(stored(), 4);

    fs::remove_file(repo.join(".ddup-bak/chunks/index")).unwrap();
    let rebuilt = Repository::rebuild(&repo, CHUNK_SIZE, 0, None, None, None).unwrap();
    assert_eq!(rebuilt.hash_algorithm(), HashAlgorithm::Blake2b256);
    let index = ChunkIndex::load(&repo.join(".ddup-bak/chunks/index")).unwrap();
    assert_eq!(index.references(&HashAlgorithm::Blake2b256.hash(&a)), 2);
    assert_eq!(
        index.references(&HashAlgorithm::Blake2b256.hash(&c[..1000])),
        1
    );
}

#[test]
fn a_format_1_index_ending_short_of_its_records_is_rejected_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    let contents: Vec<Vec<u8>> = (0..8u32).map(|i| random(200 + i as usize)).collect();
    let files: Vec<_> = contents
        .iter()
        .map(|c| ("f", std::slice::from_ref(c), CompressionFormat::Deflate))
        .collect();
    write_format_1_repository(&repo, &files);
    let index = repo.join(".ddup-bak/chunks/index");

    // Re-encode just the 32-byte header and one 34-byte record, so the stream ends cleanly.
    let mut plain = Vec::new();
    flate2::read::DeflateDecoder::new(File::open(&index).unwrap())
        .read_to_end(&mut plain)
        .unwrap();
    let mut short = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut short, &plain[..32 + 34]).unwrap();
    fs::write(&index, short.finish().unwrap()).unwrap();

    assert!(ChunkIndex::load_v1(&index).is_err());
    assert!(Repository::open(&repo, None, None).is_err());
    assert_eq!(
        Archive::open(repo.join(".ddup-bak/archives/old.ddup"))
            .unwrap()
            .version(),
        1
    );
}

#[test]
fn a_format_1_archive_with_the_longest_name_migrates() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    let content = random(5_000);
    write_format_1_repository(
        &repo,
        &[(
            "f",
            std::slice::from_ref(&content),
            CompressionFormat::Deflate,
        )],
    );
    let name = "n".repeat(250);
    let archives = repo.join(".ddup-bak/archives");
    fs::rename(
        archives.join("old.ddup"),
        archives.join(format!("{name}.ddup")),
    )
    .unwrap();

    let repository = Repository::open(&repo, None, None).unwrap();
    assert_eq!(repository.get_archive(&name).unwrap().version(), 2);
    let restored = repository.restore_archive(&name, None, 2).unwrap();
    assert_eq!(fs::read(restored.join("f")).unwrap(), content);
}

#[test]
fn migration_counts_references_from_the_archives_not_the_old_index() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    let content = random(5_000);
    write_format_1_repository(
        &repo,
        &[(
            "f",
            std::slice::from_ref(&content),
            CompressionFormat::Deflate,
        )],
    );
    // An older version's interrupted backup: a second archive the index never counted.
    let archives = repo.join(".ddup-bak/archives");
    fs::copy(archives.join("old.ddup"), archives.join("twin.ddup")).unwrap();

    let repository = Repository::open(&repo, None, None).unwrap();
    repository.delete_archive("old", None).unwrap();
    let restored = repository.restore_archive("twin", None, 2).unwrap();
    assert_eq!(fs::read(restored.join("f")).unwrap(), content);
}

#[test]
fn an_archive_with_a_damaged_body_blocks_deletion_but_neither_migration_nor_its_own_removal() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    let content = random(5_000);
    write_format_1_repository(
        &repo,
        &[(
            "f",
            std::slice::from_ref(&content),
            CompressionFormat::Deflate,
        )],
    );
    // A format 2 archive whose entry table reads but whose hash list is one byte.
    let bad = repo.join(".ddup-bak/archives/bad.ddup");
    let mut archive = Archive::new(File::create(&bad).unwrap()).unwrap();
    let entry = archive
        .write_file_entry(
            Cursor::new(vec![0u8]),
            Some(1),
            "f",
            EntryMode::new(0o644),
            SystemTime::UNIX_EPOCH,
            (0, 0),
            CompressionFormat::None,
        )
        .unwrap();
    archive.entries.push(Entry::File(entry));
    archive.write_end_header().unwrap();
    drop(archive);

    let repository = Repository::open(&repo, None, None).unwrap();
    assert_eq!(repository.get_archive("old").unwrap().version(), 2);
    assert_eq!(repository.unreadable_archives().unwrap(), ["bad"]);
    assert_eq!(
        repository.delete_archive("old", None).unwrap_err().kind(),
        std::io::ErrorKind::Unsupported
    );

    repository.delete_archive("bad", None).unwrap();
    repository.delete_archive("old", None).unwrap();
    repository.clean(None).unwrap();
    assert!(
        ChunkStorageLocal(repo.join(".ddup-bak/chunks"))
            .list_chunk_hashes()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn a_legacy_chunk_larger_than_this_version_ever_writes_still_reads() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    let big = vec![7u8; 17 << 20];
    write_format_1_repository(
        &repo,
        &[("f", std::slice::from_ref(&big), CompressionFormat::Deflate)],
    );

    let repository = Repository::open(&repo, None, None).unwrap();
    let restored = repository.restore_archive("old", None, 2).unwrap();
    assert_eq!(fs::read(restored.join("f")).unwrap(), big);
}

#[test]
fn a_damaged_format_1_archive_migrates_once_it_reads_again() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    let content = random(5_000);
    write_format_1_repository(
        &repo,
        &[(
            "f",
            std::slice::from_ref(&content),
            CompressionFormat::Deflate,
        )],
    );
    let archives = repo.join(".ddup-bak/archives");
    let intact = fs::read(archives.join("old.ddup")).unwrap();
    fs::write(archives.join("twin.ddup"), &intact[..intact.len() - 8]).unwrap();
    let ids = repo.join(".ddup-bak/chunks/index.v1");

    let repository = Repository::open(&repo, None, None).unwrap();
    assert_eq!(repository.get_archive("old").unwrap().version(), 2);
    assert_eq!(repository.unreadable_archives().unwrap(), ["twin"]);
    assert!(ids.exists());

    fs::write(archives.join("twin.ddup"), &intact).unwrap();
    let repository = Repository::open(&repo, None, None).unwrap();
    assert_eq!(repository.get_archive("twin").unwrap().version(), 2);
    assert!(!ids.exists());
    repository.delete_archive("old", None).unwrap();
    let restored = repository.restore_archive("twin", None, 2).unwrap();
    assert_eq!(fs::read(restored.join("f")).unwrap(), content);
}

#[test]
fn rebuild_recovers_what_a_damaged_format_1_index_still_covers() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    let contents: Vec<Vec<u8>> = (0..400u32).map(|i| random(200 + i as usize)).collect();
    let names: Vec<String> = (0..contents.len()).map(|i| format!("f{i}")).collect();
    let files: Vec<_> = (0..contents.len())
        .map(|i| {
            (
                names[i].as_str(),
                std::slice::from_ref(&contents[i]),
                CompressionFormat::Deflate,
            )
        })
        .collect();
    write_format_1_repository(&repo, &files);
    let index = repo.join(".ddup-bak/chunks/index");
    let intact = fs::read(&index).unwrap();
    let storage = ChunkStorageLocal(repo.join(".ddup-bak/chunks"));
    assert_eq!(storage.list_chunk_hashes().unwrap().len(), contents.len());

    fs::write(&index, &intact[..intact.len() * 2 / 3]).unwrap();
    assert!(Repository::open(&repo, None, None).is_err());
    assert!(ChunkIndex::load_v1(&index).is_err());
    assert_eq!(
        Archive::open(repo.join(".ddup-bak/archives/old.ddup"))
            .unwrap()
            .version(),
        1
    );

    // The truncated Deflate stream still yields most records, but never all.
    let (_, salvaged) = ChunkIndex::salvage_v1(&index).unwrap();
    assert!(
        salvaged.len() > contents.len() / 2 && salvaged.len() < contents.len(),
        "salvaged {} of {}",
        salvaged.len(),
        contents.len()
    );

    let repository = Repository::rebuild(&repo, CHUNK_SIZE, 0, None, None, None).unwrap();
    assert_eq!(repository.unreadable_archives().unwrap(), ["old"]);
    assert_eq!(
        repository.clean(None).unwrap_err().kind(),
        std::io::ErrorKind::Unsupported
    );
    assert_eq!(storage.list_chunk_hashes().unwrap().len(), contents.len());

    fs::write(&index, &intact).unwrap();
    let repository = Repository::open(&repo, None, None).unwrap();
    let restored = repository.restore_archive("old", None, 2).unwrap();
    for (name, content) in names.iter().zip(&contents) {
        assert_eq!(&fs::read(restored.join(name)).unwrap(), content, "{name}");
    }
    assert!(repository.unreadable_archives().unwrap().is_empty());
    repository.clean(None).unwrap();
    assert_eq!(storage.list_chunk_hashes().unwrap().len(), contents.len());
}

#[cfg(unix)]
#[test]
fn a_running_old_version_holds_off_the_migration() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    let content = random(1000);
    write_format_1_repository(
        &repo,
        &[(
            "a",
            std::slice::from_ref(&content),
            CompressionFormat::Deflate,
        )],
    );

    // Old lock file layout: mode, presence flag and pid, each padded to eight bytes.
    let mut state = vec![0u8; 48];
    state[0] = 2;
    state[8] = 1;
    state[16..24].copy_from_slice(&u64::from(std::process::id()).to_le_bytes());
    let lock = repo.join(".ddup-bak/chunks/index.lock");
    fs::write(&lock, &state).unwrap();
    let blocked = Repository::open(&repo, None, None)
        .err()
        .expect("migration went ahead");
    assert_eq!(blocked.kind(), std::io::ErrorKind::WouldBlock);

    // A dead pid is a crash leftover and must not block.
    state[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
    fs::write(&lock, &state).unwrap();
    let repository = Repository::open(&repo, None, None).unwrap();
    assert_eq!(
        fs::read(
            repository
                .restore_archive("old", None, 2)
                .unwrap()
                .join("a")
        )
        .unwrap(),
        content
    );
}

#[test]
fn a_damaged_archive_does_not_block_the_migration_of_the_others() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    let content = random(1000);
    write_format_1_repository(
        &repo,
        &[(
            "a",
            std::slice::from_ref(&content),
            CompressionFormat::Deflate,
        )],
    );
    let archives = repo.join(".ddup-bak/archives");
    let broken = archives.join("broken.ddup");
    fs::copy(archives.join("old.ddup"), &broken).unwrap();
    File::options()
        .write(true)
        .open(&broken)
        .unwrap()
        .set_len(300)
        .unwrap();

    let repository = Repository::open(&repo, None, None).unwrap();
    assert_eq!(
        Archive::open(archives.join("old.ddup")).unwrap().version(),
        2
    );
    assert_eq!(
        Archive::open(&broken).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    let restored = repository.restore_archive("old", None, 2).unwrap();
    assert_eq!(fs::read(restored.join("a")).unwrap(), content);
    assert!(repository.get_archive("broken").is_err());
}

#[test]
fn hash_algorithm_is_chosen_at_init() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    let repository =
        Repository::new_with_hash(&repo, CHUNK_SIZE, 0, HashAlgorithm::Blake3, None).unwrap();
    let content = random(500);
    let source = dir.path().join("src");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("f"), &content).unwrap();
    let walker = ignore::WalkBuilder::new(&source)
        .standard_filters(false)
        .build();
    repository
        .create_archive("b", Some(walker), Some(&source), None, None, 2)
        .unwrap();

    let storage = ChunkStorageLocal(repo.join(".ddup-bak/chunks"));
    assert!(
        repo.join(".ddup-bak/chunks")
            .join(storage.path_from_chunk(&HashAlgorithm::Blake3.hash(&content)))
            .exists()
    );
    assert_eq!(
        Repository::open(&repo, None, None)
            .unwrap()
            .hash_algorithm(),
        HashAlgorithm::Blake3
    );
}

#[cfg(feature = "brotli")]
#[test]
fn brotli_chunks_roundtrip() {
    let fixture = Fixture::new();
    let text = b"brotli brotli brotli ".repeat(100);
    let source = fixture.source("src", &[("text", &text)]);
    let walker = ignore::WalkBuilder::new(&source)
        .standard_filters(false)
        .build();
    let compression: CompressionFormatCallback = Some(Arc::new(|_, _| CompressionFormat::Brotli));
    fixture
        .repository
        .create_archive("b", Some(walker), Some(&source), None, compression, 2)
        .unwrap();

    assert_eq!(
        fs::read(fixture.chunk_path(&text)).unwrap()[0],
        CompressionFormat::Brotli.encode()
    );
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["text"]);
}

#[test]
fn zstd_chunks_roundtrip() {
    let fixture = Fixture::new();
    let text = b"zstd zstd zstd ".repeat(100);
    let source = fixture.source("src", &[("text", &text)]);
    let walker = ignore::WalkBuilder::new(&source)
        .standard_filters(false)
        .build();
    let compression: CompressionFormatCallback = Some(Arc::new(|_, _| CompressionFormat::Zstd));
    fixture
        .repository
        .create_archive("b", Some(walker), Some(&source), None, compression, 2)
        .unwrap();

    assert_eq!(
        fs::read(fixture.chunk_path(&text)).unwrap()[0],
        CompressionFormat::Zstd.encode()
    );
    assert_same_files(&source, &fixture.restore("b").unwrap(), &["text"]);
}
