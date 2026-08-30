use ddup_bak::{
    archive::{
        Archive, CompressionFormat, CompressionFormatCallback,
        entries::{Entry, EntryMode, FileEntry},
    },
    chunks::{
        ChunkIndex, HashAlgorithm,
        storage::{ChunkStorage, ChunkStorageLocal},
    },
    repository::Repository,
};
use std::{
    fs::{self, File},
    io::{Cursor, Read},
    path::{Path, PathBuf},
    sync::Arc,
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
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let repository = Repository::new(&root.join("repo"), CHUNK_SIZE, 0, None).unwrap();
        fs::create_dir_all(root.join("repo/.ddup-bak/chunks")).unwrap();
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
    let mut state = 0x9E3779B97F4A7C15u64;
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

/// The walk every version has used skips dot entries and obeys `.ignore` (and `.gitignore`
/// inside a git repository), so upgrading must not quietly start storing files that were never
/// in anyone's backups.
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

    // And a walker without the filters stores all of it.
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

/// A backslash is an ordinary filename byte on Unix, and directories that use it (systemd's
/// escaped device units, for one) have to back up and migrate like any other.
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
fn unchanged_files_are_not_chunked_again() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let fixture = Fixture::new();
    let (big, small) = (random(20_000), random(100));
    let source = fixture.source("src", &[("big", &big), ("small", &small)]);

    // The compression callback runs once per file that is actually read and chunked.
    let chunked = Arc::new(AtomicUsize::new(0));
    let backup = |name: &str| {
        let chunked = Arc::clone(&chunked);
        let compression: CompressionFormatCallback = Some(Arc::new(move |_, _| {
            chunked.fetch_add(1, Ordering::Relaxed);
            CompressionFormat::Deflate
        }));
        let walker = ignore::WalkBuilder::new(&source)
            .standard_filters(false)
            .build();
        fixture
            .repository
            .create_archive(name, Some(walker), Some(&source), None, compression, 4)
            .unwrap();
    };

    backup("first");
    assert_eq!(chunked.load(Ordering::Relaxed), 2);
    backup("second");
    assert_eq!(chunked.load(Ordering::Relaxed), 2);
    assert_same_files(
        &source,
        &fixture.restore("second").unwrap(),
        &["big", "small"],
    );

    // A rewrite of the same length is picked up: its ctime differs.
    let changed = small.iter().map(|b| !b).collect::<Vec<_>>();
    fs::write(source.join("small"), &changed).unwrap();
    backup("third");
    assert_eq!(chunked.load(Ordering::Relaxed), 3);
    assert_same_files(
        &source,
        &fixture.restore("third").unwrap(),
        &["big", "small"],
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

/// Writes a repository exactly as versions before archive format 2 did: BLAKE2b chunk names,
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

        // Older versions stored the id list with the file's own compression, so the entry
        // header carries a compression id next to the mode bits.
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

    // Archive format 1 differs from 2 only in what file bodies hold.
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
    c.reverse(); // `random` is deterministic; keep c's halves distinct from a
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
        3
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

    // Opening again is a no-op, and new backups dedup against the BLAKE2b-named chunks.
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

    // Rebuild recovers the algorithm from the chunks themselves.
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

/// A format 1 index is a Deflate stream of chunk records, so a truncated one still decodes up
/// to the damage. `rebuild` keeps that much and recovers every archive it covers, rather than
/// treating the whole repository as lost.
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

    // Opening will not guess at a damaged index: it says so and changes nothing.
    fs::write(&index, &intact[..intact.len() * 2 / 3]).unwrap();
    assert!(Repository::open(&repo, None, None).is_err());
    assert!(ChunkIndex::load_v1(&index).is_err());
    assert_eq!(
        Archive::open(repo.join(".ddup-bak/archives/old.ddup"))
            .unwrap()
            .version(),
        1
    );

    // Asked to recover, it keeps every record the Deflate stream still yields: most of them
    // here, and never all, so the one archive stays unaccounted for.
    let (_, salvaged) = ChunkIndex::salvage_v1(&index).unwrap();
    assert!(
        salvaged.len() > contents.len() / 2 && salvaged.len() < contents.len(),
        "salvaged {} of {}",
        salvaged.len(),
        contents.len()
    );

    // While an archive is unaccounted for, nothing may delete the chunks it might still need.
    let repository = Repository::rebuild(&repo, CHUNK_SIZE, 0, None, None, None).unwrap();
    assert_eq!(repository.unreadable_archives().unwrap(), ["old"]);
    assert_eq!(
        repository.clean(None).unwrap_err().kind(),
        std::io::ErrorKind::Unsupported
    );
    assert_eq!(storage.list_chunk_hashes().unwrap().len(), contents.len());

    // With the index back, the migration completes and everything returns.
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

/// Versions before archive format 2 lock the repository with their own scheme, which this one
/// cannot take part in. Migrating underneath such a process would let it write its own index
/// over the migrated one, so the migration has to wait for it instead.
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

    // The old lock file: a mode byte, a presence byte and a pid, each padded to eight bytes.
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

    // A pid that is not running is a leftover from a crash and must not block anything.
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

/// One unreadable archive used to fail the whole migration, which locked the reader out of
/// every healthy archive next to it.
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
