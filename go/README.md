# ddupbak Go bindings

Go bindings for `libddupbak`.

## Build

```bash
cd /path/to/ddup-bak/c && cargo build --release
export CGO_CFLAGS="-I/path/to/ddup-bak/c/include"
export CGO_LDFLAGS="-L/path/to/ddup-bak/target/release"
export LD_LIBRARY_PATH="/path/to/ddup-bak/target/release"   # DYLD_LIBRARY_PATH on macOS
```

## Usage

```go
repo, err := ddupbak.OpenRepository("/srv/server", nil) // older repositories are migrated in place
if err != nil {
    log.Fatal(err)
}
defer repo.Free()

_, err = repo.CreateArchive("nightly", "/srv/server/world", func(path string) {
    log.Println("chunking", path)
}, nil, nil, 8) // nil compression callback = deflate

path, err := repo.RestoreArchive("nightly", nil, nil, 8) // into .ddup-bak/archives-restored/nightly
err = repo.RestoreArchiveTo("nightly", "/srv/restore", nil, nil, 8)

archive, err := repo.GetArchive("nightly")
defer archive.Free()
entries, _ := archive.Entries()
for _, entry := range entries {
    if entry.Type() == ddupbak.EntryTypeFile {
        reader, _ := repo.NewEntryReader(entry)
        io.Copy(os.Stdout, reader)
        reader.Close()
    }
}
```

New repositories use BLAKE2b chunk names and deflate unless asked otherwise:
`NewRepositoryWithHash(dir, size, count, ddupbak.HashBlake3)` and `CompressionZstd` from a
compression callback are faster. `CompressionBrotli` needs the library built with the `brotli`
feature, which is on by default.

Callbacks may be called concurrently from library worker threads. Entries returned by `Archive.Entries`
and `DirectoryEntry.Entries` live until the archive is freed; entries from `Archive.FindEntry` must be
freed individually. Errors carry the message of the underlying I/O error.
