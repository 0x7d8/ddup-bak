# ddup-bak archive format version 2

## definitions

### varint

varints are used to efficiently store integer values with as few bytes as possible

#### segment

`   1 bit`  - control bit<br>
`...7 bit` - le byte data

when the control bit (128) is set, you must read the next byte of the file for the second part of the varint.
it is important that data stays le, so if first part is "1 0110111" and second part is "0 1111111", then the resulting integer
must be "01101111111111" (7167). varints are either u32 or u64, this is defined by the spec using varint(u32) or varint(u64)

### compression_format

the compression format is an enum describing what compression the content of an archived file uses.

#### variants

- **`0`**: No Compression
- **`1`**: Gzip Compression
- **`2`**: Deflate Compression
- **`3`**: Brotli Compression
- **`4`**: Zstd Compression

### entry_type

the entry type format is an enum describing what kind of entry an entry is.

#### variants

- **`0`**: File
- **`1`**: Directory
- **`2`**: Symlink

### type_compression_mode

encoded version of entry type + compression format + unix file mode

| Bit     | Desription                      |
| ------- | ------------------------------- |
| `1..2`  | LE Bytes for entry_type         |
| `2..6`  | LE Bytes for compression_format |
| `6..32` | LE Bytes for unix permissions   |

### signature

each archive file has an 8-byte signature at the beginning, this signature is made out of 2 parts.

68, 68, 85, 80, 66, 65, 75

| Byte | Value       |
| ---- | ----------- |
| 1    | 68 (D)      |
| 2    | 68 (D)      |
| 3    | 85 (U)      |
| 4    | 80 (P)      |
| 5    | 66 (B)      |
| 6    | 65 (A)      |
| 7    | 75 (K)      |
| 8    | 2 (version) |

### entry

each archive file has an array of entries with can be files, symlinks or directories.
all entries have a few base properties that will always be available

`...varint(u32)          ` - Byte Length of Name String (UTF8)<br>
`...u8                   ` - Array of Name (file name only, no path) utf8 scalar values (as many as in the byte length)<br>
`   type_compression_mode` - Entry Type, Compression Format and File Mode (Permissions)<br>
`...varint(u32)          ` - Unix User Id (File owner)<br>
`...varint(u32)          ` - Unix Group Id (File owner)<br>
`...varint(u64)          ` - Seconds since 1970-01-01 00:00:00 UTC of when the file was last modified (Unix Epoch)<br>

#### file_entry (0x0)

`...varint(u64)` - Byte Length of Uncompressed file content<br>
`...varint(u64)` - Byte Length of Compressed file content (**ONLY EXISTS IF `compression_format` IS NOT 0**)<br>
`...varint(u64)` - Byte Length of "Real" file size, for repository archives this is the size of the original file<br>
`...varint(u64)` - Byte Offset (signature included) at which to read the file content in the archive

#### directory_entry (0x1)

`...varint(u64)` - Entry (**!**) amount of top-level entries in the directory to read

#### symlink_entry (0x2)

`...varint(u64)` - Byte Length of Target String (UTF8)<br>
`...u8         ` - Array of utf8 scalar values (as many as in the target byte length)<br>
`    bool        ` - Boolean of whether the target is a directory or not (relevant for windows)

## format

a ddup-bak archive is structured in the following way:

`...u8     ` - Raw/Compressed File data<br>
`...entries` - Deflate Encoded Entries<br>
`    u64     ` - LE Entry Count (not Bytes)<br>
`    u64     ` - LE Byte offset at which to begin reading entries

an implementation is expected to read the last 16 bytes of an archive to determine how many entries to read
and at what offset to read them, implementations usually read entries upon opening an archive, since it does
not require reading file data

## repository archives

archives inside a repository (`.ddup-bak/archives/*.ddup`) do not store file data inline. the content of every
file entry is a list of 32-byte chunk hashes (compression_format 0), and the "real" size is the size of the
original file. the chunks themselves live in `.ddup-bak/chunks/<xx>/<yy>/<rest>.chunk`, named by the hex hash
of their uncompressed content. each chunk file starts with one compression_format byte followed by the data.

the hash is either BLAKE2b-256 (`0`, the default) or BLAKE3-256 (`1`), chosen when the repository is created
and fixed for its lifetime, since chunk files are named by it.

`.ddup-bak/chunks/index` caches reference counts and can always be rebuilt from the archives:

`    u8[8]      ` - `DDUPIDX4`<br>
`    u32        ` - LE average chunk size<br>
`    u32        ` - LE max chunk count per file (0 = unlimited)<br>
`    u8         ` - hash algorithm<br>
`    u64        ` - LE entry count<br>
`...entry      ` - `u8[32]` chunk hash followed by `varint(u64)` reference count<br>
`    u8[32]     ` - BLAKE3 hash of everything above, checked when the index is loaded

three older index formats are still read: `DDUPIDX3`, the same without the trailing hash; `DDUPIDX2`, a
deflate stream with the same fields minus the hash algorithm byte and always BLAKE3-256, and format 1, a
deflate stream keyed by index-assigned chunk ids.

deleting an archive moves it to `.ddup-bak/deleting/<name>.ddup` (`<name>.ddup.1` and so on while that
is taken, with no name at all if that would not fit), removes the chunks only it referenced, saves the
index and then removes it from there. an archive left in `deleting` means the index may still count it;
the next delete, clean or rebuild recounts those chunks from the archives that remain before going on,
and a delete does so without saving the index until its own chunks are freed. a backup meanwhile checks
that every chunk it reuses is there, so it needs no recount. a backup writes `.partial-<hash>` in
`archives` and moves it into place last; `clean` removes any left behind. the archive-name existence
check is made under the exclusive index lock, so a waiting creator cannot replace a just-published
archive with the same name. existing symlinks also reserve archive names.

CLI restores and restores into `.ddup-bak/archives-restored/<name>` take a lock in
`.ddup-bak/restore-locks/destination-<hash>` keyed by the canonical destination. the destination
must be a directory, not a symlink. each operation exclusively creates a private
`.ddup-bak-restore-<pid>-<counter>` directory inside it, on the same filesystem. restored data goes
into `new`; only after every entry has decoded successfully are old destination entries renamed
into `previous` and new entries moved into place. original entries are removed only after all
publication moves succeed. ownership is applied before final permissions, preserving setuid/setgid.

a failed move rolls back the entries already moved. if rollback also fails, the error names the
recovery directory; originals remain in the destination and/or `previous`, and that directory is
not removed. `.ddup-bak`, `.ddup-bak-restore`, and `.ddup-bak-restore-*` destination entries are
reserved and preserved, including recovery directories from interrupted operations. archives
containing those top-level names cannot replace destination contents through this operation.
inspect and recover any retained `previous` entries before manually removing their staging
directory. a process killed during publication can leave a mixture of old and new entries;
this rollback protocol is not a whole-directory atomic swap or a power-loss guarantee. other
writers, including restores from another repository, must not modify the same destination during
publication. the non-replacing `restore_archive_to` / `restore_entries_to` APIs never overwrite or
follow existing destination paths.

### changes from version 1

version 1 repository archives referenced chunks by index-assigned ids. opening such a repository rewrites its
archives and index into version 2 in place before anything else reads them.
entry names are validated on read and write: they must be non-empty, not `.` or `..`, and contain no `/` or NUL.
on windows `\` is rejected as well, since it separates paths there.
