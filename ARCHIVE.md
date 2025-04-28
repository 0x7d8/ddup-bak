# ddup-bak archive format version 1

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

### entry_type

the entry type format is an enum describing what kind of entry an entry is.

#### variants

- **`0`**: File
- **`1`**: Directory
- **`2`**: Symlink

### type_compression_mode

encoded version of entry type + compression format + unix file permissions

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
| 8    | 1 (version) |

### entry

each archive file has an array of entries with can be files, symlinks or directories.
all entries have a few base properties that will always be available

`...varint(u32)          ` - Byte Length of Name String (UTF8)<br>
`...u8                   ` - Array of Name (file name only, no path) utf8 scalar values (as many as in the byte length)<br>
`   type_compression_mode` - Entry Type, Compression Format and File Permissions<br>
`...varint(u32)          ` - Unix User Id (File owner)<br>
`...varint(u32)          ` - Unix Group Id (File owner)<br>
`...varint(u64)          ` - Seconds since 1970-01-01 00:00:00 UTC of when the file was last modified (Unix Epoch)<br>

#### file_entry (0x0)

`...varint(u64)` - Byte Length of Uncompressed file content<br>
`...varint(u64)` - Byte Length of Compressed file content (**ONLY AVAILABLE IF `compression_format` IS NOT 0**)<br>
`...varint(u64)` - Byte Lenght of "Real" file size, this is mainly used by the dedup part of this repo<br>
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
