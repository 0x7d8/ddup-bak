#ifndef LIB_DDUPBAK_H
#define LIB_DDUPBAK_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef enum CCompressionFormat {
  None = 0,
  Gzip = 1,
  Deflate = 2,
  Brotli = 3,
  Zstd = 4,
} CCompressionFormat;

typedef enum CEntryType {
  File = 0,
  Directory = 1,
  Symlink = 2,
} CEntryType;

/**
 * Chunk hash algorithm of a repository; fixed at creation.
 */
typedef enum CHashAlgorithm {
  Blake2b256 = 0,
  Blake3 = 1,
} CHashAlgorithm;

/**
 * Opaque archive handle.
 */
typedef struct CArchive {
  uint8_t _private[0];
} CArchive;

/**
 * Opaque pointer handed back to every callback unchanged. Callbacks may run on several threads
 * at once.
 */
typedef void *CUserData;

typedef void (*CProgressCallback)(const char *path, CUserData user_data);

typedef enum CCompressionFormat (*CArchiveCompressionCallback)(const char *path,
                                                               uint64_t size,
                                                               CUserData user_data);

typedef uint64_t (*CRealSizeCallback)(const char *path, CUserData user_data);

/**
 * Tagged pointer to a `CFileEntry`, `CDirectoryEntry` or `CSymlinkEntry`.
 */
typedef struct CEntry {
  enum CEntryType entry_type;
  void *entry;
} CEntry;

typedef struct CEntryCommon {
  char *name;
  uint32_t mode;
  uint32_t uid;
  uint32_t gid;
  uint64_t mtime;
  enum CEntryType entry_type;
} CEntryCommon;

typedef struct CFileEntry {
  struct CEntryCommon common;
  enum CCompressionFormat compression;
  uint64_t size;
  uint64_t size_real;
  uint64_t size_compressed;
  void *file;
  uint64_t offset;
} CFileEntry;

typedef struct CDirectoryEntry {
  struct CEntryCommon common;
  unsigned int entries_count;
  struct CEntry **entries;
} CDirectoryEntry;

typedef struct CSymlinkEntry {
  struct CEntryCommon common;
  char *target;
  bool target_dir;
} CSymlinkEntry;

/**
 * Opaque streaming reader over a repository file entry. Holds a shared repository lock until
 * freed: `repository_clean` and `repository_delete_archive` fail while one is open.
 */
typedef struct CEntryReader {
  uint8_t _private[0];
} CEntryReader;

/**
 * Opaque repository handle.
 */
typedef struct CRepository {
  uint8_t _private[0];
} CRepository;

typedef void (*CRebuildProgressCallback)(const uint8_t *hash,
                                         uint64_t references,
                                         CUserData user_data);

typedef void (*CDeletionProgressCallback)(const uint8_t *hash, bool deleted, CUserData user_data);

typedef enum CCompressionFormat (*CCompressionFormatCallback)(const char *path,
                                                              uint64_t size,
                                                              CUserData user_data);

/**
 * Message of the last error that happened on the calling thread. Valid until the next failing
 * call on the same thread.
 */
const char *last_error(void);

void free_string(char *ptr);

/**
 * Frees a null-terminated array of strings returned by this library.
 */
void free_string_array(char **ptr);

struct CArchive *new_archive(const char *path);

struct CArchive *open_archive(const char *path);

void free_archive(struct CArchive *archive);

int archive_add_directory(struct CArchive *archive,
                          const char *path,
                          CProgressCallback progress,
                          CUserData user_data);

void archive_set_compression_callback(struct CArchive *archive,
                                      CArchiveCompressionCallback callback,
                                      CUserData user_data);

void archive_set_real_size_callback(struct CArchive *archive,
                                    CRealSizeCallback callback,
                                    CUserData user_data);

unsigned int archive_entries_count(struct CArchive *archive);

/**
 * Top-level entries of the archive, free with `free_entry_array`.
 */
struct CEntry **archive_entries(struct CArchive *archive);

/**
 * Looks up an entry by path inside the archive, free with `free_entry`.
 */
struct CEntry *archive_find_entry(struct CArchive *archive, const char *path);

enum CEntryType get_entry_type(const struct CEntry *entry);

const struct CEntryCommon *entry_get_common(const struct CEntry *entry);

const char *entry_name(const struct CEntry *entry);

const struct CFileEntry *entry_as_file(const struct CEntry *entry);

const struct CDirectoryEntry *entry_as_directory(const struct CEntry *entry);

const struct CSymlinkEntry *entry_as_symlink(const struct CEntry *entry);

/**
 * Frees an entry and, for directories, all of its children.
 */
void free_entry(struct CEntry *entry);

/**
 * Frees an entry array returned by `archive_entries` together with its entries.
 */
void free_entry_array(struct CEntry **entries, unsigned int count);

struct CEntryReader *repository_create_entry_reader(struct CRepository *repo,
                                                    const struct CFileEntry *entry);

/**
 * Reads up to `buffer_size` bytes. Returns the byte count, 0 at end of file, -1 on error.
 */
int entry_reader_read(struct CEntryReader *reader, char *buffer, uintptr_t buffer_size);

void free_entry_reader(struct CEntryReader *reader);

struct CRepository *new_repository(const char *directory,
                                   unsigned int chunk_size,
                                   unsigned int max_chunk_count);

struct CRepository *new_repository_with_hash(const char *directory,
                                             unsigned int chunk_size,
                                             unsigned int max_chunk_count,
                                             enum CHashAlgorithm hash_algorithm);

/**
 * Kept for callers of older versions; the index is persisted by every operation.
 */
int repository_save(struct CRepository *repo);

/**
 * Kept for callers of older versions; has no effect.
 */
struct CRepository *repository_set_save_on_drop(struct CRepository *repo, bool _save_on_drop);

struct CRepository *open_repository(const char *directory, const char *chunks_directory);

struct CRepository *rebuild_repository(const char *directory,
                                       unsigned int chunk_size,
                                       unsigned int max_chunk_count,
                                       const char *chunks_directory,
                                       CRebuildProgressCallback progress_callback,
                                       CUserData user_data);

void free_repository(struct CRepository *repo);

int repository_clean(struct CRepository *repo,
                     CDeletionProgressCallback progress_callback,
                     CUserData user_data);

/**
 * Backs up `directory` (or the repository directory when null) into a new archive.
 */
struct CArchive *repository_create_archive(struct CRepository *repo,
                                           const char *name,
                                           const char *directory,
                                           CProgressCallback progress_callback,
                                           CCompressionFormatCallback compression_callback,
                                           CUserData user_data,
                                           unsigned int threads);

/**
 * Null-terminated array of archive names, free with `free_string_array`.
 */
char **repository_list_archives(struct CRepository *repo, unsigned int *count);

struct CArchive *repository_get_archive(struct CRepository *repo, const char *archive_name);

/**
 * Restores an archive into the repository's `archives-restored` directory and returns that
 * path, free with `free_string`. Null on error.
 */
char *repository_restore_archive(struct CRepository *repo,
                                 const char *archive_name,
                                 CProgressCallback progress_callback,
                                 CUserData user_data,
                                 unsigned int threads);

/**
 * Restores an archive into `destination`, which is created if missing.
 */
int repository_restore_archive_to(struct CRepository *repo,
                                  const char *archive_name,
                                  const char *destination,
                                  CProgressCallback progress_callback,
                                  CUserData user_data,
                                  unsigned int threads);

int repository_delete_archive(struct CRepository *repo,
                              const char *archive_name,
                              CDeletionProgressCallback progress_callback,
                              CUserData user_data);

#endif  /* LIB_DDUPBAK_H */
