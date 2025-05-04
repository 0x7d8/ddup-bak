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
} CCompressionFormat;

typedef enum CEntryType {
  File = 0,
  Directory = 1,
  Symlink = 2,
} CEntryType;

typedef struct Option_ProgressCallbackFn Option_ProgressCallbackFn;

typedef struct CArchive {
  uint8_t _private[0];
} CArchive;

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

typedef struct CEntryReader {
  uint8_t _private[0];
} CEntryReader;

typedef struct CRepository {
  uint8_t _private[0];
} CRepository;

typedef void (*CDeletionProgressCallback)(uint64_t chunk_id, bool deleted);

typedef void (*CProgressCallback)(const char*);

typedef enum CCompressionFormat (*CCompressionFormatCallback)(const char*);

void free_string(char *ptr);

void free_string_array(char **ptr);

struct CArchive *new_archive(const char *path);

struct CArchive *open_archive(const char *path);

void free_archive(struct CArchive *archive);

int archive_add_directory(struct CArchive *archive,
                          const char *path,
                          struct Option_ProgressCallbackFn progress_callback);

struct CArchive *archive_set_compression_callback(struct CArchive *archive,
                                                  enum CCompressionFormat (*callback)(const char *path,
                                                                                      uint64_t size));

struct CArchive *archive_set_real_size_callback(struct CArchive *archive,
                                                uint64_t (*callback)(const char *path));

unsigned int archive_entries_count(const struct CArchive *archive);

const struct CEntry **archive_entries(const struct CArchive *archive);

struct CEntry *archive_find_entry(const struct CArchive *archive, const char *path);

enum CEntryType get_entry_type(const struct CEntry *entry);

const struct CEntryCommon *entry_get_common(const struct CEntry *entry);

const char *entry_name(const struct CEntry *entry);

void free_entry(struct CEntry *entry);

const struct CFileEntry *entry_as_file(const struct CEntry *entry);

const struct CDirectoryEntry *entry_as_directory(const struct CEntry *entry);

const struct CSymlinkEntry *entry_as_symlink(const struct CEntry *entry);

struct CEntryReader *repository_create_entry_reader(struct CRepository *repo,
                                                    const struct CFileEntry *entry);

int entry_reader_read(struct CEntryReader *reader, char *buffer, uintptr_t buffer_size);

void free_entry_reader(struct CEntryReader *reader);

struct CRepository *new_repository(const char *directory,
                                   unsigned int chunk_size,
                                   unsigned int max_chunk_count,
                                   const char *const *ignored_files);

struct CRepository *open_repository(const char *directory, const char *chunks_directory);

void free_repository(struct CRepository *repo);

int repository_save(struct CRepository *repo);

struct CRepository *repository_set_save_on_drop(struct CRepository *repo, bool save_on_drop);

struct CRepository *repository_add_ignored_file(struct CRepository *repo, const char *file);

struct CRepository *repository_remove_ignored_file(struct CRepository *repo, const char *file);

bool repository_is_ignored(struct CRepository *repo, const char *file);

char **repository_get_ignored_files(struct CRepository *repo);

int repository_clean(struct CRepository *repo, CDeletionProgressCallback progress_callback);

struct CArchive *repository_create_archive(struct CRepository *repo,
                                           const char *name,
                                           const char *directory,
                                           CProgressCallback progress_chunking,
                                           CProgressCallback progress_archiving,
                                           CCompressionFormatCallback compression_callback,
                                           unsigned int threads);

char **repository_list_archives(struct CRepository *repo, unsigned int *count);

struct CArchive *repository_get_archive(struct CRepository *repo, const char *archive_name);

char *repository_restore_archive(struct CRepository *repo,
                                 const char *archive_name,
                                 CProgressCallback progress_callback,
                                 unsigned int threads);

int repository_delete_archive(struct CRepository *repo,
                              const char *archive_name,
                              CDeletionProgressCallback progress_callback);

#endif /* LIB_DDUPBAK_H */
