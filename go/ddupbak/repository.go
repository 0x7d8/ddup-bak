package ddupbak

/*
#include <stdlib.h>
#include "callbacks.h"
*/
import "C"

import (
	"errors"
	"runtime"
	"unsafe"
)

var errClosed = errors.New("ddupbak: repository is closed")

// Nil callbacks map to null so the library skips a cgo call per file or chunk.
func cProgressCallback(cb ...ProgressCallback) C.CProgressCallback {
	for _, cb := range cb {
		if cb != nil {
			return C.progressCallback()
		}
	}
	return nil
}

func cRestoredCallback(cb RestoredProgressCallback) C.CProgressCallback {
	if cb == nil {
		return nil
	}
	return C.restoredCallback()
}

func cDeletionCallback(cb DeletionProgressCallback) C.CDeletionProgressCallback {
	if cb == nil {
		return nil
	}
	return C.deletionCallback()
}

// Repository represents a ddupbak repository
type Repository struct {
	repo *C.struct_CRepository
}

func wrapRepository(repo *C.struct_CRepository, fallback string) (*Repository, error) {
	if repo == nil {
		return nil, lastError(fallback)
	}

	repository := &Repository{repo: repo}
	runtime.SetFinalizer(repository, (*Repository).Free)
	return repository, nil
}

// NewRepository creates a new repository with the specified parameters
func NewRepository(directory string, chunkSize, maxChunkCount uint) (*Repository, error) {
	return NewRepositoryWithHash(directory, chunkSize, maxChunkCount, HashBlake2b256)
}

// NewRepositoryWithHash is NewRepository with a chosen chunk hash algorithm.
func NewRepositoryWithHash(directory string, chunkSize, maxChunkCount uint, hash HashAlgorithm) (*Repository, error) {
	cDirectory := cString(directory)
	defer freeCString(cDirectory)

	return wrapRepository(
		C.new_repository_with_hash(cDirectory, C.uint(chunkSize), C.uint(maxChunkCount), uint32(hash)),
		"ddupbak: failed to create repository",
	)
}

// OpenRepository opens an existing repository.
// Repositories written by older versions are migrated in place.
func OpenRepository(directory string, chunksDirectory *string) (*Repository, error) {
	cDirectory, cChunks := cString(directory), optionalCString(chunksDirectory)
	defer freeCString(cDirectory)
	defer freeCString(cChunks)

	return wrapRepository(C.open_repository(cDirectory, cChunks), "ddupbak: failed to open repository")
}

// RebuildRepository rebuilds the chunk index from the archives and stored chunks.
func RebuildRepository(
	directory string,
	chunkSize, maxChunkCount uint,
	chunksDirectory *string,
	progress RebuildProgressCallback,
) (*Repository, error) {
	cDirectory, cChunks := cString(directory), optionalCString(chunksDirectory)
	defer freeCString(cDirectory)
	defer freeCString(cChunks)

	data, release := userData(&callbacks{rebuild: progress})
	defer release()

	var cProgress C.CRebuildProgressCallback
	if progress != nil {
		cProgress = C.rebuildCallback()
	}

	return wrapRepository(
		C.rebuild_repository(cDirectory, C.uint(chunkSize), C.uint(maxChunkCount), cChunks, cProgress, data),
		"ddupbak: failed to rebuild repository",
	)
}

// Free releases resources associated with the repository
func (r *Repository) Free() {
	if r.repo != nil {
		C.free_repository(r.repo)
		r.repo = nil
	}
}

// Close calls Free.
func (r *Repository) Close() { r.Free() }

// Save is a no-op kept for older callers; every operation persists the index itself.
func (r *Repository) Save() error {
	if r.repo == nil {
		return errClosed
	}
	if C.repository_save(r.repo) != 0 {
		return lastError("ddupbak: save failed")
	}
	return nil
}

// SetSaveOnDrop is kept for older callers and has no effect.
func (r *Repository) SetSaveOnDrop(saveOnDrop bool) error {
	if r.repo == nil {
		return errClosed
	}
	C.repository_set_save_on_drop(r.repo, C.bool(saveOnDrop))
	return nil
}

// Clean removes unused chunks from the repository
func (r *Repository) Clean(progressCallback CleaningProgressCallback) error {
	if r.repo == nil {
		return errClosed
	}

	data, release := userData(&callbacks{deletion: progressCallback})
	defer release()

	if C.repository_clean(r.repo, cDeletionCallback(progressCallback), data) != 0 {
		return lastError("ddupbak: clean failed")
	}
	return nil
}

// CreateArchive creates a new archive in the repository.
// An empty directory backs up the repository directory.
func (r *Repository) CreateArchive(
	name string,
	directory string,
	chunkingCallback ChunkingProgressCallback,
	archivingCallback ArchivingProgressCallback,
	compressionFormatCallback CompressionFormatCallback,
	threads uint,
) (*Archive, error) {
	if r.repo == nil {
		return nil, errClosed
	}

	cName, cDirectory := cString(name), optionalCString(&directory)
	defer freeCString(cName)
	defer freeCString(cDirectory)

	data, release := userData(&callbacks{
		progress:    chunkingCallback,
		archiving:   archivingCallback,
		compression: compressionFormatCallback,
	})
	defer release()

	var cCompression C.CCompressionFormatCallback
	if compressionFormatCallback != nil {
		cCompression = C.compressionCallback()
	}

	archive := C.repository_create_archive(
		r.repo, cName, cDirectory,
		cProgressCallback(chunkingCallback, archivingCallback), cCompression, data,
		C.uint(threads),
	)
	return wrapArchive(archive, "ddupbak: failed to create archive")
}

// ListArchives returns the list of archive names in the repository
func (r *Repository) ListArchives() ([]string, error) {
	if r.repo == nil {
		return nil, errClosed
	}

	var count C.uint
	names := C.repository_list_archives(r.repo, &count)
	if names == nil {
		return nil, lastError("ddupbak: failed to list archives")
	}
	defer C.free_string_array(names)

	result := make([]string, int(count))
	for i, name := range unsafe.Slice(names, int(count)) {
		result[i] = C.GoString(name)
	}
	return result, nil
}

// GetArchive opens an existing archive
func (r *Repository) GetArchive(archiveName string) (*Archive, error) {
	if r.repo == nil {
		return nil, errClosed
	}

	cName := cString(archiveName)
	defer freeCString(cName)

	return wrapArchive(C.repository_get_archive(r.repo, cName), "ddupbak: archive not found")
}

// RestoreArchive restores an archive into .ddup-bak/archives-restored/<name> in the repository,
// replacing a previous restore, and returns that path.
//
// Each entry is reported with its final path, once to progressCallback before it is created and
// once to restoredCallback after it is fully restored. A directory counts as restored after all
// its children, so children reach restoredCallback before their parent. Entries that fail never
// reach restoredCallback. Entries are moved from staging into the reported paths just before
// this returns.
func (r *Repository) RestoreArchive(
	archiveName string,
	progressCallback RestoringProgressCallback,
	restoredCallback RestoredProgressCallback,
	threads uint,
) (string, error) {
	if r.repo == nil {
		return "", errClosed
	}

	cName := cString(archiveName)
	defer freeCString(cName)

	data, release := userData(&callbacks{progress: progressCallback, restored: restoredCallback})
	defer release()

	path := C.repository_restore_archive(
		r.repo, cName, cProgressCallback(progressCallback), cRestoredCallback(restoredCallback),
		data, C.uint(threads),
	)
	if path == nil {
		return "", lastError("ddupbak: restore failed")
	}
	defer C.free_string(path)
	return C.GoString(path), nil
}

// RestoreArchiveTo restores an archive into destination, which is created if missing. Existing
// paths inside it are never overwritten. Callbacks work as in RestoreArchive, except entries are
// restored in place.
func (r *Repository) RestoreArchiveTo(
	archiveName string,
	destination string,
	progressCallback RestoringProgressCallback,
	restoredCallback RestoredProgressCallback,
	threads uint,
) error {
	if r.repo == nil {
		return errClosed
	}

	cName, cDestination := cString(archiveName), cString(destination)
	defer freeCString(cName)
	defer freeCString(cDestination)

	data, release := userData(&callbacks{progress: progressCallback, restored: restoredCallback})
	defer release()

	if C.repository_restore_archive_to(
		r.repo, cName, cDestination, cProgressCallback(progressCallback),
		cRestoredCallback(restoredCallback), data, C.uint(threads),
	) != 0 {
		return lastError("ddupbak: restore failed")
	}
	return nil
}

// DeleteArchive deletes an archive from the repository
func (r *Repository) DeleteArchive(
	archiveName string,
	progressCallback CleaningProgressCallback,
) error {
	if r.repo == nil {
		return errClosed
	}

	cName := cString(archiveName)
	defer freeCString(cName)

	data, release := userData(&callbacks{deletion: progressCallback})
	defer release()

	if C.repository_delete_archive(r.repo, cName, cDeletionCallback(progressCallback), data) != 0 {
		return lastError("ddupbak: delete failed")
	}
	return nil
}
