package ddupbak

/*
#include <stdlib.h>
#include <stdint.h>
#include <libddupbak.h>

// Forward declarations for callback handling
extern void goProgressChunkingCallback(char* path);
extern void goProgressArchivingCallback(char* path);
extern void goProgressRestoringCallback(char* path);
extern void goProgressCleaningCallback(uint64_t chunkID, _Bool deleted);
extern CCompressionFormat goCompressionFormatCallback(char* path);

// Define proper function pointers for callbacks
static CProgressCallback getChunkingCallback() {
    return (CProgressCallback)goProgressChunkingCallback;
}

static CProgressCallback getArchivingCallback() {
    return (CProgressCallback)goProgressArchivingCallback;
}

static CProgressCallback getRestoringCallback() {
    return (CProgressCallback)goProgressRestoringCallback;
}

static CDeletionProgressCallback getCleaningCallback() {
    return (CDeletionProgressCallback)goProgressCleaningCallback;
}

static CCompressionFormatCallback getCompressionFormatCallback() {
	return (CCompressionFormatCallback)goCompressionFormatCallback;
}
*/
import "C"
import (
	"errors"
	"runtime"
	"sync"
	"unsafe"
)

// Repository represents a ddupbak repository
type Repository struct {
	repo *C.struct_CRepository
}

// ProgressCallback is a callback for tracking progress operations (chunking, archiving, restoring)
type ProgressCallback func(path string)

// ChunkingProgressCallback is a callback for tracking chunking progress
type ChunkingProgressCallback = ProgressCallback

// ArchivingProgressCallback is a callback for tracking archiving progress
type ArchivingProgressCallback = ProgressCallback

// RestoringProgressCallback is a callback for tracking restoring progress
type RestoringProgressCallback = ProgressCallback

// DeletionProgressCallback is a callback for tracking deletion progress
type DeletionProgressCallback func(chunkID uint64, deleted bool)

// CleaningProgressCallback is a callback for tracking cleaning progress
type CleaningProgressCallback = DeletionProgressCallback

// CompressionFormatCallback is a callback for determining the compression format
type CompressionFormatCallback func(path string) CompressionFormat

// Callback registry maps
var (
	activeCallbacks     = make(map[string]interface{})
	activeCallbacksLock sync.Mutex
)

//export goProgressChunkingCallback
func goProgressChunkingCallback(path *C.char) {
	pathStr := C.GoString(path)
	activeCallbacksLock.Lock()
	defer activeCallbacksLock.Unlock()

	if cb, ok := activeCallbacks["chunking"]; ok {
		if callback, ok := cb.(ChunkingProgressCallback); ok {
			callback(pathStr)
		}
	}
}

//export goProgressArchivingCallback
func goProgressArchivingCallback(path *C.char) {
	pathStr := C.GoString(path)
	activeCallbacksLock.Lock()
	defer activeCallbacksLock.Unlock()

	if cb, ok := activeCallbacks["archiving"]; ok {
		if callback, ok := cb.(ArchivingProgressCallback); ok {
			callback(pathStr)
		}
	}
}

//export goProgressRestoringCallback
func goProgressRestoringCallback(path *C.char) {
	pathStr := C.GoString(path)
	activeCallbacksLock.Lock()
	defer activeCallbacksLock.Unlock()

	if cb, ok := activeCallbacks["restoring"]; ok {
		if callback, ok := cb.(RestoringProgressCallback); ok {
			callback(pathStr)
		}
	}
}

//export goProgressCleaningCallback
func goProgressCleaningCallback(chunkID C.uint64_t, deleted C._Bool) {
	activeCallbacksLock.Lock()
	defer activeCallbacksLock.Unlock()

	if cb, ok := activeCallbacks["cleaning"]; ok {
		if callback, ok := cb.(CleaningProgressCallback); ok {
			callback(uint64(chunkID), bool(deleted))
		}
	}
}

//export goCompressionFormatCallback
func goCompressionFormatCallback(path *C.char) C.CCompressionFormat {
	pathStr := C.GoString(path)
	activeCallbacksLock.Lock()
	defer activeCallbacksLock.Unlock()
	if cb, ok := activeCallbacks["compression"]; ok {
		if callback, ok := cb.(CompressionFormatCallback); ok {
			format := callback(pathStr)
			switch format {
			case CompressionNone:
				return 0
			case CompressionGzip:
				return 1
			case CompressionDeflate:
				return 2
			case CompressionBrotli:
				return 3
			}
		}
	}

	return 0
}

// NewRepository creates a new repository with the specified parameters
func NewRepository(directory string, chunkSize uint, maxChunkCount uint) (*Repository, error) {
	cDirectory := C.CString(directory)
	defer C.free(unsafe.Pointer(cDirectory))

	repo := C.new_repository(
		cDirectory,
		C.uint(chunkSize),
		C.uint(maxChunkCount),
	)

	if repo == nil {
		return nil, errors.New("failed to create repository")
	}

	repository := &Repository{repo: repo}
	runtime.SetFinalizer(repository, (*Repository).Free)

	return repository, nil
}

// OpenRepository opens an existing repository
func OpenRepository(directory string, chunksDirectory *string) (*Repository, error) {
	cDirectory := C.CString(directory)
	defer C.free(unsafe.Pointer(cDirectory))

	var cChunksDirectory *C.char
	if chunksDirectory != nil {
		cChunksDirectory = C.CString(*chunksDirectory)
		defer C.free(unsafe.Pointer(cChunksDirectory))
	}

	repo := C.open_repository(cDirectory, cChunksDirectory)
	if repo == nil {
		return nil, errors.New("failed to open repository")
	}

	repository := &Repository{repo: repo}
	runtime.SetFinalizer(repository, (*Repository).Free)

	return repository, nil
}

// Free releases resources associated with the repository
func (r *Repository) Free() {
	if r.repo != nil {
		activeCallbacksLock.Lock()
		delete(activeCallbacks, "chunking")
		delete(activeCallbacks, "archiving")
		delete(activeCallbacks, "restoring")
		delete(activeCallbacks, "cleaning")
		activeCallbacksLock.Unlock()

		C.free_repository(r.repo)
		r.repo = nil
	}
}

// Save persists the repository metadata to disk
func (r *Repository) Save() error {
	if r.repo == nil {
		return errors.New("repository is closed")
	}

	ret := C.repository_save(r.repo)
	return cErrorToGoError(ret)
}

// SetSaveOnDrop configures whether to save metadata on repository drop
func (r *Repository) SetSaveOnDrop(saveOnDrop bool) error {
	if r.repo == nil {
		return errors.New("repository is closed")
	}

	C.repository_set_save_on_drop(r.repo, C._Bool(saveOnDrop))
	return nil
}

// Clean removes unused chunks from the repository
func (r *Repository) Clean(progressCallback CleaningProgressCallback) error {
	if r.repo == nil {
		return errors.New("repository is closed")
	}

	var cCallback C.CDeletionProgressCallback
	if progressCallback != nil {
		activeCallbacksLock.Lock()
		activeCallbacks["cleaning"] = progressCallback
		activeCallbacksLock.Unlock()

		cCallback = C.getCleaningCallback()
	}

	ret := C.repository_clean(r.repo, cCallback)
	return cErrorToGoError(ret)
}

// CreateArchive creates a new archive in the repository
func (r *Repository) CreateArchive(
	name string,
	directory string,
	chunkingCallback ChunkingProgressCallback,
	archivingCallback ArchivingProgressCallback,
	compressionFormatCallback CompressionFormatCallback,
	threads uint,
) (*Archive, error) {
	if r.repo == nil {
		return nil, errors.New("repository is closed")
	}

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	cDirectory := C.CString(directory)
	defer C.free(unsafe.Pointer(cDirectory))

	var cChunkingCallback C.CProgressCallback
	if chunkingCallback != nil {
		activeCallbacksLock.Lock()
		activeCallbacks["chunking"] = chunkingCallback
		activeCallbacksLock.Unlock()

		cChunkingCallback = C.getChunkingCallback()
	}

	var cArchivingCallback C.CProgressCallback
	if archivingCallback != nil {
		activeCallbacksLock.Lock()
		activeCallbacks["archiving"] = archivingCallback
		activeCallbacksLock.Unlock()

		cArchivingCallback = C.getArchivingCallback()
	}

	var cCompressionFormatCallback C.CCompressionFormatCallback
	if compressionFormatCallback != nil {
		activeCallbacksLock.Lock()
		activeCallbacks["compression"] = compressionFormatCallback
		activeCallbacksLock.Unlock()
		cCompressionFormatCallback = C.getCompressionFormatCallback()
	}

	cArchive := C.repository_create_archive(
		r.repo,
		cName,
		cDirectory,
		cChunkingCallback,
		cArchivingCallback,
		cCompressionFormatCallback,
		C.uint(threads),
	)

	if cArchive == nil {
		return nil, errors.New("failed to create archive")
	}

	archive := &Archive{archive: cArchive}
	runtime.SetFinalizer(archive, (*Archive).Free)

	return archive, nil
}

// ListArchives returns the list of archive names in the repository
func (r *Repository) ListArchives() ([]string, error) {
	if r.repo == nil {
		return nil, errors.New("repository is closed")
	}

	var count C.uint
	cArchives := C.repository_list_archives(r.repo, &count)
	if cArchives == nil {
		return []string{}, nil
	}

	result := cStringsToGoStrings(cArchives, count)

	C.free_string_array(cArchives)

	return result, nil
}

// GetArchive opens an existing archive
func (r *Repository) GetArchive(archiveName string) (*Archive, error) {
	if r.repo == nil {
		return nil, errors.New("repository is closed")
	}

	cArchiveName := C.CString(archiveName)
	defer C.free(unsafe.Pointer(cArchiveName))

	cArchive := C.repository_get_archive(r.repo, cArchiveName)
	if cArchive == nil {
		return nil, errors.New("failed to open archive")
	}

	archive := &Archive{archive: cArchive}
	runtime.SetFinalizer(archive, (*Archive).Free)
	return archive, nil
}

// RestoreArchive restores an archive to a directory
func (r *Repository) RestoreArchive(
	archiveName string,
	progressCallback RestoringProgressCallback,
	threads uint,
) (string, error) {
	if r.repo == nil {
		return "", errors.New("repository is closed")
	}

	cArchiveName := C.CString(archiveName)
	defer C.free(unsafe.Pointer(cArchiveName))

	var cCallback C.CProgressCallback
	if progressCallback != nil {
		activeCallbacksLock.Lock()
		activeCallbacks["restoring"] = progressCallback
		activeCallbacksLock.Unlock()

		cCallback = C.getRestoringCallback()
	}

	cRestorePath := C.repository_restore_archive(
		r.repo,
		cArchiveName,
		cCallback,
		C.uint(threads),
	)

	if cRestorePath == nil {
		return "", errors.New("failed to restore archive")
	}

	restorePath := C.GoString(cRestorePath)
	C.free_string(cRestorePath)

	return restorePath, nil
}

// DeleteArchive deletes an archive from the repository
func (r *Repository) DeleteArchive(
	archiveName string,
	progressCallback CleaningProgressCallback,
) error {
	if r.repo == nil {
		return errors.New("repository is closed")
	}

	cArchiveName := C.CString(archiveName)
	defer C.free(unsafe.Pointer(cArchiveName))

	var cCallback C.CDeletionProgressCallback
	if progressCallback != nil {
		activeCallbacksLock.Lock()
		activeCallbacks["cleaning"] = progressCallback
		activeCallbacksLock.Unlock()

		cCallback = C.getCleaningCallback()
	}

	ret := C.repository_delete_archive(r.repo, cArchiveName, cCallback)
	return cErrorToGoError(ret)
}
