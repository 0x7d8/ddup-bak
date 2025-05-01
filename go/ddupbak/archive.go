package ddupbak

/*
#include <stdlib.h>
#include <stdint.h>
#include "../../c/include/libddupbak.h"

extern enum CCompressionFormat goCompressionCallback(char* path, uint64_t size);
extern uint64_t goRealSizeCallback(char* path);

// Define proper function pointers for callbacks
static enum CCompressionFormat (*getCompressionCallback(void))(const char*, uint64_t) {
    return (enum CCompressionFormat(*)(const char*, uint64_t))goCompressionCallback;
}

static uint64_t (*getRealSizeCallback(void))(const char*) {
    return (uint64_t(*)(const char*))goRealSizeCallback;
}
*/
import "C"
import (
	"errors"
	"runtime"
	"sync"
	"unsafe"
)

// Archive represents a ddupbak archive
type Archive struct {
	archive *C.struct_CArchive
}

// CompressionCallback determines the compression format for a file
type CompressionCallback func(path string, size uint64) CompressionFormat

// RealSizeCallback determines the real size of a file before compression
type RealSizeCallback func(path string) uint64

var (
	activeCompressionCallback CompressionCallback
	activeRealSizeCallback    RealSizeCallback
	archiveCallbacksLock      sync.Mutex
)

//export goCompressionCallback
func goCompressionCallback(path *C.char, size C.uint64_t) C.enum_CCompressionFormat {
	archiveCallbacksLock.Lock()
	defer archiveCallbacksLock.Unlock()

	if activeCompressionCallback != nil {
		pathStr := C.GoString(path)
		format := activeCompressionCallback(pathStr, uint64(size))
		return C.enum_CCompressionFormat(format)
	}

	return C.enum_CCompressionFormat(0)
}

//export goRealSizeCallback
func goRealSizeCallback(path *C.char) C.uint64_t {
	archiveCallbacksLock.Lock()
	defer archiveCallbacksLock.Unlock()

	if activeRealSizeCallback != nil {
		pathStr := C.GoString(path)
		size := activeRealSizeCallback(pathStr)
		return C.uint64_t(size)
	}

	return 0
}

// NewArchive creates a new empty archive
func NewArchive(path string) (*Archive, error) {
	if path == "" {
		return nil, errors.New("path cannot be empty")
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	archive := C.new_archive(cPath)
	if archive == nil {
		return nil, errors.New("failed to create archive")
	}

	result := &Archive{archive: archive}
	runtime.SetFinalizer(result, (*Archive).Free)

	return result, nil
}

// OpenArchive opens an existing archive
func OpenArchive(path string) (*Archive, error) {
	if path == "" {
		return nil, errors.New("path cannot be empty")
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	archive := C.open_archive(cPath)
	if archive == nil {
		return nil, errors.New("failed to open archive")
	}

	result := &Archive{archive: archive}
	runtime.SetFinalizer(result, (*Archive).Free)

	return result, nil
}

// Free releases resources associated with the archive
func (a *Archive) Free() {
	if a.archive != nil {
		archiveCallbacksLock.Lock()
		activeCompressionCallback = nil
		activeRealSizeCallback = nil
		archiveCallbacksLock.Unlock()

		C.free_archive(a.archive)
		a.archive = nil
	}
}

// SetCompressionCallback sets a callback to determine compression format for files
func (a *Archive) SetCompressionCallback(callback CompressionCallback) error {
	if a.archive == nil {
		return errors.New("archive is closed")
	}

	if callback == nil {
		return errors.New("callback cannot be nil")
	}

	archiveCallbacksLock.Lock()
	activeCompressionCallback = callback
	archiveCallbacksLock.Unlock()

	cCallback := C.getCompressionCallback()
	C.archive_set_compression_callback(a.archive, cCallback)

	return nil
}

// SetRealSizeCallback sets a callback to determine the real size of files
func (a *Archive) SetRealSizeCallback(callback RealSizeCallback) error {
	if a.archive == nil {
		return errors.New("archive is closed")
	}

	if callback == nil {
		return errors.New("callback cannot be nil")
	}

	archiveCallbacksLock.Lock()
	activeRealSizeCallback = callback
	archiveCallbacksLock.Unlock()

	cCallback := C.getRealSizeCallback()
	C.archive_set_real_size_callback(a.archive, cCallback)

	return nil
}

// EntriesCount returns the number of entries in the archive
func (a *Archive) EntriesCount() (uint, error) {
	if a.archive == nil {
		return 0, errors.New("archive is closed")
	}

	count := C.archive_entries_count(a.archive)
	return uint(count), nil
}

// Entries returns all entries in the archive
func (a *Archive) Entries() ([]*Entry, error) {
	if a.archive == nil {
		return nil, errors.New("archive is closed")
	}

	count, err := a.EntriesCount()
	if err != nil {
		return nil, err
	}

	entries := make([]*Entry, count)
	for i := uint(0); i < count; i++ {
		entry, err := a.GetEntry(i)
		if err != nil {
			return nil, err
		}
		entries[i] = entry
	}

	return entries, nil
}

// GetEntry returns an entry by index
func (a *Archive) GetEntry(index uint) (*Entry, error) {
	if a.archive == nil {
		return nil, errors.New("archive is closed")
	}

	cEntry := C.archive_get_entry(a.archive, C.uint(index))
	if cEntry == nil {
		return nil, errors.New("entry not found")
	}

	entry := &Entry{entry: cEntry}
	runtime.SetFinalizer(entry, (*Entry).Free)

	return entry, nil
}

// FindEntry finds an entry by path
func (a *Archive) FindEntry(path string) (*Entry, error) {
	if a.archive == nil {
		return nil, errors.New("archive is closed")
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	cEntry := C.archive_find_entry(a.archive, cPath)
	if cEntry == nil {
		return nil, errors.New("entry not found")
	}

	entry := &Entry{entry: cEntry}
	runtime.SetFinalizer(entry, (*Entry).Free)

	return entry, nil
}
