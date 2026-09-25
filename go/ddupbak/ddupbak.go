// Package ddupbak wraps libddupbak, a deduplicating backup repository.
//
// Build the C library with `cargo build --release` in the `c` directory and point cgo at it:
//
//	CGO_CFLAGS="-I/path/to/ddup-bak/c/include" CGO_LDFLAGS="-L/path/to/ddup-bak/target/release"
//
// Callbacks may run concurrently on library worker threads.
package ddupbak

/*
#cgo LDFLAGS: -lddupbak
#include <stdint.h>
#include <stdlib.h>
#include <libddupbak.h>
*/
import "C"

import (
	"encoding/binary"
	"errors"
	"runtime/cgo"
	"unsafe"
)

// CompressionFormat defines the compression algorithm used for files
type CompressionFormat uint8

const (
	CompressionNone    CompressionFormat = 0
	CompressionGzip    CompressionFormat = 1
	CompressionDeflate CompressionFormat = 2
	CompressionBrotli  CompressionFormat = 3
	CompressionZstd    CompressionFormat = 4
)

// HashAlgorithm is the chunk hash of a repository, fixed at creation.
type HashAlgorithm uint8

const (
	HashBlake2b256 HashAlgorithm = 0 // default
	HashBlake3     HashAlgorithm = 1
)

// EntryType defines the type of a filesystem entry
type EntryType uint8

const (
	EntryTypeFile      EntryType = 0
	EntryTypeDirectory EntryType = 1
	EntryTypeSymlink   EntryType = 2
)

// ChunkHash is the 256-bit hash identifying a chunk.
type ChunkHash [32]byte

// ID is the first 8 bytes of the hash, passed to DeletionProgressCallback as chunkID.
func (h ChunkHash) ID() uint64 {
	return binary.LittleEndian.Uint64(h[:8])
}

// ProgressCallback is a callback for tracking progress operations (chunking, archiving, restoring)
type ProgressCallback func(path string)

type (
	// ChunkingProgressCallback is a callback for tracking chunking progress
	ChunkingProgressCallback = ProgressCallback

	// ArchivingProgressCallback is a callback for tracking archiving progress
	ArchivingProgressCallback = ProgressCallback

	// RestoringProgressCallback is called with an entry's path before it is restored
	RestoringProgressCallback = ProgressCallback

	// RestoredProgressCallback is called with an entry's path after it is fully restored
	RestoredProgressCallback = ProgressCallback
)

// DeletionProgressCallback is a callback for tracking deletion progress
type DeletionProgressCallback func(chunkID uint64, deleted bool)

// CleaningProgressCallback is a callback for tracking cleaning progress
type CleaningProgressCallback = DeletionProgressCallback

// RebuildProgressCallback is called once for every chunk with its final reference count, zero
// included, after a rebuild has counted all archives.
type RebuildProgressCallback func(hash ChunkHash, references uint64)

// CompressionFormatCallback is a callback for determining the compression format
type CompressionFormatCallback func(path string) CompressionFormat

// CompressionCallback determines the compression format for a file
type CompressionCallback func(path string, size uint64) CompressionFormat

// RealSizeCallback determines the real size of a file before compression
type RealSizeCallback func(path string) uint64

type callbacks struct {
	progress    ProgressCallback
	archiving   ProgressCallback
	restored    ProgressCallback
	deletion    DeletionProgressCallback
	rebuild     RebuildProgressCallback
	compression CompressionFormatCallback
	archiveComp CompressionCallback
	realSize    RealSizeCallback
}

// userData stores a cgo.Handle to cb in C memory for use as user_data. The returned func frees both.
func userData(cb *callbacks) (C.CUserData, func()) {
	handle := cgo.NewHandle(cb)
	cell := (*C.uintptr_t)(C.malloc(C.size_t(unsafe.Sizeof(C.uintptr_t(0)))))
	*cell = C.uintptr_t(handle)

	return C.CUserData(unsafe.Pointer(cell)), func() {
		C.free(unsafe.Pointer(cell))
		handle.Delete()
	}
}

func userCallbacks(data unsafe.Pointer) *callbacks {
	return cgo.Handle(*(*C.uintptr_t)(data)).Value().(*callbacks)
}

func hashOf(hash *C.uint8_t) ChunkHash {
	return *(*ChunkHash)(unsafe.Pointer(hash))
}

//export goProgressCallback
func goProgressCallback(path *C.char, data unsafe.Pointer) {
	cbs := userCallbacks(data)
	if cbs.progress != nil || cbs.archiving != nil {
		p := C.GoString(path)
		if cbs.progress != nil {
			cbs.progress(p)
		}
		if cbs.archiving != nil {
			cbs.archiving(p)
		}
	}
}

//export goRestoredCallback
func goRestoredCallback(path *C.char, data unsafe.Pointer) {
	if cb := userCallbacks(data).restored; cb != nil {
		cb(C.GoString(path))
	}
}

//export goDeletionCallback
func goDeletionCallback(hash *C.uint8_t, deleted C.bool, data unsafe.Pointer) {
	if cb := userCallbacks(data).deletion; cb != nil {
		cb(hashOf(hash).ID(), bool(deleted))
	}
}

//export goRebuildCallback
func goRebuildCallback(hash *C.uint8_t, references C.uint64_t, data unsafe.Pointer) {
	if cb := userCallbacks(data).rebuild; cb != nil {
		cb(hashOf(hash), uint64(references))
	}
}

//export goCompressionCallback
func goCompressionCallback(path *C.char, size C.uint64_t, data unsafe.Pointer) C.CCompressionFormat {
	cbs := userCallbacks(data)
	if cbs.compression != nil {
		return C.CCompressionFormat(cbs.compression(C.GoString(path)))
	}
	if cbs.archiveComp != nil {
		return C.CCompressionFormat(cbs.archiveComp(C.GoString(path), uint64(size)))
	}
	return C.CCompressionFormat(CompressionDeflate)
}

//export goRealSizeCallback
func goRealSizeCallback(path *C.char, data unsafe.Pointer) C.uint64_t {
	if cb := userCallbacks(data).realSize; cb != nil {
		return C.uint64_t(cb(C.GoString(path)))
	}
	return 0
}

func lastError(fallback string) error {
	if message := C.GoString(C.last_error()); message != "" {
		return errors.New(message)
	}
	return errors.New(fallback)
}

func cString(value string) *C.char {
	return C.CString(value)
}

func optionalCString(value *string) *C.char {
	if value == nil || *value == "" {
		return nil
	}
	return C.CString(*value)
}

func freeCString(value *C.char) {
	if value != nil {
		C.free(unsafe.Pointer(value))
	}
}
