// Package ddupbak wraps libddupbak, a deduplicating backup repository.
//
// Build the C library with `cargo build --release` in the `c` directory and point cgo at it:
//
//	CGO_CFLAGS="-I/path/to/ddup-bak/c/include" CGO_LDFLAGS="-L/path/to/ddup-bak/target/release"
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

// CompressionFormat defines the compression algorithm used for files and chunks.
type CompressionFormat uint8

const (
	CompressionNone    CompressionFormat = 0
	CompressionGzip    CompressionFormat = 1
	CompressionDeflate CompressionFormat = 2
	CompressionBrotli  CompressionFormat = 3 // needs the library built with the brotli feature (default)
	CompressionZstd    CompressionFormat = 4
)

// HashAlgorithm identifies chunks by content; fixed when a repository is created.
type HashAlgorithm uint8

const (
	HashBlake2b256 HashAlgorithm = 0 // the default, used by every version
	HashBlake3     HashAlgorithm = 1
)

// EntryType defines the type of a filesystem entry.
type EntryType uint8

const (
	EntryTypeFile      EntryType = 0
	EntryTypeDirectory EntryType = 1
	EntryTypeSymlink   EntryType = 2
)

// ChunkHash is the 256-bit hash identifying a chunk (see HashAlgorithm).
type ChunkHash [32]byte

// ID is the chunk id passed to DeletionProgressCallback: the first 8 bytes of the hash.
func (h ChunkHash) ID() uint64 {
	return binary.LittleEndian.Uint64(h[:8])
}

// ProgressCallback is called once per file with its path.
type ProgressCallback func(path string)

type (
	ChunkingProgressCallback  = ProgressCallback
	ArchivingProgressCallback = ProgressCallback
	RestoringProgressCallback = ProgressCallback
)

// DeletionProgressCallback is called for every chunk dereferenced by a delete or clean.
// chunkID is ChunkHash.ID of the chunk.
type DeletionProgressCallback func(chunkID uint64, deleted bool)

type CleaningProgressCallback = DeletionProgressCallback

// RebuildProgressCallback is called for every chunk reference counted during a rebuild.
type RebuildProgressCallback func(hash ChunkHash, references uint64)

// CompressionFormatCallback picks the compression of each file backed up into a repository.
type CompressionFormatCallback func(path string) CompressionFormat

// CompressionCallback picks the compression of each file added to a standalone archive.
type CompressionCallback func(path string, size uint64) CompressionFormat

// RealSizeCallback overrides the recorded uncompressed size of a file added to a standalone archive.
type RealSizeCallback func(path string) uint64

// Callbacks may be invoked concurrently from library worker threads.
type callbacks struct {
	progress    ProgressCallback
	archiving   ProgressCallback
	deletion    DeletionProgressCallback
	rebuild     RebuildProgressCallback
	compression CompressionFormatCallback
	archiveComp CompressionCallback
	realSize    RealSizeCallback
}

// userData stores a cgo.Handle to cb in C memory and returns the pointer handed to the
// library as user_data together with a release function.
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
