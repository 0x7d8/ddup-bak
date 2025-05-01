package ddupbak

/*
#cgo LDFLAGS: -lddupbak
#include <stdlib.h>
#include <stdint.h>
#include <libddupbak.h>
*/
import "C"
import (
	"errors"
	"unsafe"
)

// CompressionFormat defines the compression algorithm used for files
type CompressionFormat int

const (
	CompressionNone    CompressionFormat = 0
	CompressionGzip    CompressionFormat = 1
	CompressionDeflate CompressionFormat = 2
)

// EntryType defines the type of a filesystem entry
type EntryType int

const (
	EntryTypeFile      EntryType = 0
	EntryTypeDirectory EntryType = 1
	EntryTypeSymlink   EntryType = 2
)

// Error handling - converting C integer return values to Go errors
func cErrorToGoError(code C.int) error {
	if code == 0 {
		return nil
	}
	return errors.New("ddupbak operation failed")
}

// Helper function to convert Go string array to C string array
func goStringsToCStrings(strings []string) (**C.char, func()) {
	if len(strings) == 0 {
		return nil, func() {}
	}

	cStrings := C.malloc(C.size_t(len(strings)) * C.size_t(unsafe.Sizeof(uintptr(0))))
	for i, str := range strings {
		cStr := C.CString(str)
		ptr := (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(cStrings)) + uintptr(i)*unsafe.Sizeof(uintptr(0))))
		*ptr = cStr
	}

	cleanup := func() {
		for i := 0; i < len(strings); i++ {
			ptr := (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(cStrings)) + uintptr(i)*unsafe.Sizeof(uintptr(0))))
			C.free(unsafe.Pointer(*ptr))
		}
		C.free(unsafe.Pointer(cStrings))
	}

	return (**C.char)(cStrings), cleanup
}

// Helper function to convert C string array to Go string array
func cStringsToGoStrings(cArray **C.char, count C.uint) []string {
	if cArray == nil || count == 0 {
		return []string{}
	}

	result := make([]string, count)
	for i := 0; i < int(count); i++ {
		ptr := (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(cArray)) + uintptr(i)*unsafe.Sizeof(uintptr(0))))
		result[i] = C.GoString(*ptr)
	}

	return result
}
