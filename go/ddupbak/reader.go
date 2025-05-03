package ddupbak

/*
#include <stdlib.h>
#include <stdint.h>
#include <libddupbak.h>
*/
import "C"
import (
	"errors"
	"io"
	"runtime"
	"unsafe"
)

// EntryReader provides an io.Reader interface for reading file entry content from a repository
type EntryReader struct {
	reader *C.struct_CEntryReader
	buffer []byte // Reusable buffer for read operations
}

// NewEntryReader creates a new reader for the specified file entry
func (r *Repository) NewEntryReader(entry *Entry) (*EntryReader, error) {
	if r.repo == nil {
		return nil, errors.New("repository is closed")
	}

	if entry == nil || entry.entry == nil {
		return nil, errors.New("entry is nil")
	}

	if entry.Type() != EntryTypeFile {
		return nil, errors.New("entry is not a file")
	}

	fileEntry, err := C.entry_as_file(entry.entry)
	if err != nil {
		return nil, errors.New("failed to convert entry to file entry")
	}
	reader := C.repository_create_entry_reader(r.repo, fileEntry)
	if reader == nil {
		return nil, errors.New("failed to create entry reader")
	}

	result := &EntryReader{
		reader: reader,
		buffer: make([]byte, 4096), // Default buffer size
	}
	runtime.SetFinalizer(result, (*EntryReader).Close)

	return result, nil
}

// Read implements the io.Reader interface for reading from the entry
func (er *EntryReader) Read(p []byte) (n int, err error) {
	if er.reader == nil {
		return 0, errors.New("reader is closed")
	}

	if len(p) == 0 {
		return 0, nil
	}

	// Create a C buffer to hold the data
	buffer := (*C.char)(unsafe.Pointer(&p[0]))
	bufferSize := C.size_t(len(p))

	bytesRead := C.entry_reader_read(er.reader, buffer, bufferSize)
	if bytesRead < 0 {
		return 0, errors.New("error reading from entry")
	}

	if bytesRead == 0 {
		return 0, io.EOF
	}

	return int(bytesRead), nil
}

// Close releases resources associated with the reader
func (er *EntryReader) Close() error {
	if er.reader != nil {
		C.free_entry_reader(er.reader)
		er.reader = nil
	}
	return nil
}

// ReadAll reads the entire file entry content into a byte slice
func (er *EntryReader) ReadAll() ([]byte, error) {
	if er.reader == nil {
		return nil, errors.New("reader is closed")
	}

	var result []byte
	buffer := make([]byte, 4096) // Use a reasonably sized buffer

	for {
		n, err := er.Read(buffer)
		if n > 0 {
			result = append(result, buffer[:n]...)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	return result, nil
}
