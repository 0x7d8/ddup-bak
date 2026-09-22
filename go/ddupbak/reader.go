package ddupbak

/*
#include <libddupbak.h>
*/
import "C"

import (
	"errors"
	"io"
	"runtime"
	"unsafe"
)

// EntryReader provides an io.Reader interface for reading file entry content from a repository.
// Until closed it holds a shared repository lock, so Clean and DeleteArchive in this process fail.
type EntryReader struct {
	reader *C.struct_CEntryReader
}

// NewEntryReader creates a new reader for the specified file entry
func (r *Repository) NewEntryReader(entry *Entry) (*EntryReader, error) {
	if r.repo == nil {
		return nil, errClosed
	}
	if entry == nil || entry.entry == nil {
		return nil, errEntryClosed
	}
	if entry.Type() != EntryTypeFile {
		return nil, errors.New("ddupbak: entry is not a file")
	}

	file := C.entry_as_file(entry.entry)
	if file == nil {
		return nil, errors.New("ddupbak: entry is not a file")
	}

	reader := C.repository_create_entry_reader(r.repo, file)
	if reader == nil {
		return nil, lastError("ddupbak: failed to create entry reader")
	}

	result := &EntryReader{reader: reader}
	runtime.SetFinalizer(result, func(reader *EntryReader) { reader.Close() })
	return result, nil
}

// Read implements the io.Reader interface for reading from the entry
func (er *EntryReader) Read(p []byte) (int, error) {
	if er.reader == nil {
		return 0, errors.New("ddupbak: reader is closed")
	}
	if len(p) == 0 {
		return 0, nil
	}

	n := C.entry_reader_read(er.reader, (*C.char)(unsafe.Pointer(&p[0])), C.size_t(len(p)))
	switch {
	case n < 0:
		return 0, lastError("ddupbak: read failed")
	case n == 0:
		return 0, io.EOF
	}
	return int(n), nil
}

// ReadAll reads the entire file entry content into a byte slice
func (er *EntryReader) ReadAll() ([]byte, error) {
	if er.reader == nil {
		return nil, errors.New("ddupbak: reader is closed")
	}
	return io.ReadAll(er)
}

// Close releases resources associated with the reader
func (er *EntryReader) Close() error {
	if er.reader != nil {
		C.free_entry_reader(er.reader)
		er.reader = nil
	}
	return nil
}
