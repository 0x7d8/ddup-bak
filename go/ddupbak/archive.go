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

var errArchiveClosed = errors.New("ddupbak: archive is closed")

// Archive represents a ddupbak archive
type Archive struct {
	archive *C.struct_CArchive

	entries      **C.struct_CEntry
	entriesCount C.uint
	releases     []func()
}

func wrapArchive(archive *C.struct_CArchive, fallback string) (*Archive, error) {
	if archive == nil {
		return nil, lastError(fallback)
	}

	result := &Archive{archive: archive}
	runtime.SetFinalizer(result, (*Archive).Free)
	return result, nil
}

// NewArchive creates a new empty archive
func NewArchive(path string) (*Archive, error) {
	if path == "" {
		return nil, errors.New("ddupbak: path cannot be empty")
	}

	cPath := cString(path)
	defer freeCString(cPath)

	return wrapArchive(C.new_archive(cPath), "ddupbak: failed to create archive")
}

// OpenArchive opens an existing archive
func OpenArchive(path string) (*Archive, error) {
	if path == "" {
		return nil, errors.New("ddupbak: path cannot be empty")
	}

	cPath := cString(path)
	defer freeCString(cPath)

	return wrapArchive(C.open_archive(cPath), "ddupbak: failed to open archive")
}

// Free releases resources associated with the archive
func (a *Archive) Free() {
	if a.archive == nil {
		return
	}

	if a.entries != nil {
		C.free_entry_array(a.entries, a.entriesCount)
		a.entries = nil
	}
	C.free_archive(a.archive)
	a.archive = nil

	for _, release := range a.releases {
		release()
	}
	a.releases = nil
}

// Close calls Free.
func (a *Archive) Close() { a.Free() }

// AddDirectory adds the contents of path to the archive.
func (a *Archive) AddDirectory(path string, progress ProgressCallback) error {
	if a.archive == nil {
		return errArchiveClosed
	}

	cPath := cString(path)
	defer freeCString(cPath)

	data, release := userData(&callbacks{progress: progress})
	defer release()

	if C.archive_add_directory(a.archive, cPath, cProgressCallback(progress), data) != 0 {
		return lastError("ddupbak: failed to add directory")
	}
	return nil
}

// SetCompressionCallback sets a callback to determine compression format for files
func (a *Archive) SetCompressionCallback(callback CompressionCallback) error {
	if a.archive == nil {
		return errArchiveClosed
	}
	if callback == nil {
		return errors.New("ddupbak: callback cannot be nil")
	}

	data, release := userData(&callbacks{archiveComp: callback})
	a.releases = append(a.releases, release)
	C.archive_set_compression_callback(a.archive, C.archiveCompressionCallback(), data)
	return nil
}

// SetRealSizeCallback sets a callback to determine the real size of files
func (a *Archive) SetRealSizeCallback(callback RealSizeCallback) error {
	if a.archive == nil {
		return errArchiveClosed
	}
	if callback == nil {
		return errors.New("ddupbak: callback cannot be nil")
	}

	data, release := userData(&callbacks{realSize: callback})
	a.releases = append(a.releases, release)
	C.archive_set_real_size_callback(a.archive, C.realSizeCallback(), data)
	return nil
}

// EntriesCount returns the number of entries in the archive
func (a *Archive) EntriesCount() (uint, error) {
	if a.archive == nil {
		return 0, errArchiveClosed
	}
	return uint(C.archive_entries_count(a.archive)), nil
}

// Entries returns all entries in the archive
func (a *Archive) Entries() ([]*Entry, error) {
	if a.archive == nil {
		return nil, errArchiveClosed
	}

	if a.entries == nil {
		a.entriesCount = C.archive_entries_count(a.archive)
		a.entries = C.archive_entries(a.archive)
		if a.entries == nil && a.entriesCount > 0 {
			return nil, lastError("ddupbak: failed to read entries")
		}
	}

	entries := make([]*Entry, 0, int(a.entriesCount))
	for _, entry := range unsafe.Slice(a.entries, int(a.entriesCount)) {
		entries = append(entries, &Entry{entry: entry})
	}
	return entries, nil
}

// FindEntry finds an entry by path
func (a *Archive) FindEntry(path string) (*Entry, error) {
	if a.archive == nil {
		return nil, errArchiveClosed
	}

	cPath := cString(path)
	defer freeCString(cPath)

	entry := C.archive_find_entry(a.archive, cPath)
	if entry == nil {
		return nil, errors.New("ddupbak: entry not found")
	}

	result := &Entry{entry: entry, owned: true}
	runtime.SetFinalizer(result, (*Entry).Free)
	return result, nil
}
