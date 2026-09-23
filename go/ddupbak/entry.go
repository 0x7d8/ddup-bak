package ddupbak

/*
#include <libddupbak.h>
*/
import "C"

import (
	"errors"
	"time"
	"unsafe"
)

var errEntryClosed = errors.New("ddupbak: entry is closed")

// Entry represents a filesystem entry in an archive.
// Entries from Archive.Entries and DirectoryEntry.Entries are freed with their archive.
// Entries from Archive.FindEntry must be freed by the caller.
type Entry struct {
	entry *C.struct_CEntry
	owned bool
}

// EntryCommon contains common metadata for all entry types
type EntryCommon struct {
	Name  string
	Mode  uint16
	UID   uint32
	GID   uint32
	MTime time.Time
	Type  EntryType
}

// FileEntry represents a file in an archive
type FileEntry struct {
	Common         EntryCommon
	Compression    CompressionFormat
	Size           uint64
	SizeReal       uint64
	SizeCompressed uint64
}

// DirectoryEntry represents a directory in an archive
type DirectoryEntry struct {
	Common  EntryCommon
	Entries []*Entry
}

// SymlinkEntry represents a symbolic link in an archive
type SymlinkEntry struct {
	Common    EntryCommon
	Target    string
	TargetDir bool
}

// Free releases resources associated with the entry
func (e *Entry) Free() {
	if e.entry != nil && e.owned {
		C.free_entry(e.entry)
	}
	e.entry = nil
}

// Close calls Free.
func (e *Entry) Close() { e.Free() }

// Type returns the type of this entry
func (e *Entry) Type() EntryType {
	if e.entry == nil {
		return EntryTypeFile
	}
	return EntryType(C.get_entry_type(e.entry))
}

// Name returns the name of this entry
func (e *Entry) Name() string {
	if e.entry == nil {
		return ""
	}
	return C.GoString(C.entry_name(e.entry))
}

// GetCommon returns common metadata for this entry
func (e *Entry) GetCommon() (EntryCommon, error) {
	if e.entry == nil {
		return EntryCommon{}, errEntryClosed
	}

	common := C.entry_get_common(e.entry)
	if common == nil {
		return EntryCommon{}, errors.New("ddupbak: invalid entry")
	}

	return EntryCommon{
		Name:  C.GoString(common.name),
		Mode:  uint16(common.mode),
		UID:   uint32(common.uid),
		GID:   uint32(common.gid),
		MTime: time.Unix(int64(common.mtime), 0),
		Type:  EntryType(common.entry_type),
	}, nil
}

// Common calls GetCommon.
func (e *Entry) Common() (EntryCommon, error) { return e.GetCommon() }

// AsFile converts this entry to a FileEntry
func (e *Entry) AsFile() (*FileEntry, error) {
	common, err := e.GetCommon()
	if err != nil {
		return nil, err
	}

	file := C.entry_as_file(e.entry)
	if file == nil {
		return nil, errors.New("ddupbak: entry is not a file")
	}

	return &FileEntry{
		Common:         common,
		Compression:    CompressionFormat(file.compression),
		Size:           uint64(file.size),
		SizeReal:       uint64(file.size_real),
		SizeCompressed: uint64(file.size_compressed),
	}, nil
}

// AsDirectory converts this entry to a DirectoryEntry
func (e *Entry) AsDirectory() (*DirectoryEntry, error) {
	common, err := e.GetCommon()
	if err != nil {
		return nil, err
	}

	dir := C.entry_as_directory(e.entry)
	if dir == nil {
		return nil, errors.New("ddupbak: entry is not a directory")
	}

	entries := make([]*Entry, 0, int(dir.entries_count))
	for _, child := range unsafe.Slice(dir.entries, int(dir.entries_count)) {
		entries = append(entries, &Entry{entry: child})
	}

	return &DirectoryEntry{Common: common, Entries: entries}, nil
}

// AsSymlink converts this entry to a SymlinkEntry
func (e *Entry) AsSymlink() (*SymlinkEntry, error) {
	common, err := e.GetCommon()
	if err != nil {
		return nil, err
	}

	link := C.entry_as_symlink(e.entry)
	if link == nil {
		return nil, errors.New("ddupbak: entry is not a symlink")
	}

	return &SymlinkEntry{
		Common:    common,
		Target:    C.GoString(link.target),
		TargetDir: bool(link.target_dir),
	}, nil
}

// RecursiveFree frees an entry and all its children if it's a directory
func RecursiveFree(e *Entry) {
	if e == nil || e.entry == nil {
		return
	}

	if e.Type() == EntryTypeDirectory {
		if dir, err := e.AsDirectory(); err == nil {
			for _, child := range dir.Entries {
				RecursiveFree(child)
			}
		}
	}
	e.Free()
}

// ProcessDirectoryEntries processes all entries in a directory recursively
// This is a helper function that can be used to traverse directories
func ProcessDirectoryEntries(dirEntry *DirectoryEntry, processFn func(*Entry) error) error {
	if dirEntry == nil {
		return errors.New("ddupbak: directory entry is nil")
	}

	for _, entry := range dirEntry.Entries {
		if err := processFn(entry); err != nil {
			return err
		}

		if entry.Type() == EntryTypeDirectory {
			sub, err := entry.AsDirectory()
			if err != nil {
				return err
			}
			if err := ProcessDirectoryEntries(sub, processFn); err != nil {
				return err
			}
		}
	}
	return nil
}

// Walk calls ProcessDirectoryEntries.
func Walk(dir *DirectoryEntry, fn func(*Entry) error) error { return ProcessDirectoryEntries(dir, fn) }
