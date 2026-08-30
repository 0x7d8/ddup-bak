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

// Entry is a file, directory or symlink inside an archive. Entries from Archive.Entries or
// DirectoryEntry.Entries are owned by their archive and freed with it; Free on them is a no-op.
// Entries from Archive.FindEntry are owned by the caller.
type Entry struct {
	entry *C.struct_CEntry
	owned bool
}

// EntryCommon is the metadata every entry type has.
type EntryCommon struct {
	Name  string
	Mode  uint16
	UID   uint32
	GID   uint32
	MTime time.Time
	Type  EntryType
}

type FileEntry struct {
	Common         EntryCommon
	Compression    CompressionFormat
	Size           uint64
	SizeReal       uint64
	SizeCompressed uint64
}

type DirectoryEntry struct {
	Common  EntryCommon
	Entries []*Entry
}

type SymlinkEntry struct {
	Common    EntryCommon
	Target    string
	TargetDir bool
}

// Free releases an entry obtained from Archive.FindEntry; a no-op for archive-owned entries.
func (e *Entry) Free() {
	if e.entry != nil && e.owned {
		C.free_entry(e.entry)
	}
	e.entry = nil
}

// Close is Free.
func (e *Entry) Close() { e.Free() }

func (e *Entry) Type() EntryType {
	if e.entry == nil {
		return EntryTypeFile
	}
	return EntryType(C.get_entry_type(e.entry))
}

func (e *Entry) Name() string {
	if e.entry == nil {
		return ""
	}
	return C.GoString(C.entry_name(e.entry))
}

// GetCommon returns the metadata shared by all entry types.
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

// Common is GetCommon.
func (e *Entry) Common() (EntryCommon, error) { return e.GetCommon() }

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

// RecursiveFree frees an entry and, for directories, its children. Archive-owned entries are
// left to the archive.
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

// ProcessDirectoryEntries calls processFn for every entry below dirEntry, depth first.
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

// Walk is ProcessDirectoryEntries.
func Walk(dir *DirectoryEntry, fn func(*Entry) error) error { return ProcessDirectoryEntries(dir, fn) }
