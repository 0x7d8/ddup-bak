package ddupbak

// #include <stdlib.h>
// #include <stdint.h>
// #include <libddupbak.h>
import "C"
import (
	"errors"
	"time"
	"unsafe"
)

// Entry represents a filesystem entry in an archive
type Entry struct {
	entry *C.struct_CEntry
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
	if e.entry != nil {
		C.free_entry(e.entry)
		e.entry = nil
	}
}

// Type returns the type of this entry
func (e *Entry) Type() EntryType {
	if e.entry == nil {
		return EntryTypeFile
	}

	entryType := C.get_entry_type(e.entry)
	return EntryType(entryType)
}

// Name returns the name of this entry
func (e *Entry) Name() string {
	if e.entry == nil {
		return ""
	}

	cName := C.entry_name(e.entry)
	if cName == nil {
		return ""
	}

	return C.GoString(cName)
}

// GetCommon returns common metadata for this entry
func (e *Entry) GetCommon() (EntryCommon, error) {
	if e.entry == nil {
		return EntryCommon{}, errors.New("entry is closed")
	}

	cCommon := C.entry_get_common(e.entry)
	if cCommon == nil {
		return EntryCommon{}, errors.New("failed to get entry common data")
	}

	result := EntryCommon{
		Name:  C.GoString(cCommon.name),
		Mode:  uint16(cCommon.mode),
		UID:   uint32(cCommon.uid),
		GID:   uint32(cCommon.gid),
		MTime: time.Unix(int64(cCommon.mtime), 0),
		Type:  EntryType(cCommon.entry_type),
	}

	return result, nil
}

// AsFile converts this entry to a FileEntry
func (e *Entry) AsFile() (*FileEntry, error) {
	if e.entry == nil {
		return nil, errors.New("entry is closed")
	}

	if e.Type() != EntryTypeFile {
		return nil, errors.New("entry is not a file")
	}

	cFile := C.entry_as_file(e.entry)
	if cFile == nil {
		return nil, errors.New("failed to convert entry to file")
	}

	common, err := e.GetCommon()
	if err != nil {
		return nil, err
	}

	result := &FileEntry{
		Common:         common,
		Compression:    CompressionFormat(cFile.compression),
		Size:           uint64(cFile.size),
		SizeReal:       uint64(cFile.size_real),
		SizeCompressed: uint64(cFile.size_compressed),
	}

	return result, nil
}

// AsDirectory converts this entry to a DirectoryEntry
func (e *Entry) AsDirectory() (*DirectoryEntry, error) {
	if e.entry == nil {
		return nil, errors.New("entry is closed")
	}

	if e.Type() != EntryTypeDirectory {
		return nil, errors.New("entry is not a directory")
	}

	cDir := C.entry_as_directory(e.entry)
	if cDir == nil {
		return nil, errors.New("failed to convert entry to directory")
	}

	common, err := e.GetCommon()
	if err != nil {
		return nil, err
	}

	entriesCount := int(cDir.entries_count)
	entries := make([]*Entry, 0, entriesCount)

	for i := 0; i < entriesCount; i++ {
		// Get pointer to the i-th entry
		entryPtrPtr := (**C.struct_CEntry)(unsafe.Pointer(
			uintptr(unsafe.Pointer(cDir.entries)) + uintptr(i)*unsafe.Sizeof(uintptr(0)),
		))

		if *entryPtrPtr != nil {
			// Create a new Entry wrapper around the C pointer
			entry := &Entry{entry: *entryPtrPtr}
			entries = append(entries, entry)
		}
	}

	result := &DirectoryEntry{
		Common:  common,
		Entries: entries,
	}

	return result, nil
}

// AsSymlink converts this entry to a SymlinkEntry
func (e *Entry) AsSymlink() (*SymlinkEntry, error) {
	if e.entry == nil {
		return nil, errors.New("entry is closed")
	}

	if e.Type() != EntryTypeSymlink {
		return nil, errors.New("entry is not a symlink")
	}

	cSymlink := C.entry_as_symlink(e.entry)
	if cSymlink == nil {
		return nil, errors.New("failed to convert entry to symlink")
	}

	common, err := e.GetCommon()
	if err != nil {
		return nil, err
	}

	result := &SymlinkEntry{
		Common:    common,
		Target:    C.GoString(cSymlink.target),
		TargetDir: bool(cSymlink.target_dir),
	}

	return result, nil
}

// RecursiveFree frees an entry and all its children if it's a directory
func RecursiveFree(e *Entry) {
	if e == nil {
		return
	}

	// If it's a directory, free all its children first
	if e.Type() == EntryTypeDirectory {
		dir, err := e.AsDirectory()
		if err == nil && dir != nil {
			for _, childEntry := range dir.Entries {
				RecursiveFree(childEntry)
			}
		}
	}

	// Then free the entry itself
	e.Free()
}

// ProcessDirectoryEntries processes all entries in a directory recursively
// This is a helper function that can be used to traverse directories
func ProcessDirectoryEntries(dirEntry *DirectoryEntry, processFn func(*Entry) error) error {
	if dirEntry == nil {
		return errors.New("directory entry is nil")
	}

	for _, entry := range dirEntry.Entries {
		// Process this entry
		if err := processFn(entry); err != nil {
			return err
		}

		// If it's a directory, process its entries recursively
		if entry.Type() == EntryTypeDirectory {
			subDir, err := entry.AsDirectory()
			if err != nil {
				return err
			}

			if err := ProcessDirectoryEntries(subDir, processFn); err != nil {
				return err
			}
		}
	}

	return nil
}
