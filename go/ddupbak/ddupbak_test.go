package ddupbak

// Uses the API as callers of pre-BLAKE3 versions do. Needs libddupbak on the linker and loader paths.

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func chunkFormats(t *testing.T, repoDir string) map[byte]int {
	t.Helper()
	formats := map[byte]int{}
	err := filepath.Walk(filepath.Join(repoDir, ".ddup-bak", "chunks"), func(path string, info os.FileInfo, err error) error {
		if err == nil && strings.HasSuffix(path, ".chunk") {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			formats[data[0]]++
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return formats
}

func TestMainEraAPI(t *testing.T) {
	dir := t.TempDir()
	repoDir, src := filepath.Join(dir, "repo"), filepath.Join(dir, "src")
	random := make([]byte, 5000)
	for i := range random {
		random[i] = byte(i*7919 + i>>3)
	}
	text := bytes.Repeat([]byte("B"), 900)
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "a"), random, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "b"), text, 0o644); err != nil {
		t.Fatal(err)
	}

	repo, err := NewRepository(repoDir, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Free()

	var chunked, archived int32
	archive, err := repo.CreateArchive("first", src,
		func(string) { atomic.AddInt32(&chunked, 1) },
		func(string) { atomic.AddInt32(&archived, 1) },
		nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Free()
	// One call per entry: a, sub and sub/b.
	if chunked != 3 || archived != 3 {
		t.Fatalf("progress callbacks: chunked %d archived %d", chunked, archived)
	}
	if err := repo.Save(); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetSaveOnDrop(true); err != nil {
		t.Fatal(err)
	}

	// Defaults: BLAKE2b names, deflate for what compresses, raw for what does not.
	formats := chunkFormats(t, repoDir)
	if formats[byte(CompressionDeflate)] == 0 || formats[byte(CompressionBrotli)] != 0 || formats[byte(CompressionZstd)] != 0 {
		t.Fatalf("unexpected chunk formats %v", formats)
	}

	names, err := repo.ListArchives()
	if err != nil || len(names) != 1 || names[0] != "first" {
		t.Fatalf("ListArchives: %v %v", names, err)
	}

	count, err := archive.EntriesCount()
	if err != nil || count != 2 {
		t.Fatalf("EntriesCount: %d %v", count, err)
	}
	entries, err := archive.Entries()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, entry := range entries {
		seen[entry.Name()] = true
		if entry.Type() == EntryTypeDirectory {
			sub, err := entry.AsDirectory()
			if err != nil {
				t.Fatal(err)
			}
			if err := ProcessDirectoryEntries(sub, func(e *Entry) error { seen[e.Name()] = true; return nil }); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !seen["a"] || !seen["sub"] || !seen["b"] {
		t.Fatalf("entries: %v", seen)
	}

	entry, err := archive.FindEntry("a")
	if err != nil {
		t.Fatal(err)
	}
	file, err := entry.AsFile()
	if err != nil || file.Common.Name != "a" || file.SizeReal != uint64(len(random)) {
		t.Fatalf("AsFile: %+v %v", file, err)
	}
	reader, err := repo.NewEntryReader(entry)
	if err != nil {
		t.Fatal(err)
	}
	data, err := reader.ReadAll()
	reader.Close()
	if err != nil || !bytes.Equal(data, random) {
		t.Fatalf("ReadAll: %d bytes %v", len(data), err)
	}
	RecursiveFree(entry)

	path, err := repo.RestoreArchive("first", nil, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(path) != "first" {
		t.Fatalf("restore path %q", path)
	}
	restored, err := os.ReadFile(filepath.Join(path, "sub", "b"))
	if err != nil || !bytes.Equal(restored, text) {
		t.Fatalf("restored content: %v", err)
	}
	// Restoring again replaces the previous restore.
	if _, err := repo.RestoreArchive("first", nil, nil, 2); err != nil {
		t.Fatal(err)
	}

	// A second backup of the same data adds no chunks.
	before := chunkFormats(t, repoDir)
	second, err := repo.CreateArchive("second", src, nil, nil, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	second.Free()
	if after := chunkFormats(t, repoDir); len(after) != len(before) || after[0] != before[0] || after[2] != before[2] {
		t.Fatalf("dedup: %v -> %v", before, after)
	}

	var deletions int32
	if err := repo.DeleteArchive("first", func(uint64, bool) { atomic.AddInt32(&deletions, 1) }); err != nil {
		t.Fatal(err)
	}
	if deletions == 0 {
		t.Fatal("deletion callback not called")
	}
	if err := repo.Clean(nil); err != nil {
		t.Fatal(err)
	}
	repo.Free()

	reopened, err := OpenRepository(repoDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Free()
	if names, _ := reopened.ListArchives(); len(names) != 1 || names[0] != "second" {
		t.Fatalf("after delete: %v", names)
	}
}
