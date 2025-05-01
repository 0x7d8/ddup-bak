# ddupbak Go Bindings

This package provides Go bindings for the `ddupbak` library, allowing you to use the de-duplicating backup functionality from Go applications.

## Prerequisites

- Go 1.19 or later
- C compiler (gcc, clang, etc.)
- The compiled ddupbak C library (libddupbak.so on Linux or libddupbak.dylib on macOS)

## Installation

1. Build the C library first:
   ```bash
   cd /path/to/ddup-bak/c
   cargo build --release
   ```

2. Add this library to your Go project:
   ```bash
   go get github.com/0x7d8/ddupbak
   ```

3. Ensure the library can be found at runtime:
   ```bash
   export LD_LIBRARY_PATH=/path/to/ddup-bak/c/target/release:$LD_LIBRARY_PATH
   ```

## Basic Usage

```go
package main

import (
    "fmt"
    "github.com/0x7d8/ddupbak/ddupbak"
)

func main() {
    // Create a new repository
    repo, err := ddupbak.NewRepository(
        "/tmp/backup-repo",
        1024*1024,  // 1MB chunk size
        1000,       // max 1000 chunks
        []string{".git", "node_modules"}, // Ignored files
    )
    if err != nil {
        panic(err)
    }
    defer repo.Free()

    // Create an archive
    archive, err := repo.CreateArchive(
        "my-backup",
        "/path/to/backup",
        func(path string) {
            fmt.Printf("Chunking: %s\n", path)
        },
        func(path string) {
            fmt.Printf("Archiving: %s\n", path)
        },
        4, // Use 4 threads
    )
    if err != nil {
        panic(err)
    }
    defer archive.Free()

    // List archives
    archives, err := repo.ListArchives()
    if err != nil {
        panic(err)
    }
    
    fmt.Println("Available archives:")
    for _, name := range archives {
        fmt.Printf("  - %s\n", name)
    }

    // Restore an archive
    restoredPath, err := repo.RestoreArchive(
        "my-backup",
        func(path string) {
            fmt.Printf("Restoring: %s\n", path)
        },
        4, // Use 4 threads
    )
    if err != nil {
        panic(err)
    }
    
    fmt.Printf("Archive restored to: %s\n", restoredPath)
}
```

## Features

- Create and open repositories
- Manage ignored files
- Create archives with custom compression settings
- List, restore, and delete archives
- Read archive contents
- Efficient chunk handling with callbacks for progress reporting
- Memory safety with proper resource cleanup

## Thread Safety

The bindings are not fully thread-safe. Concurrent operations on the same repository or archive instance may lead to unexpected behavior. Use separate instances for each thread or implement your own synchronization.

## Limitations

- Callbacks are simplified and may not work in all scenarios, especially with concurrent operations
- Error handling is basic and might not provide detailed error information
- Some advanced features of the C library are not fully exposed

## License

Same as the underlying ddupbak library
