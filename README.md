# ddup-bak

very experimental archive/dedup format that can use multiple different compression formats per file/chunk

repositories written before 0.11 use archive format version 1 and are migrated to version 2 in place the first time 0.11 or later opens them, see ARCHIVE.md.

0.11 changes the C API (callbacks and the functions taking them gained a `user_data` argument, and the restore functions gained a `restored_callback`), so C programs built against 0.10 must be rebuilt, not just pointed at the new library.
