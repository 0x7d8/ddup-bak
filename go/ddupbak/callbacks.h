#ifndef DDUPBAK_GO_CALLBACKS_H
#define DDUPBAK_GO_CALLBACKS_H

#include <stdbool.h>
#include <stdint.h>
#include <libddupbak.h>

extern void goProgressCallback(char *path, void *data);
extern void goDeletionCallback(uint8_t *hash, bool deleted, void *data);
extern void goRebuildCallback(uint8_t *hash, uint64_t references, void *data);
extern CCompressionFormat goCompressionCallback(char *path, uint64_t size, void *data);
extern uint64_t goRealSizeCallback(char *path, void *data);

static CProgressCallback progressCallback(void) { return (CProgressCallback)goProgressCallback; }
static CDeletionProgressCallback deletionCallback(void) { return (CDeletionProgressCallback)goDeletionCallback; }
static CRebuildProgressCallback rebuildCallback(void) { return (CRebuildProgressCallback)goRebuildCallback; }
static CCompressionFormatCallback compressionCallback(void) { return (CCompressionFormatCallback)goCompressionCallback; }
static CArchiveCompressionCallback archiveCompressionCallback(void) { return (CArchiveCompressionCallback)goCompressionCallback; }
static CRealSizeCallback realSizeCallback(void) { return (CRealSizeCallback)goRealSizeCallback; }

#endif
