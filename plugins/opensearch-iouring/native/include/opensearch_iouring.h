#ifndef OPENSEARCH_IOURING_H
#define OPENSEARCH_IOURING_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* =======================
 * Opaque Types (Handles)
 * ======================= */

typedef struct osur_ring osur_ring_t;

/* =======================
 * Constants
 * ======================= */

#define OSUR_VERSION "1.0.0"

#define OSUR_RING_DEFAULT        0x0
#define OSUR_RING_SQPOLL         0x1
#define OSUR_RING_IOPOLL         0x2
#define OSUR_RING_SINGLE_ISSUER  0x4

/* =======================
 * Data Structures
 * ======================= */

typedef struct {
    void*    base;
    uint32_t length;
} osur_buffer_t;

typedef struct {
    uint64_t user_data;
    int32_t  result;
    uint32_t flags;
} osur_completion_t;

/* =======================
 * Capability / Info
 * ======================= */

int         osur_is_supported(void);
const char* osur_version(void);
const char* osur_strerror(int error);

/* =======================
 * Ring Lifecycle
 * ======================= */

int  osur_ring_create(uint32_t depth, uint32_t flags, osur_ring_t** out);
void osur_ring_destroy(osur_ring_t* ring);

uint32_t osur_ring_queue_depth(osur_ring_t* ring);
uint32_t osur_ring_pending(osur_ring_t* ring);

/* =======================
 * I/O Submission
 * ======================= */

int osur_submit_read(
    osur_ring_t* ring,
    int fd,
    void* buf,
    uint32_t len,
    uint64_t offset,
    uint64_t user_data
);

int osur_submit(osur_ring_t* ring);

/* =======================
 * Completion
 * ======================= */

int osur_wait_completion(
    osur_ring_t* ring,
    osur_completion_t* completion
);

int osur_poll_completion(
    osur_ring_t* ring,
    osur_completion_t* completion
);

/* =======================
 * Memory Helpers
 * ======================= */

void* osur_alloc_aligned(size_t size, size_t alignment);
void  osur_free_aligned(void* ptr);

#ifdef __cplusplus
}
#endif

#endif /* OPENSEARCH_IOURING_H */
