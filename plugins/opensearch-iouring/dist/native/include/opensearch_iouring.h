/*
 * opensearch_iouring.h
 *
 * A minimal, high-performance IO-uring wrapper for OpenSearch.
 *
 * This library provides a clean C API over liburing, designed specifically
 * for the OpenSearch/Lucene use case of high-throughput file I/O.
 *
 * Thread Safety:
 *   - Each ring instance should be used from a single thread OR
 *   - External synchronization must be provided for multi-threaded access
 *   - Multiple rings can be used safely from different threads
 *
 * Error Handling:
 *   - Functions return 0 on success, negative error code on failure
 *   - Use osur_strerror() to get error description
 *
 * Copyright (c) OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OPENSEARCH_IOURING_H
#define OPENSEARCH_IOURING_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Version and Feature Detection
 * ============================================================================ */

/**
 * Library version (major * 10000 + minor * 100 + patch)
 */
#define OSUR_VERSION 10000  /* 1.0.0 */

/**
 * Check if IO-uring is supported on this system.
 *
 * @return 1 if supported, 0 if not supported
 */
int osur_is_supported(void);

/**
 * Get the library version string.
 *
 * @return Version string (e.g., "1.0.0")
 */
const char* osur_version(void);

/**
 * Get error description for an error code.
 *
 * @param error_code Negative error code returned by osur_* functions
 * @return Human-readable error description
 */
const char* osur_strerror(int error_code);

/* ============================================================================
 * Ring Management
 * ============================================================================ */

/**
 * Opaque ring handle.
 */
typedef struct osur_ring osur_ring_t;

/**
 * Ring configuration flags.
 */
typedef enum {
    OSUR_RING_DEFAULT     = 0,
    OSUR_RING_SQPOLL      = (1 << 0),  /* Kernel-side submission polling */
    OSUR_RING_IOPOLL      = (1 << 1),  /* Busy-wait for I/O completion */
    OSUR_RING_SINGLE_ISSUER = (1 << 2) /* Single thread will submit */
} osur_ring_flags_t;

/**
 * Create a new IO-uring ring.
 *
 * @param queue_depth Number of entries in SQ/CQ (power of 2, e.g., 256, 512, 1024)
 * @param flags       Ring configuration flags (osur_ring_flags_t)
 * @param ring_out    Output: pointer to the created ring
 * @return 0 on success, negative error code on failure
 *
 * Common errors:
 *   -ENOMEM: Out of memory
 *   -EINVAL: Invalid queue_depth
 *   -ENOSYS: IO-uring not supported (kernel too old)
 */
int osur_ring_create(uint32_t queue_depth, uint32_t flags, osur_ring_t** ring_out);

/**
 * Destroy a ring and free all resources.
 *
 * @param ring The ring to destroy (safe to pass NULL)
 */
void osur_ring_destroy(osur_ring_t* ring);

/**
 * Get the queue depth of a ring.
 *
 * @param ring The ring
 * @return Queue depth
 */
uint32_t osur_ring_queue_depth(osur_ring_t* ring);

/**
 * Get the number of pending submissions (not yet submitted to kernel).
 *
 * @param ring The ring
 * @return Number of pending SQEs
 */
uint32_t osur_ring_pending(osur_ring_t* ring);

/* ============================================================================
 * File Registration (Optional Optimization)
 *
 * Registering file descriptors allows the kernel to avoid per-operation
 * fd lookup overhead. Beneficial when repeatedly accessing the same files.
 * ============================================================================ */

/**
 * Register file descriptors for optimized access.
 *
 * After registration, use osur_submit_read_registered() with the index
 * returned by this function instead of the raw fd.
 *
 * @param ring  The ring
 * @param fds   Array of file descriptors to register
 * @param count Number of file descriptors
 * @return 0 on success, negative error code on failure
 */
int osur_register_files(osur_ring_t* ring, const int* fds, uint32_t count);

/**
 * Update a registered file descriptor.
 *
 * @param ring  The ring
 * @param index Index of the file to update
 * @param fd    New file descriptor (-1 to unregister this slot)
 * @return 0 on success, negative error code on failure
 */
int osur_update_registered_file(osur_ring_t* ring, uint32_t index, int fd);

/**
 * Unregister all file descriptors.
 *
 * @param ring The ring
 * @return 0 on success, negative error code on failure
 */
int osur_unregister_files(osur_ring_t* ring);

/* ============================================================================
 * Buffer Registration (Optional Optimization)
 *
 * Registering buffers allows the kernel to avoid per-operation buffer
 * mapping overhead. Beneficial for frequently reused buffers.
 * ============================================================================ */

/**
 * Buffer descriptor for registration.
 */
typedef struct {
    void*  base;   /* Buffer base address */
    size_t length; /* Buffer length */
} osur_buffer_t;

/**
 * Register buffers for optimized access.
 *
 * @param ring    The ring
 * @param buffers Array of buffer descriptors
 * @param count   Number of buffers
 * @return 0 on success, negative error code on failure
 */
int osur_register_buffers(osur_ring_t* ring, const osur_buffer_t* buffers, uint32_t count);

/**
 * Unregister all buffers.
 *
 * @param ring The ring
 * @return 0 on success, negative error code on failure
 */
int osur_unregister_buffers(osur_ring_t* ring);

/* ============================================================================
 * I/O Submission
 *
 * These functions prepare I/O operations. Operations are not sent to the
 * kernel until osur_submit() is called.
 * ============================================================================ */

/**
 * Submit a read operation.
 *
 * @param ring       The ring
 * @param fd         File descriptor to read from
 * @param buf        Buffer to read into
 * @param len        Number of bytes to read
 * @param offset     File offset to read from
 * @param user_data  User data returned with completion (for request tracking)
 * @return 0 on success, -EAGAIN if SQ is full, other negative on error
 */
int osur_submit_read(osur_ring_t* ring,
                     int fd,
                     void* buf,
                     uint32_t len,
                     uint64_t offset,
                     uint64_t user_data);

/**
 * Submit a write operation.
 *
 * @param ring       The ring
 * @param fd         File descriptor to write to
 * @param buf        Buffer containing data to write
 * @param len        Number of bytes to write
 * @param offset     File offset to write to
 * @param user_data  User data returned with completion
 * @return 0 on success, -EAGAIN if SQ is full, other negative on error
 */
int osur_submit_write(osur_ring_t* ring,
                      int fd,
                      const void* buf,
                      uint32_t len,
                      uint64_t offset,
                      uint64_t user_data);

/**
 * Submit a read operation using a registered file descriptor.
 *
 * @param ring       The ring
 * @param fd_index   Index of the registered file descriptor
 * @param buf        Buffer to read into
 * @param len        Number of bytes to read
 * @param offset     File offset to read from
 * @param user_data  User data returned with completion
 * @return 0 on success, negative error code on failure
 */
int osur_submit_read_registered(osur_ring_t* ring,
                                uint32_t fd_index,
                                void* buf,
                                uint32_t len,
                                uint64_t offset,
                                uint64_t user_data);

/**
 * Submit a write operation using a registered file descriptor.
 */
int osur_submit_write_registered(osur_ring_t* ring,
                                 uint32_t fd_index,
                                 const void* buf,
                                 uint32_t len,
                                 uint64_t offset,
                                 uint64_t user_data);

/**
 * Submit a read using both registered file and buffer.
 * This is the most optimized path.
 *
 * @param ring       The ring
 * @param fd_index   Index of the registered file descriptor
 * @param buf_index  Index of the registered buffer
 * @param len        Number of bytes to read
 * @param offset     File offset to read from
 * @param user_data  User data returned with completion
 * @return 0 on success, negative error code on failure
 */
int osur_submit_read_fixed(osur_ring_t* ring,
                           uint32_t fd_index,
                           uint32_t buf_index,
                           uint32_t len,
                           uint64_t offset,
                           uint64_t user_data);

/**
 * Submit an fsync operation.
 *
 * @param ring       The ring
 * @param fd         File descriptor to sync
 * @param user_data  User data returned with completion
 * @return 0 on success, negative error code on failure
 */
int osur_submit_fsync(osur_ring_t* ring, int fd, uint64_t user_data);

/**
 * Submit all pending operations to the kernel.
 *
 * @param ring The ring
 * @return Number of SQEs submitted, or negative error code
 */
int osur_submit(osur_ring_t* ring);

/**
 * Submit and wait for at least min_complete completions.
 *
 * @param ring         The ring
 * @param min_complete Minimum completions to wait for
 * @return Number of SQEs submitted, or negative error code
 */
int osur_submit_and_wait(osur_ring_t* ring, uint32_t min_complete);

/* ============================================================================
 * Completion Handling
 * ============================================================================ */

/**
 * Completion result structure.
 */
typedef struct {
    uint64_t user_data;  /* User data from the submission */
    int32_t  result;     /* Result: bytes transferred (>=0) or error (<0) */
    uint32_t flags;      /* Completion flags (reserved) */
} osur_completion_t;

/**
 * Poll for a completion (non-blocking).
 *
 * @param ring       The ring
 * @param completion Output: completion result (if available)
 * @return 1 if completion available, 0 if none available, negative on error
 */
int osur_poll_completion(osur_ring_t* ring, osur_completion_t* completion);

/**
 * Wait for a completion (blocking).
 *
 * @param ring       The ring
 * @param completion Output: completion result
 * @return 0 on success, negative error code on failure
 */
int osur_wait_completion(osur_ring_t* ring, osur_completion_t* completion);

/**
 * Wait for a completion with timeout.
 *
 * @param ring        The ring
 * @param completion  Output: completion result
 * @param timeout_ms  Timeout in milliseconds (0 = non-blocking, -1 = infinite)
 * @return 0 on success, -ETIMEDOUT on timeout, other negative on error
 */
int osur_wait_completion_timeout(osur_ring_t* ring,
                                  osur_completion_t* completion,
                                  int32_t timeout_ms);

/**
 * Poll for multiple completions (batch processing).
 *
 * @param ring        The ring
 * @param completions Output array for completions
 * @param max_count   Maximum completions to retrieve
 * @return Number of completions retrieved (>=0), or negative error code
 */
int osur_poll_completions(osur_ring_t* ring,
                          osur_completion_t* completions,
                          uint32_t max_count);

/**
 * Get the number of ready completions (without consuming them).
 *
 * @param ring The ring
 * @return Number of ready completions
 */
uint32_t osur_completion_ready(osur_ring_t* ring);

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/**
 * Allocate page-aligned memory suitable for direct I/O.
 *
 * @param size      Size in bytes
 * @param alignment Alignment (typically 4096 for direct I/O)
 * @return Pointer to allocated memory, or NULL on failure
 */
void* osur_alloc_aligned(size_t size, size_t alignment);

/**
 * Free memory allocated by osur_alloc_aligned().
 *
 * @param ptr Pointer to free (safe to pass NULL)
 */
void osur_free_aligned(void* ptr);

#ifdef __cplusplus
}
#endif

#endif /* OPENSEARCH_IOURING_H */
