/*
 * opensearch_iouring.c
 *
 * Implementation of the OpenSearch IO-uring wrapper.
 *
 * Copyright (c) OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#include "opensearch_iouring.h"
#include <liburing.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>

/* ============================================================================
 * Internal Structures
 * ============================================================================ */

struct osur_ring {
    struct io_uring ring;
    uint32_t queue_depth;
    uint32_t flags;
    int files_registered;
    int buffers_registered;
};

/* ============================================================================
 * Version and Feature Detection
 * ============================================================================ */

static const char* VERSION_STRING = "1.0.0";

int osur_is_supported(void) {
    struct io_uring ring;
    int ret = io_uring_queue_init(8, &ring, 0);
    if (ret < 0) {
        return 0;
    }
    io_uring_queue_exit(&ring);
    return 1;
}

const char* osur_version(void) {
    return VERSION_STRING;
}

const char* osur_strerror(int error_code) {
    if (error_code >= 0) {
        return "Success";
    }

    int err = -error_code;

    switch (err) {
        case EAGAIN:      return "Resource temporarily unavailable (queue full)";
        case ENOMEM:      return "Out of memory";
        case EINVAL:      return "Invalid argument";
        case ENOSYS:      return "IO-uring not supported (kernel too old)";
        case EPERM:       return "Permission denied";
        case EBADF:       return "Bad file descriptor";
        case EFAULT:      return "Bad address";
        case EBUSY:       return "Resource busy";
        case ETIMEDOUT:   return "Operation timed out";
        case ECANCELED:   return "Operation canceled";
        case EOPNOTSUPP:  return "Operation not supported";
        default:          return strerror(err);
    }
}

/* ============================================================================
 * Ring Management
 * ============================================================================ */

int osur_ring_create(uint32_t queue_depth, uint32_t flags, osur_ring_t** ring_out) {
    if (!ring_out) {
        return -EINVAL;
    }

    *ring_out = NULL;

    // Validate queue_depth is power of 2
    if (queue_depth == 0 || (queue_depth & (queue_depth - 1)) != 0) {
        return -EINVAL;
    }

    osur_ring_t* ring = calloc(1, sizeof(osur_ring_t));
    if (!ring) {
        return -ENOMEM;
    }

    // Convert our flags to io_uring flags
    unsigned int uring_flags = 0;
    if (flags & OSUR_RING_SQPOLL) {
        uring_flags |= IORING_SETUP_SQPOLL;
    }
    if (flags & OSUR_RING_IOPOLL) {
        uring_flags |= IORING_SETUP_IOPOLL;
    }
#ifdef IORING_SETUP_SINGLE_ISSUER
    if (flags & OSUR_RING_SINGLE_ISSUER) {
        uring_flags |= IORING_SETUP_SINGLE_ISSUER;
    }
#endif

    int ret = io_uring_queue_init(queue_depth, &ring->ring, uring_flags);
    if (ret < 0) {
        free(ring);
        return ret;
    }

    ring->queue_depth = queue_depth;
    ring->flags = flags;
    ring->files_registered = 0;
    ring->buffers_registered = 0;

    *ring_out = ring;
    return 0;
}

void osur_ring_destroy(osur_ring_t* ring) {
    if (!ring) {
        return;
    }

    // Unregister resources if needed
    if (ring->files_registered) {
        io_uring_unregister_files(&ring->ring);
    }
    if (ring->buffers_registered) {
        io_uring_unregister_buffers(&ring->ring);
    }

    io_uring_queue_exit(&ring->ring);
    free(ring);
}

uint32_t osur_ring_queue_depth(osur_ring_t* ring) {
    return ring ? ring->queue_depth : 0;
}

uint32_t osur_ring_pending(osur_ring_t* ring) {
    if (!ring) return 0;
    return io_uring_sq_ready(&ring->ring);
}

/* ============================================================================
 * File Registration
 * ============================================================================ */

int osur_register_files(osur_ring_t* ring, const int* fds, uint32_t count) {
    if (!ring || !fds || count == 0) {
        return -EINVAL;
    }

    if (ring->files_registered) {
        // Must unregister first
        return -EBUSY;
    }

    int ret = io_uring_register_files(&ring->ring, fds, count);
    if (ret < 0) {
        return ret;
    }

    ring->files_registered = 1;
    return 0;
}

int osur_update_registered_file(osur_ring_t* ring, uint32_t index, int fd) {
    if (!ring || !ring->files_registered) {
        return -EINVAL;
    }

    int ret = io_uring_register_files_update(&ring->ring, index, &fd, 1);
    return ret < 0 ? ret : 0;
}

int osur_unregister_files(osur_ring_t* ring) {
    if (!ring) {
        return -EINVAL;
    }

    if (!ring->files_registered) {
        return 0;  // Nothing to do
    }

    int ret = io_uring_unregister_files(&ring->ring);
    if (ret == 0) {
        ring->files_registered = 0;
    }
    return ret;
}

/* ============================================================================
 * Buffer Registration
 * ============================================================================ */

int osur_register_buffers(osur_ring_t* ring, const osur_buffer_t* buffers, uint32_t count) {
    if (!ring || !buffers || count == 0) {
        return -EINVAL;
    }

    if (ring->buffers_registered) {
        return -EBUSY;
    }

    // Convert to iovec array
    struct iovec* iovecs = calloc(count, sizeof(struct iovec));
    if (!iovecs) {
        return -ENOMEM;
    }

    for (uint32_t i = 0; i < count; i++) {
        iovecs[i].iov_base = buffers[i].base;
        iovecs[i].iov_len = buffers[i].length;
    }

    int ret = io_uring_register_buffers(&ring->ring, iovecs, count);
    free(iovecs);

    if (ret < 0) {
        return ret;
    }

    ring->buffers_registered = 1;
    return 0;
}

int osur_unregister_buffers(osur_ring_t* ring) {
    if (!ring) {
        return -EINVAL;
    }

    if (!ring->buffers_registered) {
        return 0;
    }

    int ret = io_uring_unregister_buffers(&ring->ring);
    if (ret == 0) {
        ring->buffers_registered = 0;
    }
    return ret;
}

/* ============================================================================
 * I/O Submission
 * ============================================================================ */

int osur_submit_read(osur_ring_t* ring,
                     int fd,
                     void* buf,
                     uint32_t len,
                     uint64_t offset,
                     uint64_t user_data) {
    if (!ring || !buf) {
        return -EINVAL;
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring->ring);
    if (!sqe) {
        return -EAGAIN;
    }

    io_uring_prep_read(sqe, fd, buf, len, offset);
    io_uring_sqe_set_data64(sqe, user_data);

    return 0;
}

int osur_submit_write(osur_ring_t* ring,
                      int fd,
                      const void* buf,
                      uint32_t len,
                      uint64_t offset,
                      uint64_t user_data) {
    if (!ring || !buf) {
        return -EINVAL;
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring->ring);
    if (!sqe) {
        return -EAGAIN;
    }

    io_uring_prep_write(sqe, fd, buf, len, offset);
    io_uring_sqe_set_data64(sqe, user_data);

    return 0;
}

int osur_submit_read_registered(osur_ring_t* ring,
                                uint32_t fd_index,
                                void* buf,
                                uint32_t len,
                                uint64_t offset,
                                uint64_t user_data) {
    if (!ring || !buf) {
        return -EINVAL;
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring->ring);
    if (!sqe) {
        return -EAGAIN;
    }

    io_uring_prep_read(sqe, fd_index, buf, len, offset);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data64(sqe, user_data);

    return 0;
}

int osur_submit_write_registered(osur_ring_t* ring,
                                 uint32_t fd_index,
                                 const void* buf,
                                 uint32_t len,
                                 uint64_t offset,
                                 uint64_t user_data) {
    if (!ring || !buf) {
        return -EINVAL;
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring->ring);
    if (!sqe) {
        return -EAGAIN;
    }

    io_uring_prep_write(sqe, fd_index, buf, len, offset);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data64(sqe, user_data);

    return 0;
}

int osur_submit_read_fixed(osur_ring_t* ring,
                           uint32_t fd_index,
                           uint32_t buf_index,
                           uint32_t len,
                           uint64_t offset,
                           uint64_t user_data) {
    if (!ring) {
        return -EINVAL;
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring->ring);
    if (!sqe) {
        return -EAGAIN;
    }

    // For fixed buffers, we need to use prep_read_fixed
    // The buffer address is obtained from the registered buffer at buf_index
    io_uring_prep_read_fixed(sqe, fd_index, NULL, len, offset, buf_index);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data64(sqe, user_data);

    return 0;
}

int osur_submit_fsync(osur_ring_t* ring, int fd, uint64_t user_data) {
    if (!ring) {
        return -EINVAL;
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring->ring);
    if (!sqe) {
        return -EAGAIN;
    }

    io_uring_prep_fsync(sqe, fd, 0);
    io_uring_sqe_set_data64(sqe, user_data);

    return 0;
}

int osur_submit(osur_ring_t* ring) {
    if (!ring) {
        return -EINVAL;
    }
    return io_uring_submit(&ring->ring);
}

int osur_submit_and_wait(osur_ring_t* ring, uint32_t min_complete) {
    if (!ring) {
        return -EINVAL;
    }
    return io_uring_submit_and_wait(&ring->ring, min_complete);
}

/* ============================================================================
 * Completion Handling
 * ============================================================================ */

int osur_poll_completion(osur_ring_t* ring, osur_completion_t* completion) {
    if (!ring || !completion) {
        return -EINVAL;
    }

    struct io_uring_cqe* cqe;
    int ret = io_uring_peek_cqe(&ring->ring, &cqe);

    if (ret < 0) {
        return 0;  // No completion available (not an error)
    }

    completion->user_data = io_uring_cqe_get_data64(cqe);
    completion->result = cqe->res;
    completion->flags = cqe->flags;

    io_uring_cqe_seen(&ring->ring, cqe);

    return 1;
}

int osur_wait_completion(osur_ring_t* ring, osur_completion_t* completion) {
    if (!ring || !completion) {
        return -EINVAL;
    }

    struct io_uring_cqe* cqe;
    int ret = io_uring_wait_cqe(&ring->ring, &cqe);

    if (ret < 0) {
        return ret;
    }

    completion->user_data = io_uring_cqe_get_data64(cqe);
    completion->result = cqe->res;
    completion->flags = cqe->flags;

    io_uring_cqe_seen(&ring->ring, cqe);

    return 0;
}

int osur_wait_completion_timeout(osur_ring_t* ring,
                                  osur_completion_t* completion,
                                  int32_t timeout_ms) {
    if (!ring || !completion) {
        return -EINVAL;
    }

    // Non-blocking case
    if (timeout_ms == 0) {
        return osur_poll_completion(ring, completion) == 1 ? 0 : -ETIMEDOUT;
    }

    struct io_uring_cqe* cqe;
    int ret;

    if (timeout_ms < 0) {
        // Infinite wait
        ret = io_uring_wait_cqe(&ring->ring, &cqe);
    } else {
        // Timed wait
        struct __kernel_timespec ts;
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000L;

        ret = io_uring_wait_cqe_timeout(&ring->ring, &cqe, &ts);
    }

    if (ret < 0) {
        return ret;
    }

    completion->user_data = io_uring_cqe_get_data64(cqe);
    completion->result = cqe->res;
    completion->flags = cqe->flags;

    io_uring_cqe_seen(&ring->ring, cqe);

    return 0;
}

int osur_poll_completions(osur_ring_t* ring,
                          osur_completion_t* completions,
                          uint32_t max_count) {
    if (!ring || !completions || max_count == 0) {
        return -EINVAL;
    }

    uint32_t count = 0;
    struct io_uring_cqe* cqe;

    while (count < max_count) {
        int ret = io_uring_peek_cqe(&ring->ring, &cqe);
        if (ret < 0) {
            break;  // No more completions
        }

        completions[count].user_data = io_uring_cqe_get_data64(cqe);
        completions[count].result = cqe->res;
        completions[count].flags = cqe->flags;

        io_uring_cqe_seen(&ring->ring, cqe);
        count++;
    }

    return count;
}

uint32_t osur_completion_ready(osur_ring_t* ring) {
    if (!ring) return 0;
    return io_uring_cq_ready(&ring->ring);
}

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

void* osur_alloc_aligned(size_t size, size_t alignment) {
    void* ptr = NULL;
    if (posix_memalign(&ptr, alignment, size) != 0) {
        return NULL;
    }
    return ptr;
}

void osur_free_aligned(void* ptr) {
    free(ptr);
}
