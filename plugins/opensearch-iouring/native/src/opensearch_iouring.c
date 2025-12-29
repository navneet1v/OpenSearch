#include "opensearch_iouring.h"
#include <liburing.h>
#include <stdlib.h>
#include <errno.h>

struct osur_ring {
    struct io_uring ring;
};

int osur_ring_create(uint32_t queue_depth,
                     uint32_t flags,
                     osur_ring_t** out) {
    if (!out || queue_depth == 0) return -EINVAL;

    osur_ring_t* r = calloc(1, sizeof(*r));
    if (!r) return -ENOMEM;

    int ret = io_uring_queue_init(queue_depth, &r->ring, flags);
    if (ret < 0) {
        free(r);
        return ret;
    }

    *out = r;
    return 0;
}

void osur_ring_destroy(osur_ring_t* ring) {
    if (!ring) return;
    io_uring_queue_exit(&ring->ring);
    free(ring);
}

int osur_submit_read(osur_ring_t* ring,
                     int fd,
                     void* buffer,
                     uint32_t length,
                     uint64_t offset,
                     uint64_t request_id) {
    if (!ring || !buffer) return -EINVAL;

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring->ring);
    if (!sqe) return -EAGAIN;

    io_uring_prep_read(sqe, fd, buffer, length, offset);
    io_uring_sqe_set_data(sqe, (void*) request_id);
    osur_submit(ring);
    return 0;
}

int osur_submit(osur_ring_t* ring) {
    if (!ring) return -EINVAL;
    return io_uring_submit(&ring->ring);
}

int osur_poll_completion(osur_ring_t* ring,
                         osur_completion_t* out) {
    if (!ring || !out) return -EINVAL;

    struct io_uring_cqe* cqe;
    int ret = io_uring_peek_cqe(&ring->ring, &cqe);
    if (ret < 0) return 0;

    out->user_data = (uint64_t) io_uring_cqe_get_data(cqe);
    out->result = cqe->res;

    io_uring_cqe_seen(&ring->ring, cqe);
    return 1;
}

int osur_wait_completion(osur_ring_t* ring,
                         osur_completion_t* out) {
    if (!ring || !out) return -EINVAL;

    struct io_uring_cqe* cqe;
    int ret = io_uring_wait_cqe(&ring->ring, &cqe);
    if (ret < 0) return ret;

    out->user_data = (uint64_t) io_uring_cqe_get_data(cqe);
    out->result = cqe->res;

    io_uring_cqe_seen(&ring->ring, cqe);
    return 0;
}
