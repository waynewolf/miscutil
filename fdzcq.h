/**
 * FDZCQ is fd based zero copy queue
 *
 * FDZCQ is fit for dmabuf based camera buffer transfer across processes.
 *
 * The consumer will always get the oldest buffer available in queue. If a consumer is running
 * slow, the buffer is gone. A buffer may be consumed multiple times by different consumers.
 *
 * FDZCQ is actually an SPMC (single producer, multiple consumer) queue.
 * The best usage scenario is using FDZCQ to connect producer and consumers across processes.
 */
#ifndef MISCUTIL_FDZCQ_H
#define MISCUTIL_FDZCQ_H

#include <stdint.h>

#define MSU_FDZCQ_MAX_CONSUMER          4

#ifdef __cplusplus
extern "C"{
#endif

typedef enum msu_fdzcq_status_e {
    MSU_FDZCQ_STATUS_OK,
    MSU_FDZCQ_STATUS_ERR,
    MSU_FDZCQ_STATUS_CONSUMER_NOT_FOUND,
    MSU_FDZCQ_STATUS_NO_BUF,
    MSU_FDZCQ_STATUS_MEMORY_ERR,
} msu_fdzcq_status_t;

typedef struct msu_fdzcq_s *msu_fdzcq_handle_t;

typedef struct msu_fdbuf_s {
    int                 fd;
    int                 ref_count;          /* zero means slot empty */
} msu_fdbuf_t;

/* the callback function is called within the semaphore protection */
typedef void (*msu_fdbuf_release_func_t)(msu_fdzcq_handle_t q, msu_fdbuf_t *fdbuf);

/**
 * producer create fdzcq
 *
 * @param capacity maximum nr of items in fdzcq
 * @return the handle of fdzcq
 */
msu_fdzcq_handle_t msu_fdzcq_create(uint8_t capacity, msu_fdbuf_release_func_t free_cb, void *user_data);

/**
 * producer destroy fdzcq
 *
 * @param q the handle of fdzcq
 */
void msu_fdzcq_destroy(msu_fdzcq_handle_t q);

/**
 * consumer acquires fdzcq
 *
 * @return the handle of fdzcq
 */
msu_fdzcq_handle_t msu_fdzcq_acquire(msu_fdbuf_release_func_t free_cb, void *user_data);

/**
 * consumer releases fdzcq
 *
 * @param q the handle of fdzcq
 */
void msu_fdzcq_release(msu_fdzcq_handle_t q);

/**
 * register consumer
 *
 * @param q the handle of fdzcq
 * @return -1 failed, >= 0 the consumer id
 */
int msu_fdzcq_register_consumer(msu_fdzcq_handle_t q);

/**
 * deregister consumer
 * @param q the handle of fdzcq
 * @param consumer_id the consumer id to be deregisterred
 */
void msu_fdzcq_deregister_consumer(msu_fdzcq_handle_t q, int consumer_id);

/**
 * enumerate consumers
 *
 * @param q the handle of fdzcq
 * @param consumer consumer id array
 * @return number of consumers
 */
int msu_fdzcq_enumerate_consumers(msu_fdzcq_handle_t q, int consumer[MSU_FDZCQ_MAX_CONSUMER]);

/**
 * produce a fd-bazed buf in queue
 *
 * @param q the handle of fdzcq
 * @param fd the buf based on fd
 * @return status
 */
msu_fdzcq_status_t msu_fdzcq_produce(msu_fdzcq_handle_t q, int fd);

/**
 * producer check whether data has arrived from consumer
 *
 * @param q the handle of fdzcq
 * @return 0: no data, >0: the client socket to to read
 */
int msu_fdzcq_producer_has_data(msu_fdzcq_handle_t q);

void msu_fdzcq_producer_handle_data(msu_fdzcq_handle_t q, int client_sock, uint8_t *buf, size_t max_len);

void msu_fdzcq_producer_run(msu_fdzcq_handle_t q);

/**
 * consume a fd-bazed buf in queue. Notice that refcount is added.
 *
 * @param q the handle of fdzcq
 * @param consumer_id the consumer id returned by msu_fdzcq_register_consumer
 * @param fdbuf the output data wrapped in msu_fdbuf_t.
 *              Notice: the pointer should NOT be freed by the caller.
 * @param fd the output correct fd in separate process.
 *              Notice: fdbuf->fd is the fd in producer process, which CANNOT be used in the consumer process.
 * @return status
 */
msu_fdzcq_status_t msu_fdzcq_consume(msu_fdzcq_handle_t q, int consumer_id, msu_fdbuf_t **fdbuf, int *fd);

/**
 * get the number of the buffers in the queue
 *
 * @param q the handle of fdzcq
 * @return number of buffers in the queue
 */
int msu_fdzcq_size(msu_fdzcq_handle_t q);

/**
 * whether the buffer is empty or not
 *
 * @param q the handle of fdzcq
 * @return 1 empty, 0 otherwise
 */
int msu_fdzcq_empty(msu_fdzcq_handle_t q);

/**
 * whether queue is full or not
 *
 * @param q the handle of fdzcq
 * @return 1 full, 0 otherwise
 */
int msu_fdzcq_full(msu_fdzcq_handle_t q);

/**
 * Add a reference to fdbuf.
 *
 * @param q the handle of the fdzcq
 * @param fdb the msu_fdbut_t pointer
 */
void msu_fdbuf_ref(msu_fdzcq_handle_t q, msu_fdbuf_t *fdb);

/**
 * Subtract a reference to fdbuf. If refcount reaches 0, free buf function is called.
 *
 * @param q the handle of the fdzcq
 * @param fdb the msu_fdbut_t pointer
 */
void msu_fdbuf_unref(msu_fdzcq_handle_t q, msu_fdbuf_t *fdb);

/**
 * If the fd in fdbuf is dmabuf, lock it before access.
 *
 * @param q the handle of fdzcq
 * @param fdb the msu_fdbut_t pointer
 */
void msu_fdbuf_dmabuf_lock(msu_fdzcq_handle_t q, msu_fdbuf_t *fdb);

/**
 * If the fd in fdbuf is dmabuf, unlock it after access.
 *
 * @param q the handle of fdzcq
 * @param fdb the msu_fdbut_t pointer
 */
void msu_fdbuf_dmabuf_unlock(msu_fdzcq_handle_t q, msu_fdbuf_t *fdb);

#ifdef __cplusplus
}
#endif

#endif //MISCUTIL_FDZCQ_H