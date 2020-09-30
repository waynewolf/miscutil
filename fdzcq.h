/**
 * FDZCQ is fd based zero copy queue
 *
 * FDZCQ is fit for dmabuf based camera buffer transfer across processes.
 *
 * The consumer will always get the latest meaningful buffer. If a consumer is running slow, the buffer is gone.
 * A buffer may be consumed multiple times by different consumers.
 *
 * FDZCQ is actually an SPMC (single producer, multiple consumer) queue.
 * The best usage scenario is using FDZCQ to connect producer and consumers across processes.
 */
#ifndef MISCUTIL_FDZCQ_H
#define MISCUTIL_FDZCQ_H

#include <stdint.h>

#define MSU_FDZCQ_MAX_CONSUMER          8

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
    int         fd;
    uint8_t     ref_count;          /* zero means slot empty */
} msu_fdbuf_t;

/**
 * producer create fdzcq
 *
 * @param capacity maximum nr of items in fdzcq
 * @return the handle of fdzcq
 */
msu_fdzcq_handle_t msu_fdzcq_create(uint8_t capacity);

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
msu_fdzcq_handle_t msu_fdzcq_acquire();

/**
 * consumer releases fdzcq
 *
 * @param q the handle of fdzcq
 */
void msu_fdzcq_release(msu_fdzcq_handle_t q);

/**
 * consumer release fdzcq
 *
 * @param q the handle of fdzcq
 */
void msu_fdzcq_put(msu_fdzcq_handle_t q);

/**
 * register consumer
 *
 * @param q the handle of fdzcq
 * @return -1 failed, >0 the consumer id
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

msu_fdzcq_status_t msu_fdzcq_produce(msu_fdzcq_handle_t q, int fd);

msu_fdzcq_status_t msu_fdzcq_consume(msu_fdzcq_handle_t q, int consumer_id, msu_fdbuf_t **fdbuf);

int msu_fdzcq_buf_size(msu_fdzcq_handle_t q);

int msu_fdzcq_buf_empty(msu_fdzcq_handle_t q);

int msu_fdzcq_buf_full(msu_fdzcq_handle_t q);

int msu_fdzcq_local_buf_empty(msu_fdzcq_handle_t q, int consumer_id);

int msu_fdzcq_local_buf_full(msu_fdzcq_handle_t q, int consumer_id);

int msu_fdzcq_compare_read_speed(msu_fdzcq_handle_t q, int consumer_id);

uint8_t msu_fdzcq_slowest_rd_off(msu_fdzcq_handle_t q);

msu_fdbuf_t * msu_fdbuf_new(msu_fdzcq_handle_t q, int fd);

void msu_fdbuf_ref(msu_fdbuf_t *fdb);

void msu_fdbuf_unref(msu_fdbuf_t *fdb);

#ifdef __cplusplus
}
#endif

#endif //MISCUTIL_FDZCQ_H