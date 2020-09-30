/**
 * AVLLQ is audio/video low latency queue.
 *
 * AVLLQ is fit for AV data transfer where multiple consumers and low latency are top design priorities.
 *
 * The consumer will always get the latest meaningful buffer. If a consumer is running slow, the buffer is gone.
 * A buffer may be consumed multiple times by different consumers.
 *
 * AVLLQ is actually an SPMC (single producer, multiple consumer) queue. It doesn't support inter process
 * communication. The best usage scenario is using AVLLQ to connect producer and consumers in different threads.
 */
#ifndef MISCUTIL_AVLLQ_H
#define MISCUTIL_AVLLQ_H

#include <stdint.h>
#include <stddef.h>

#define MSU_AVLLQ_MAX_CONSUMER         4
#define MSU_AVLLQ_MAX_CAPACITY         64
#define MSU_AVLLQ_MIN_CAPACITY         2

#define MSU_AVLLQ_INVALID_OFF          0xFF

#ifdef __cplusplus
extern "C"{
#endif

typedef enum msu_avllq_status_e {
    MSU_AVLLQ_STATUS_OK,
    MSU_AVLLQ_STATUS_ERR,
    MSU_AVLLQ_STATUS_CONSUMER_NOT_FOUND,
    MSU_AVLLQ_STATUS_NO_BUF,
    MSU_AVLLQ_STATUS_MEMORY_ERR,
} msu_avllq_status_t;

typedef struct msu_avllq_item_s {
    void       *data;
    size_t      len;
    int         type;
} msu_avllq_item_t;

typedef struct msu_avllq_s *msu_avllq_handle_t;

msu_avllq_handle_t msu_avllq_create(uint8_t capacity, int max_item_size);

void msu_avllq_destroy(msu_avllq_handle_t rb);

int msu_avllq_register_consumer(msu_avllq_handle_t rb);

void msu_avllq_deregister_consumer(msu_avllq_handle_t rb, int consumer_id);

int msu_avllq_enumerate_consumers(msu_avllq_handle_t rb, int consumer_ids[MSU_AVLLQ_MAX_CONSUMER]);

msu_avllq_status_t msu_avllq_produce(msu_avllq_handle_t rb, const msu_avllq_item_t *item);

msu_avllq_status_t msu_avllq_produce2(msu_avllq_handle_t rb, const void *data, size_t len, int type);

msu_avllq_status_t msu_avllq_consume(msu_avllq_handle_t rb, int consumer_id, msu_avllq_item_t *item);

void msu_avllq_item_release(msu_avllq_item_t const *item);

int msu_avllq_buf_size(msu_avllq_handle_t rb);

int msu_avllq_buf_empty(msu_avllq_handle_t rb);

int msu_avllq_buf_full(msu_avllq_handle_t rb);

/* for unit test only */

int msu_avllq_local_buf_empty(msu_avllq_handle_t rb, int consumer_id);

int msu_avllq_local_buf_full(msu_avllq_handle_t rb, int consumer_id);

int msu_avllq_compare_read_speed(msu_avllq_handle_t rb, int consumer_id);

uint8_t msu_avllq_slowest_rd_off(msu_avllq_handle_t rb);


#ifdef __cplusplus
}
#endif

#endif //MISCUTIL_AVLLQ_H
