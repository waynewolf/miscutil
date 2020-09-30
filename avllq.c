#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include "avllq.h"

typedef struct msu_avllq_s {
    msu_avllq_item_t   *buf_array;                                  /* all buf_item */
    uint8_t             wr_off;                                     /* producer write ptr */
    uint8_t             rd_off;                                     /* global read ptr */
    uint8_t             rd_off_local[MSU_AVLLQ_MAX_CONSUMER];       /* local read ptr */
    uint8_t             capacity;                                   /* how many items in queue, NOT the total bytes */
    int                 consumer[MSU_AVLLQ_MAX_CONSUMER];           /* consumer flag, -1 means "not exist" */
    int                 consumer_id_seq_no;
    int                 max_item_size;
    void              **preserved_buf;                              /* pre-allocated buffer */
    pthread_mutex_t     mutex;                                      /* struct mutex */
} *msu_avllq_handle_t;


#define MSU_AVLLQ_BUF_SIZE(H)              ( ((H)->wr_off + (H)->capacity - (H)->rd_off) % ((H)->capacity) )
#define MSU_AVLLQ_IS_GLOBAL_EMPTY(H)       ( (H)->wr_off == (H)->rd_off )
#define MSU_AVLLQ_IS_GLOBAL_FULL(H)        ( ((H)->wr_off + 1) % (H)->capacity == (H)->rd_off )
#define MSU_AVLLQ_IS_LOCAL_EMPTY(H, I)     ( (H)->wr_off == (H)->rd_off_local[(I)] )
#define MSU_AVLLQ_IS_LOCAL_FULL(H, I)      ( ((H)->wr_off + 1) % (H)->capacity == (H)->rd_off_local[(I)] )

#define NEXT_OFFSET(H, OFF)             ( ((OFF) + 1) % (H)->capacity )
#define ADVANCE_WR_OFF(H)               ( (H)->wr_off = ((H)->wr_off + 1) % (H)->capacity )
#define ADVANCE_GLOBAL_RD_OFFSET(H)     ( (H)->rd_off = ((H)->rd_off + 1) % (H)->capacity )
#define ADVANCE_LOCAL_RD_OFFSET(H, I)   ( (H)->rd_off_local[(I)] = ((H)->rd_off_local[(I)] + 1) % (H)->capacity )


#define CONSUMER_EXISTS(H, I)           ( (H)->consumer[(I)] != -1 )

static int msu_avllq_find_consumer_index(msu_avllq_handle_t q, int consumer_id);
static int msu_avllq_compare_read_speed2(msu_avllq_handle_t q, int consumer_index);

msu_avllq_handle_t msu_avllq_create(uint8_t capacity, int max_item_size)
{
    assert(capacity >= MSU_AVLLQ_MIN_CAPACITY && max_item_size > 0);

    if (capacity < MSU_AVLLQ_MIN_CAPACITY || capacity > MSU_AVLLQ_MAX_CAPACITY) {
        printf("Illegal msu_avllq capacity: %d\n", capacity);
        return NULL;
    }

    msu_avllq_handle_t q = (msu_avllq_handle_t)malloc(sizeof(struct msu_avllq_s));
    if (!q) {
        printf("Failed to alloc msu_avllq\n");
        return NULL;
    }

    q->buf_array = (msu_avllq_item_t *)malloc(capacity * sizeof(msu_avllq_item_t));
    if (!q->buf_array) {
        free(q);
        printf("Failed to alloc %d bufs in msu_avllq\n", capacity);
        return NULL;
    }

    q->preserved_buf = (void *)malloc(capacity * sizeof(void *));
    if (!q->preserved_buf) {
        free(q);
        printf("Failed to alloc preserved buf\n");
        return NULL;
    }

    for (int i = 0; i < capacity; i++) {
        q->preserved_buf[i] = malloc(max_item_size);
        if (!q->preserved_buf[i]) {
            free(q);
            printf("Failed to allocate preserved_buf[%d]\n", i);
            return NULL;
        }
    }

    q->wr_off = 0;
    q->rd_off = 0;
    q->capacity = capacity;
    q->consumer_id_seq_no = 0;
    q->max_item_size = max_item_size;

    memset(q->rd_off_local, 0, sizeof(q->rd_off_local));
    memset(q->consumer, -1, sizeof(q->consumer));

    pthread_mutex_init(&q->mutex, NULL);

    return q;
}

void msu_avllq_destroy(msu_avllq_handle_t q)
{
    assert(q != NULL);

    free(q->buf_array);

    if (q->preserved_buf && q->max_item_size > 0) {
        for (int i = 0; i < q->capacity; i++) {
            free(q->preserved_buf[i]);
        }
        free(q->preserved_buf);
    }

    pthread_mutex_destroy(&q->mutex);

    free(q);
}

int msu_avllq_register_consumer(msu_avllq_handle_t q)
{
    assert(q != NULL);

    pthread_mutex_lock(&q->mutex);

    int consumer_id = q->consumer_id_seq_no++;

    int found_empty_slot = 0;
    for (int i = 0; i < MSU_AVLLQ_MAX_CONSUMER; i++) {
        if (q->consumer[i] == -1) {
            q->consumer[i] = consumer_id;
            q->rd_off_local[i] = q->rd_off;
            found_empty_slot = 1;
            break;
        }
    }

    pthread_mutex_unlock(&q->mutex);

    return found_empty_slot ? consumer_id : -1;
}

void msu_avllq_deregister_consumer(msu_avllq_handle_t q, int consumer_id)
{
    assert(q != NULL);
    assert(consumer_id != -1);

    pthread_mutex_lock(&q->mutex);

    for (int i = 0; i < MSU_AVLLQ_MAX_CONSUMER; i++) {
        if (q->consumer[i] == consumer_id) {
            q->consumer[i] = -1;
            break;
        }
    }

    pthread_mutex_unlock(&q->mutex);
}

int msu_avllq_enumerate_consumers(msu_avllq_handle_t q, int consumer[MSU_AVLLQ_MAX_CONSUMER])
{
    assert(q != NULL);

    int count = 0;

    pthread_mutex_lock(&q->mutex);

    for (int i = 0; i < MSU_AVLLQ_MAX_CONSUMER; i++) {
        if (q->consumer[i] != -1) {
            consumer[count++] = q->consumer[i];
        }
    }

    pthread_mutex_unlock(&q->mutex);

    return count;
}

msu_avllq_status_t msu_avllq_produce(msu_avllq_handle_t q, const msu_avllq_item_t *item)
{
    assert(q != NULL);
    assert(item != NULL);

    return msu_avllq_produce2(q, item->data, item->len, item->type);
}

msu_avllq_status_t msu_avllq_produce2(msu_avllq_handle_t q, const void *data, size_t len, int type)
{
    assert(q != NULL);
    assert(data != NULL);
    assert(len > 0);

    void *new_data = NULL;

    pthread_mutex_lock(&q->mutex);

    new_data = q->preserved_buf[q->wr_off];

    /* enqueue */
    q->buf_array[q->wr_off].data = new_data;
    memcpy(q->buf_array[q->wr_off].data, data, len);
    q->buf_array[q->wr_off].len = len;
    q->buf_array[q->wr_off].type = type;

    /* update write ptr */
    ADVANCE_WR_OFF(q);

    /*
     * update write ptr may lead to equal write and read ptr, which means the queue is empty,
     * so we need to update read ptr accordingly. In this case, consumer will miss a buffer
     */
    if (q->rd_off == q->wr_off) {
        ADVANCE_GLOBAL_RD_OFFSET(q);
    }

    /* update local read ptr as well */
    for (int i = 0; i < MSU_AVLLQ_MAX_CONSUMER; i++) {
        if (q->consumer[i] != -1 && q->rd_off_local[i] == q->wr_off) {
            ADVANCE_LOCAL_RD_OFFSET(q, i);
        }
    }

    pthread_mutex_unlock(&q->mutex);

    return MSU_AVLLQ_STATUS_OK;
}

msu_avllq_status_t msu_avllq_consume(msu_avllq_handle_t q, int consumer_id, msu_avllq_item_t *item)
{
    assert(q != NULL);
    assert(consumer_id != -1);
    assert(item != NULL);

    pthread_mutex_lock(&q->mutex);

    int consumer_index = msu_avllq_find_consumer_index(q, consumer_id);

    if (consumer_index == -1) {
        printf("Consumer %d not registered", consumer_id);
        pthread_mutex_unlock(&q->mutex);
        return MSU_AVLLQ_STATUS_CONSUMER_NOT_FOUND;
    }

    if (MSU_AVLLQ_IS_LOCAL_EMPTY(q, consumer_index)) {
        printf("Empty queue for consumer_index: %d\n", consumer_index);
        pthread_mutex_unlock(&q->mutex);
        return MSU_AVLLQ_STATUS_NO_BUF;
    }

    uint8_t rd_off_local = q->rd_off_local[consumer_index];

    void *out_data = malloc(q->buf_array[rd_off_local].len);
    if (!out_data) {
        printf("Failed to alloc memory for output consume data\n");
        pthread_mutex_unlock(&q->mutex);
        return MSU_AVLLQ_STATUS_MEMORY_ERR;
    }

    item->type = q->buf_array[rd_off_local].type;
    item->len = q->buf_array[rd_off_local].len;
    item->data = out_data;
    memcpy(item->data, q->buf_array[rd_off_local].data, item->len);

    ADVANCE_LOCAL_RD_OFFSET(q, consumer_index);

    /* calculate the number of local read ptrs which are faster than global read ptr */
    int consumer_count = 0;
    int fast_consumer_count = 0;
    for (int i = 0; i < MSU_AVLLQ_MAX_CONSUMER; i++) {
        if (q->consumer[i] != -1) {
            consumer_count++;
            if (msu_avllq_compare_read_speed2(q, i) < 0) {
                fast_consumer_count++;
            }
        }
    }

    /*
     * if all local read ptr is faster than global read ptr,
     * update global read ptr to the read ptr of the slowest consumer
     */
    if (fast_consumer_count == consumer_count && fast_consumer_count > 0) {
        q->rd_off = msu_avllq_slowest_rd_off(q);

        if (q->rd_off == MSU_AVLLQ_INVALID_OFF) {
            printf("Invalid offset, should not happen if consumer registered\n");
        }
    }

    pthread_mutex_unlock(&q->mutex);

    return MSU_AVLLQ_STATUS_OK;
}

void msu_avllq_item_release(msu_avllq_item_t const* item)
{
    assert(item != NULL);

    free(item->data);
}

/* should be called inside lock */
static int msu_avllq_find_consumer_index(msu_avllq_handle_t q, int consumer_id)
{
    int idx;
    for (idx = 0; idx < MSU_AVLLQ_MAX_CONSUMER; idx++) {
        if (q->consumer[idx] == consumer_id)
            break;
    }

    if (idx == MSU_AVLLQ_MAX_CONSUMER) {
        printf("No consumer_id %d found\n", consumer_id);
        idx = -1;
    }

    return idx;
}

/*
 * compare the speed of global read offset and the read offset of a consumer.
 * return <0, global is slow; =0, equal; >0, global is faster
 */
static int msu_avllq_compare_read_speed2(msu_avllq_handle_t q, int consumer_index)
{
    assert(CONSUMER_EXISTS(q, consumer_index));

    if (q->rd_off == q->rd_off_local[consumer_index]) {
        return 0;
    }

    int diff1 = (int)q->rd_off - (int)q->wr_off;
    int diff2 = (int)q->rd_off_local[consumer_index] - (int)q->wr_off;

    if (diff1 == 0) {
        if (diff2 != 0) {
            /*
             * Queue empty, but local read ptr is not pointing to wr_off, should not happen.
             * According to definition, global read ptr is slowest, it can only advance after
             * all global read has completed.
             */
            printf("Whole ring buffer is empty, but local read pointer is ahead, not allowed!\n");
        }

        return 0;
    }

    /*
     * Unread buffer exists globally，but local read ptr and global write ptr is equal,
     * which means that the consumer has completed fetching buffer, global is slower.
     */
    if (diff2 == 0) {
        return -1;
    }

    /* Now, mul is either 1 or -1 */
    int mul = diff1 * diff2;

    /*
     * the natual growing direction is to the right, so:
     * mul < 0, wr_off in the middle, left is faster; mul > 0, in the same side of wr_off, right is faster
     */
    return mul * ( (int)q->rd_off - (int)q->rd_off_local[consumer_index] );
}

int msu_avllq_buf_size(msu_avllq_handle_t q)
{
    assert(q != NULL);

    pthread_mutex_lock(&q->mutex);
    int sz = MSU_AVLLQ_BUF_SIZE(q);
    pthread_mutex_unlock(&q->mutex);

    return sz;
}

int msu_avllq_buf_empty(msu_avllq_handle_t q)
{
    assert(q != NULL);

    pthread_mutex_lock(&q->mutex);
    int empty = MSU_AVLLQ_IS_GLOBAL_EMPTY(q);
    pthread_mutex_unlock(&q->mutex);

    return empty;
}

int msu_avllq_buf_full(msu_avllq_handle_t q)
{
    assert(q != NULL);

    pthread_mutex_lock(&q->mutex);
    int full = MSU_AVLLQ_IS_GLOBAL_FULL(q);
    pthread_mutex_unlock(&q->mutex);

    return full;
}

int msu_avllq_local_buf_empty(msu_avllq_handle_t q, int consumer_id)
{
    assert(q != NULL);

    int idx = msu_avllq_find_consumer_index(q, consumer_id);

    return MSU_AVLLQ_IS_LOCAL_EMPTY(q, idx);
}

int msu_avllq_local_buf_full(msu_avllq_handle_t q, int consumer_id)
{
    assert(q != NULL);

    int idx = msu_avllq_find_consumer_index(q, consumer_id);

    return MSU_AVLLQ_IS_LOCAL_FULL(q, idx);
}

int msu_avllq_compare_read_speed(msu_avllq_handle_t q, int consumer_id)
{
    assert(q != NULL);

    int idx = msu_avllq_find_consumer_index(q, consumer_id);

    return msu_avllq_compare_read_speed2(q, idx);
}

uint8_t msu_avllq_slowest_rd_off(msu_avllq_handle_t q)
{
    assert(q != NULL);

    /* to the right of wr_off，the nearest one to wr_off is the slowest consumer */

    uint8_t ret = MSU_AVLLQ_INVALID_OFF;
    int min_diff = MSU_AVLLQ_MAX_CONSUMER + 1;
    for (int i = 0; i < MSU_AVLLQ_MAX_CONSUMER; i++) {

        if (q->consumer[i] != -1) {
            int diff = (int)q->rd_off_local[i] - (int)q->wr_off;

            /* diff == 0， data all fetched， speed is the fastest， we use a big value for compensation */
            if (diff == 0) {
                diff = MSU_AVLLQ_MAX_CONSUMER;
            } else if (diff < 0) {
                diff += q->capacity;
            }

            if (diff < min_diff) {
                min_diff = diff;
                ret = q->rd_off_local[i];
            }
        }
    }

    return ret;
}
