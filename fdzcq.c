#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>

#include "fdzcq.h"


typedef struct msu_fdzcq_s {
    uint8_t         capacity;                                   /* max nr of items in queue */

    msu_fdbuf_t    *fdbufs;                                     /* the fdbuf array in shm */
    uint8_t         wr_off;                                     /* producer write ptr */
    uint8_t         rd_off;                                     /* global read ptr */
    uint8_t         rd_off_local[MSU_FDZCQ_MAX_CONSUMER];       /* local read ptr */

    int             consumer[MSU_FDZCQ_MAX_CONSUMER];           /* consumer flag, -1 means "not exist" */
    int             consumer_id_seq_no;

    int             shm_fd;
    int             map_len;
    sem_t          *q_sem;                                      /* the semaphore to protect whole q in shm */
} *msu_fdzcq_handle_t;

#define MSU_FDZCQ_INVALID_OFF               0xFF

#define MSU_FDZCQ_BUF_SIZE(H)               ( ((H)->wr_off + (H)->capacity - (H)->rd_off) % ((H)->capacity) )
#define MSU_FDZCQ_IS_GLOBAL_EMPTY(H)        ( (H)->wr_off == (H)->rd_off )
#define MSU_FDZCQ_IS_GLOBAL_FULL(H)         ( ((H)->wr_off + 1) % (H)->capacity == (H)->rd_off )
#define MSU_FDZCQ_IS_LOCAL_EMPTY(H, I)      ( (H)->wr_off == (H)->rd_off_local[(I)] )
#define MSU_FDZCQ_IS_LOCAL_FULL(H, I)       ( ((H)->wr_off + 1) % (H)->capacity == (H)->rd_off_local[(I)] )

#define NEXT_OFFSET(H, OFF)                 ( ((OFF) + 1) % (H)->capacity )
#define ADVANCE_WR_OFF(H)                   ( (H)->wr_off = ((H)->wr_off + 1) % (H)->capacity )
#define ADVANCE_GLOBAL_RD_OFFSET(H)         ( (H)->rd_off = ((H)->rd_off + 1) % (H)->capacity )
#define ADVANCE_LOCAL_RD_OFFSET(H, I)       ( (H)->rd_off_local[(I)] = ((H)->rd_off_local[(I)] + 1) % (H)->capacity )

#define CONSUMER_EXISTS(H, I)               ( (H)->consumer[(I)] != -1 )


static int msu_fdzcq_find_consumer_index(msu_fdzcq_handle_t q, int consumer_id);
static int msu_fdzcq_compare_read_speed2(msu_fdzcq_handle_t q, int consumer_index);


msu_fdzcq_handle_t msu_fdzcq_create(uint8_t capacity)
{
    assert(capacity > 0);

    msu_fdzcq_handle_t q = (msu_fdzcq_handle_t)malloc(sizeof(struct msu_fdzcq_s));
    if (!q) {
        printf("Failed to allocate fdzcq handle\n");
        return NULL;
    }

    q->capacity = capacity;
    q->wr_off = 0;
    q->rd_off = 0;
    q->consumer_id_seq_no = 0;

    memset(q->rd_off_local, 0, sizeof(q->rd_off_local));
    memset(q->consumer, -1, sizeof(q->consumer));

    errno = 0;
    q->shm_fd = shm_open("fdzcq", O_CREAT | O_RDWR, 0666);
    if (q->shm_fd == -1) {
        printf("Failed to open fdzcq shm: %s\n", strerror(errno));
        free(q);
        return NULL;
    }

    q->map_len = q->capacity * sizeof(struct msu_fdbuf_s);

    if (ftruncate(q->shm_fd, q->map_len) == -1) {
        printf("ftruncate failed: %s\n", strerror(errno));
        free(q);
        return NULL;
    }

    q->fdbufs = mmap(NULL, q->map_len, PROT_READ | PROT_WRITE, MAP_SHARED, q->shm_fd, 0);
    if (q->fdbufs == MAP_FAILED) {
        printf("Failed to mmap fdzcq shm: %s\n", strerror(errno));
        close(q->shm_fd);
        free(q);
        return NULL;
    }

    memset(q->fdbufs, 0, q->map_len);

    unsigned int init_value = 1;
    q->q_sem = sem_open("fdzcq", O_CREAT | O_RDWR, 0666, init_value);
    if (q->q_sem == SEM_FAILED) {
        printf("Failed to create semaphore fdzcq: %s\n", strerror(errno));
        munmap(q->fdbufs, q->map_len);
        close(q->shm_fd);
        free(q);
    }

    return q;
}

void msu_fdzcq_destroy(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    msu_fdzcq_release(q);

    sem_unlink("fdzcq");
}

msu_fdzcq_handle_t msu_fdzcq_acquire()
{
    msu_fdzcq_handle_t q = (msu_fdzcq_handle_t)malloc(sizeof(struct msu_fdzcq_s));
    if (!q) {
        printf("Failed to allocate fdzcq handle\n");
        return NULL;
    }

    q->capacity = capacity;
    q->wr_off = 0;
    q->rd_off = 0;
    q->consumer_id_seq_no = 0;

    memset(q->rd_off_local, 0, sizeof(q->rd_off_local));
    memset(q->consumer, -1, sizeof(q->consumer));

    errno = 0;
    q->shm_fd = shm_open("fdzcq", O_CREAT | O_RDWR, 0666);
    if (q->shm_fd == -1) {
        printf("Failed to open fdzcq shm: %s\n", strerror(errno));
        free(q);
        return NULL;
    }

    q->map_len = q->capacity * sizeof(struct msu_fdbuf_s);

    if (ftruncate(q->shm_fd, q->map_len) == -1) {
        printf("ftruncate failed: %s\n", strerror(errno));
        free(q);
        return NULL;
    }

    q->fdbufs = mmap(NULL, q->map_len, PROT_READ | PROT_WRITE, MAP_SHARED, q->shm_fd, 0);
    if (q->fdbufs == MAP_FAILED) {
        printf("Failed to mmap fdzcq shm: %s\n", strerror(errno));
        close(q->shm_fd);
        free(q);
        return NULL;
    }

    memset(q->fdbufs, 0, q->map_len);

    unsigned int init_value = 1;
    q->q_sem = sem_open("fdzcq", O_CREAT | O_RDWR, 0666, init_value);
    if (q->q_sem == SEM_FAILED) {
        printf("Failed to create semaphore fdzcq: %s\n", strerror(errno));
        munmap(q->fdbufs, q->map_len);
        close(q->shm_fd);
        free(q);
    }

    return q;
}

void msu_fdzcq_release(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    sem_close(q->q_sem);

    munmap(q->fdbufs, q->map_len);

    close(q->shm_fd);

    free(q);
}

int msu_fdzcq_register_consumer(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    sem_wait(q->q_sem);

    int consumer_id = q->consumer_id_seq_no++;

    int found_empty_slot = 0;
    for (int i = 0; i < MSU_FDZCQ_MAX_CONSUMER; i++) {
        if (q->consumer[i] == -1) {
            q->consumer[i] = consumer_id;
            q->rd_off_local[i] = q->rd_off;
            found_empty_slot = 1;
            break;
        }
    }

    sem_post(q->q_sem);

    return found_empty_slot ? consumer_id : -1;
}

void msu_fdzcq_deregister_consumer(msu_fdzcq_handle_t q, int consumer_id)
{
    assert(q != NULL);
    assert(consumer_id != -1);

    sem_wait(q->q_sem);

    for (int i = 0; i < MSU_FDZCQ_MAX_CONSUMER; i++) {
        if (q->consumer[i] == consumer_id) {
            q->consumer[i] = -1;
            break;
        }
    }

    sem_post(q->q_sem);
}

int msu_fdzcq_enumerate_consumers(msu_fdzcq_handle_t q, int consumer[MSU_FDZCQ_MAX_CONSUMER])
{
    assert(q != NULL);

    int count = 0;

    sem_wait(q->q_sem);

    for (int i = 0; i < MSU_FDZCQ_MAX_CONSUMER; i++) {
        if (q->consumer[i] != -1) {
            consumer[count++] = q->consumer[i];
        }
    }

    sem_post(q->q_sem);

    return count;
}

msu_fdzcq_status_t msu_fdzcq_produce(msu_fdzcq_handle_t q, int fd)
{
    assert(q != NULL);
    assert(fd > 0);

    sem_wait(q->q_sem);

    /* enqueue */
    q->fdbufs[q->wr_off].fd = fd;
    q->fdbufs[q->wr_off].ref_count = 0;

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
    for (int i = 0; i < MSU_FDZCQ_MAX_CONSUMER; i++) {
        if (q->consumer[i] != -1 && q->rd_off_local[i] == q->wr_off) {
            ADVANCE_LOCAL_RD_OFFSET(q, i);
        }
    }

    sem_post(q->q_sem);

    return MSU_FDZCQ_STATUS_OK;
}

/* consume will add a reference to fdbuf */
msu_fdzcq_status_t msu_fdzcq_consume(msu_fdzcq_handle_t q, int consumer_id, msu_fdbuf_t **fdbuf)
{
    assert(q != NULL);
    assert(consumer_id != -1);
    assert(fdbuf != NULL);

    sem_wait(q->q_sem);

    int consumer_index = msu_fdzcq_find_consumer_index(q, consumer_id);

    if (consumer_index == -1) {
        printf("Consumer %d not registered", consumer_id);
        sem_post(q->q_sem);
        return MSU_FDZCQ_STATUS_CONSUMER_NOT_FOUND;
    }

    if (MSU_FDZCQ_IS_LOCAL_EMPTY(q, consumer_index)) {
        printf("Empty queue for consumer_index: %d\n", consumer_index);
        sem_post(q->q_sem);
        return MSU_FDZCQ_STATUS_NO_BUF;
    }

    uint8_t rd_off_local = q->rd_off_local[consumer_index];

    q->fdbufs[rd_off_local].ref_count++;
    *fdbuf = &q->fdbufs[rd_off_local];

    ADVANCE_LOCAL_RD_OFFSET(q, consumer_index);

    /* calculate the number of local read ptrs which are faster than global read ptr */
    int consumer_count = 0;
    int fast_consumer_count = 0;
    for (int i = 0; i < MSU_FDZCQ_MAX_CONSUMER; i++) {
        if (q->consumer[i] != -1) {
            consumer_count++;
            if (msu_fdzcq_compare_read_speed2(q, i) < 0) {
                fast_consumer_count++;
            }
        }
    }

    /*
     * if all local read ptr is faster than global read ptr,
     * update global read ptr to the read ptr of the slowest consumer
     */
    if (fast_consumer_count == consumer_count && fast_consumer_count > 0) {
        q->rd_off = msu_fdzcq_slowest_rd_off(q);

        if (q->rd_off == MSU_FDZCQ_INVALID_OFF) {
            printf("Invalid offset, should not happen if consumer registered\n");
        }
    }

    sem_post(q->q_sem);

    return MSU_FDZCQ_STATUS_OK;
}

/* should be called inside lock */
static int msu_fdzcq_find_consumer_index(msu_fdzcq_handle_t q, int consumer_id)
{
    int idx;
    for (idx = 0; idx < MSU_FDZCQ_MAX_CONSUMER; idx++) {
        if (q->consumer[idx] == consumer_id)
            break;
    }

    if (idx == MSU_FDZCQ_MAX_CONSUMER) {
        printf("No consumer_id %d found\n", consumer_id);
        idx = -1;
    }

    return idx;
}

/*
 * compare the speed of global read offset and the read offset of a consumer.
 * return <0, global is slow; =0, equal; >0, global is faster
 */
static int msu_fdzcq_compare_read_speed2(msu_fdzcq_handle_t q, int consumer_index)
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

int msu_fdzcq_buf_size(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    sem_wait(q->q_sem);
    int sz = MSU_FDZCQ_BUF_SIZE(q);
    sem_post(q->q_sem);

    return sz;
}

int msu_fdzcq_buf_empty(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    sem_wait(q->q_sem);
    int empty = MSU_FDZCQ_IS_GLOBAL_EMPTY(q);
    sem_post(q->q_sem);

    return empty;
}

int msu_fdzcq_buf_full(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    sem_wait(q->q_sem);
    int full = MSU_FDZCQ_IS_GLOBAL_FULL(q);
    sem_post(q->q_sem);

    return full;
}

int msu_fdzcq_local_buf_empty(msu_fdzcq_handle_t q, int consumer_id)
{
    assert(q != NULL);

    int idx = msu_fdzcq_find_consumer_index(q, consumer_id);

    return MSU_FDZCQ_IS_LOCAL_EMPTY(q, idx);
}

int msu_fdzcq_local_buf_full(msu_fdzcq_handle_t q, int consumer_id)
{
    assert(q != NULL);

    int idx = msu_fdzcq_find_consumer_index(q, consumer_id);

    return MSU_FDZCQ_IS_LOCAL_FULL(q, idx);
}

int msu_fdzcq_compare_read_speed(msu_fdzcq_handle_t q, int consumer_id)
{
    assert(q != NULL);

    int idx = msu_fdzcq_find_consumer_index(q, consumer_id);

    return msu_fdzcq_compare_read_speed2(q, idx);
}

uint8_t msu_fdzcq_slowest_rd_off(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    /* to the right of wr_off，the nearest one to wr_off is the slowest consumer */

    uint8_t ret = MSU_FDZCQ_INVALID_OFF;
    int min_diff = MSU_FDZCQ_MAX_CONSUMER + 1;
    for (int i = 0; i < MSU_FDZCQ_MAX_CONSUMER; i++) {

        if (q->consumer[i] != -1) {
            int diff = (int)q->rd_off_local[i] - (int)q->wr_off;

            /* diff == 0， data all fetched， speed is the fastest， we use a big value for compensation */
            if (diff == 0) {
                diff = MSU_FDZCQ_MAX_CONSUMER;
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

msu_fdbuf_t * msu_fdbuf_new(msu_fdzcq_handle_t q, int fd)
{
    assert(fd > 0);

    msu_fdbuf_t *fdbuf = (msu_fdbuf_t *)calloc(1, sizeof(struct msu_fdbuf_s));
    if (!fdbuf) {
        printf("Failed to allocate fdbuf\n");
        return NULL;
    }

    fdbuf->fd = fd;
    msu_fdbuf_ref(fdbuf);

    return fdbuf;
}

void msu_fdbuf_ref(msu_fdbuf_t *fdb)
{
    assert(fdb != NULL);

    fdb->ref_count++;
}

static void fdbuf_release(msu_fdbuf_t *fdb)
{
    assert(fdb != NULL);
}

void msu_fdbuf_unref(msu_fdbuf_t *fdb)
{
    assert(fdb != NULL);

    if (fdb->ref_count == 0) {
        printf("Cannot unref 0 ref_count object\n");
        return;
    }

    fdb->ref_count--;
    if (fdb->ref_count == 0) {
        free(fdb);
    }
}
