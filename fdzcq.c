#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <semaphore.h>
#include <sys/ioctl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <linux/dma-buf.h>

#include "fdzcq.h"

/* head in shm */
#pragma pack(push)
#pragma pack(1)
typedef struct msu_fdzcq_shm_head_s {
    sem_t           q_sem;                                          /* offset 0 in the mmap paged aligned memory */
    uint8_t         capacity;                                       /* max nr of items in queue */
    uint8_t         wr_off;                                         /* producer write ptr */
    uint8_t         rd_off;                                         /* global read ptr */
    uint8_t         rd_off_local[MSU_FDZCQ_MAX_CONSUMER];           /* local read ptr */

    int             consumer[MSU_FDZCQ_MAX_CONSUMER];               /* consumer flag, -1 means "not exist" */
    int             consumer_id_seq_no;
} msu_fdzcq_shm_head_t;
#pragma pack(pop)

/* control structure in each process */
typedef struct msu_fdzcq_s {
    void                       *shm_data;                           /* the data in shm, including head */
    int                         shm_fd;
    int                         map_len;
    msu_fdbuf_release_func_t    fdbuf_free_cb;                      /* callback to free fd */
    int                         consumer[MSU_FDZCQ_MAX_CONSUMER];   /* consumers for this q instance */
    int                         is_producer;                        /* producer or consumer */
    int                         sock;                               /* producer: listen sock, consumer: data sock */
    int                         quit_server;                        /* flag to quit producer socket server */
    void                       *user_data;                          /* opaque data, no touch, just pass around */
} *msu_fdzcq_handle_t;

#define PRODUCER_SERVER_SOCK                "/tmp/fdzcq.sock"

#define MSU_FDZCQ_SHM_HEAD_SIZE             sizeof(struct msu_fdzcq_shm_head_s)
#define MSU_FDZCQ_SHM_HEAD_PTR(Q)           ((msu_fdzcq_shm_head_t *)((Q)->shm_data))
#define MSU_FDZCQ_SHM_DATA_PTR(Q)           ((msu_fdbuf_t *)((uint8_t *)((Q)->shm_data) + MSU_FDZCQ_SHM_HEAD_SIZE))
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


static void fdbuf_free_func(msu_fdzcq_handle_t q, msu_fdbuf_t *fdbuf);
static int msu_fdzcq_find_consumer_index(msu_fdzcq_handle_t q, int consumer_id);
static int msu_fdzcq_compare_read_speed2(msu_fdzcq_handle_t q, int consumer_index);
static int msu_fdzcq_local_buf_empty(msu_fdzcq_handle_t q, int consumer_id);
static int msu_fdzcq_local_buf_full(msu_fdzcq_handle_t q, int consumer_id);
static int msu_fdzcq_compare_read_speed(msu_fdzcq_handle_t q, int consumer_id);
static uint8_t msu_fdzcq_slowest_rd_off(msu_fdzcq_handle_t q);
static int connect_with_timeout(int sock, struct sockaddr_un *addr, struct timeval *timeout);
static int get_fd_from_producer_locked(msu_fdzcq_handle_t q, uint8_t offset);
static ssize_t sock_fd_read(int sock, void *buf, ssize_t bufsize, int *fd);
static ssize_t sock_fd_write(int sock, void *buf, ssize_t buflen, int fd);
static ssize_t consumer_block_sock_sendn(int sock, void *buf, ssize_t bufsize);
static ssize_t consumer_block_sock_readn(int sock, void *buf, ssize_t bufsize);


msu_fdzcq_handle_t msu_fdzcq_create(uint8_t capacity, msu_fdbuf_release_func_t free_cb, void *user_data)
{
    assert(capacity > 0);

    msu_fdzcq_handle_t q = (msu_fdzcq_handle_t)malloc(sizeof(struct msu_fdzcq_s));
    if (!q) {
        printf("Failed to allocate fdzcq handle\n");
        return NULL;
    }

    q->is_producer = 1;
    q->quit_server = 0;

    q->sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (q->sock == -1) {
        printf("Failed to create socket: %s\n", strerror(errno));
        free(q);
        return NULL;
    }

    int reuse_addr = 1;
    if (setsockopt(q->sock, SOL_SOCKET, SO_REUSEADDR, (int *)&reuse_addr, sizeof(reuse_addr)) < 0) {
        printf("Failed to set reuse addr: %s\n", strerror(errno));
        close(q->sock);
        free(q);
        return NULL;
    }

    /*
     * Set socket to be non-blocking. All of the sockets for
     * the incoming connections will also be non-blocking since
     * they will inherit that state from the listening socket.
     */
    if (fcntl(q->sock, F_SETFL, O_NONBLOCK) == -1) {
        printf("Failed to set socket to non-blocking: %s\n", strerror(errno));
        close(q->sock);
        free(q);
        return NULL;
    }

    unlink(PRODUCER_SERVER_SOCK);

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(struct sockaddr_un));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, PRODUCER_SERVER_SOCK, sizeof(addr.sun_path) - 1);

    if (bind(q->sock, (struct sockaddr *)&addr, sizeof(struct sockaddr_un)) == -1) {
        printf("Failed to bind socket: %s\n", strerror(errno));
        close(q->sock);
        free(q);
        return NULL;
    }

    if (listen(q->sock, 10) == -1) {
        printf("Failed to listen socket: %s\n", strerror(errno));
        close(q->sock);
        free(q);
        return NULL;
    }

    errno = 0;
    q->shm_fd = shm_open("fdzcq", O_CREAT | O_RDWR, 0666);
    if (q->shm_fd == -1) {
        printf("Failed to open fdzcq shm: %s\n", strerror(errno));
        close(q->sock);
        free(q);
        return NULL;
    }

    memset(q->consumer, -1, sizeof(q->consumer));

    q->user_data = user_data;
    q->fdbuf_free_cb = free_cb ? free_cb : fdbuf_free_func;
    q->map_len = MSU_FDZCQ_SHM_HEAD_SIZE + capacity * sizeof(struct msu_fdbuf_s);

    if (ftruncate(q->shm_fd, q->map_len) == -1) {
        printf("ftruncate failed: %s\n", strerror(errno));
        close(q->shm_fd);
        close(q->sock);
        free(q);
        return NULL;
    }

    q->shm_data = mmap(NULL, q->map_len, PROT_READ | PROT_WRITE, MAP_SHARED, q->shm_fd, 0);
    if (q->shm_data == MAP_FAILED) {
        printf("Failed to mmap fdzcq shm: %s\n", strerror(errno));
        close(q->shm_fd);
        close(q->sock);
        free(q);
        return NULL;
    }

    memset(q->shm_data, 0, q->map_len);

    msu_fdzcq_shm_head_t *head = (msu_fdzcq_shm_head_t *)q->shm_data;
    head->capacity = capacity;

    memset(head->consumer, -1, sizeof(head->consumer));

    unsigned int init_value = 1;
    if (sem_init(&head->q_sem, 1, init_value) == -1) {
        printf("Failed to init semaphore: %s\n", strerror(errno));
        munmap(q->shm_data, q->map_len);
        close(q->shm_fd);
        close(q->sock);
        free(q);
    }

    return q;
}

void msu_fdzcq_destroy(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);

    sem_destroy(&head->q_sem);
    munmap(q->shm_data, q->map_len);
    close(q->shm_fd);
    close(q->sock);
    unlink(PRODUCER_SERVER_SOCK);

    free(q);
}

msu_fdzcq_handle_t msu_fdzcq_acquire(msu_fdbuf_release_func_t free_cb, void *user_data)
{
    msu_fdzcq_handle_t q = (msu_fdzcq_handle_t)malloc(sizeof(struct msu_fdzcq_s));
    if (!q) {
        printf("Failed to allocate fdzcq handle\n");
        return NULL;
    }

    q->is_producer = 0;
    q->sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (q->sock == -1) {
        printf("Failed to create socket: %s\n", strerror(errno));
        free(q);
        return NULL;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(struct sockaddr_un));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, PRODUCER_SERVER_SOCK, sizeof(addr.sun_path) - 1);

    struct timeval timeout;
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;
    if (connect_with_timeout(q->sock, &addr, &timeout) < 0) {
        printf("Failed to connect socket: %s\n", strerror(errno));
        close(q->sock);
        free(q);
        return NULL;
    }

    /* in consumer, we use block socket with send/recieve timeout */
    timeout.tv_sec = 0;
    timeout.tv_usec = 100 * 1000;
    if (setsockopt(q->sock, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout, sizeof(timeout)) < 0) {
        printf("setsockopt receive timeout failed: %s\n", strerror(errno));
    }
    if (setsockopt(q->sock, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout, sizeof(timeout)) < 0) {
        printf("setsockopt send timeout failed: %s\n", strerror(errno));
    }

    errno = 0;
    q->shm_fd = shm_open("fdzcq", O_RDWR, 0666);
    if (q->shm_fd == -1) {
        printf("Failed to open fdzcq shm: %s\n", strerror(errno));
        free(q);
        return NULL;
    }

    struct stat sb;
    if(fstat(q->shm_fd, &sb) == -1) {
        printf("Failed to stat shm fd: %s\n", strerror(errno));
        close(q->shm_fd);
        free(q);
        return NULL;
    }

    memset(q->consumer, -1, sizeof(q->consumer));

    q->user_data = user_data;
    q->fdbuf_free_cb = free_cb ? free_cb : fdbuf_free_func;

    /* support int length only */
    q->map_len = (int)sb.st_size;

    q->shm_data = (uint8_t *)mmap(NULL, q->map_len, PROT_READ | PROT_WRITE, MAP_SHARED, q->shm_fd, 0);
    if (q->shm_data == MAP_FAILED) {
        printf("Failed to mmap fdzcq shm: %s\n", strerror(errno));
        close(q->shm_fd);
        free(q);
        return NULL;
    }

    return q;
}

/* consumer release does not touch data in shm */
void msu_fdzcq_release(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    for (int i = 0; i < MSU_FDZCQ_MAX_CONSUMER; i++) {
        if (q->consumer[i] != -1) {
            msu_fdzcq_deregister_consumer(q, q->consumer[i]);
        }
    }

    munmap(q->shm_data, q->map_len);
    close(q->shm_fd);
    close(q->sock);

    free(q);
}

int msu_fdzcq_register_consumer(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);

    sem_wait(&head->q_sem);

    int consumer_id = head->consumer_id_seq_no++;

    int found_empty_slot = 0;
    for (int i = 0; i < MSU_FDZCQ_MAX_CONSUMER; i++) {
        if (head->consumer[i] == -1) {
            head->consumer[i] = consumer_id;
            q->consumer[i] = consumer_id;
            head->rd_off_local[i] = head->rd_off;
            found_empty_slot = 1;
            break;
        }
    }

    sem_post(&head->q_sem);

    return found_empty_slot ? consumer_id : -1;
}

void msu_fdzcq_deregister_consumer(msu_fdzcq_handle_t q, int consumer_id)
{
    assert(q != NULL);
    assert(consumer_id != -1);

    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);

    sem_wait(&head->q_sem);

    for (int i = 0; i < MSU_FDZCQ_MAX_CONSUMER; i++) {
        if (head->consumer[i] == consumer_id) {
            q->consumer[i] = -1;
            head->consumer[i] = -1;
            break;
        }
    }

    sem_post(&head->q_sem);
}

int msu_fdzcq_enumerate_consumers(msu_fdzcq_handle_t q, int consumer[MSU_FDZCQ_MAX_CONSUMER])
{
    assert(q != NULL);

    int count = 0;

    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);

    sem_wait(&head->q_sem);

    for (int i = 0; i < MSU_FDZCQ_MAX_CONSUMER; i++) {
        if (head->consumer[i] != -1) {
            consumer[count++] = head->consumer[i];
        }
    }

    sem_post(&head->q_sem);

    return count;
}

msu_fdzcq_status_t msu_fdzcq_produce(msu_fdzcq_handle_t q, int fd)
{
    assert(q != NULL);
    assert(fd > 0);

    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);
    msu_fdbuf_t *bufs = MSU_FDZCQ_SHM_DATA_PTR(q);

    sem_wait(&head->q_sem);

    bufs[head->wr_off].fd = fd;
    bufs[head->wr_off].ref_count = 0;

    if (MSU_FDZCQ_IS_GLOBAL_FULL(head)) {
        sem_post(&head->q_sem);
        msu_fdbuf_t *next_buf = &bufs[NEXT_OFFSET(head, head->wr_off)];
        msu_fdbuf_unref(q, next_buf);
        sem_wait(&head->q_sem);
    }

    /* update write ptr */
    ADVANCE_WR_OFF(head);

    /*
     * update write ptr may lead to equal write and read ptr, which means the queue is empty,
     * so we need to update read ptr accordingly. In this case, consumer will miss a buffer
     */
    if (head->rd_off == head->wr_off) {
        ADVANCE_GLOBAL_RD_OFFSET(head);
    }

    /* update local read ptr as well */
    for (int i = 0; i < MSU_FDZCQ_MAX_CONSUMER; i++) {
        if (head->consumer[i] != -1 && head->rd_off_local[i] == head->wr_off) {
            ADVANCE_LOCAL_RD_OFFSET(head, i);
        }
    }

    sem_post(&head->q_sem);

    return MSU_FDZCQ_STATUS_OK;
}

int msu_fdzcq_producer_has_data(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    struct timeval timeout;
    fd_set read_set;

    FD_ZERO(&read_set);
    FD_SET(q->sock, &read_set);

    timeout.tv_sec  = 1;
    timeout.tv_usec = 0;

    int max_sock = q->sock;
    int retval = select(max_sock + 1, &read_set, NULL, NULL, NULL);

    while (1) {
        if (retval == -1) {
            printf("select failed: %s\n", strerror(errno));
            return 0;
        }

        if (retval == 0) {
            printf("No data within timeout period\n");
            return 0;
        }

        for (int i = 0; i <= max_sock; i++) {
            if (FD_ISSET(i, &read_set)) {
                if (i == q->sock) {
                    int client_sock = accept(q->sock, NULL, NULL);
                    if (client_sock < 0) {
                        if (errno != EWOULDBLOCK) {
                            printf("accept() failed: %s\n", strerror(errno));
                        }
                        return 0;
                    }

                    FD_SET(client_sock, &read_set);
                    max_sock = client_sock > max_sock ? client_sock : max_sock;
                } else {
                    /* client socket, leave it to be processed by msu_fdzcq_producer_handle_data() */
                    FD_CLR(i, &read_set);
                    return i;
                }
            }
        }

        retval = select(max_sock + 1, &read_set, NULL, NULL, &timeout);
    }

    /* should not reach here */
    return 0;
}

void msu_fdzcq_producer_handle_data(msu_fdzcq_handle_t q, int client_sock, uint8_t *buf, size_t max_len)
{
    assert(max_len > 0);

    msu_fdbuf_t *bufs = MSU_FDZCQ_SHM_DATA_PTR(q);

    uint8_t offset = 0;
    ssize_t ssize = consumer_block_sock_readn(client_sock, &offset, sizeof(offset));

    if (ssize != sizeof(offset)) {
        printf("Invalid packet from consumer\n");
        return;
    }

    /* do not lock here, consumer already hold the semaphore */
    int fd = bufs[offset].fd;

    sock_fd_write(client_sock, buf, max_len, fd);
}

void msu_fdzcq_producer_run(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    uint8_t buf[1024];

    while (!q->quit_server) {
        int client_sock = msu_fdzcq_producer_has_data(q);
        if (client_sock > 0) {
            msu_fdzcq_producer_handle_data(q, client_sock, buf, 1024);
        }
    }
}

void msu_fdzcq_producer_quit(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    q->quit_server = 1;
}

/* consume will add a reference to fdbuf */
msu_fdzcq_status_t msu_fdzcq_consume(msu_fdzcq_handle_t q, int consumer_id, msu_fdbuf_t **fdbuf, int *fd)
{
    assert(q != NULL);
    assert(consumer_id != -1);
    assert(fdbuf != NULL);

    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);
    msu_fdbuf_t *bufs = MSU_FDZCQ_SHM_DATA_PTR(q);

    sem_wait(&head->q_sem);

    int consumer_index = msu_fdzcq_find_consumer_index(q, consumer_id);

    if (consumer_index == -1) {
        printf("Consumer %d not registered", consumer_id);
        sem_post(&head->q_sem);
        return MSU_FDZCQ_STATUS_CONSUMER_NOT_FOUND;
    }

    if (MSU_FDZCQ_IS_LOCAL_EMPTY(head, consumer_index)) {
        printf("Consume empty queue for consumer_index: %d\n", consumer_index);
        sem_post(&head->q_sem);
        return MSU_FDZCQ_STATUS_NO_BUF;
    }

    uint8_t rd_off_local = head->rd_off_local[consumer_index];
    if (fd != NULL) {
        *fd = get_fd_from_producer_locked(q, rd_off_local);
    }

    bufs[rd_off_local].ref_count++;
    *fdbuf = &bufs[rd_off_local];

    ADVANCE_LOCAL_RD_OFFSET(head, consumer_index);

    /* calculate the number of local read ptrs which are faster than global read ptr */
    int consumer_count = 0;
    int fast_consumer_count = 0;
    for (int i = 0; i < MSU_FDZCQ_MAX_CONSUMER; i++) {
        if (head->consumer[i] != -1) {
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
        head->rd_off = msu_fdzcq_slowest_rd_off(q);

        if (head->rd_off == MSU_FDZCQ_INVALID_OFF) {
            printf("Invalid offset, should not happen if consumer registered\n");
        }
    }

    sem_post(&head->q_sem);

    return MSU_FDZCQ_STATUS_OK;
}

/* should be called inside lock */
static int msu_fdzcq_find_consumer_index(msu_fdzcq_handle_t q, int consumer_id)
{
    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);

    int idx;
    for (idx = 0; idx < MSU_FDZCQ_MAX_CONSUMER; idx++) {
        if (head->consumer[idx] == consumer_id)
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
    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);

    assert(CONSUMER_EXISTS(head, consumer_index));

    if (head->rd_off == head->rd_off_local[consumer_index]) {
        return 0;
    }

    int diff1 = (int)head->rd_off - (int)head->wr_off;
    int diff2 = (int)head->rd_off_local[consumer_index] - (int)head->wr_off;

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
    return mul * ( (int)head->rd_off - (int)head->rd_off_local[consumer_index] );
}

int msu_fdzcq_size(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);

    sem_wait(&head->q_sem);
    int sz = MSU_FDZCQ_BUF_SIZE(head);
    sem_post(&head->q_sem);

    return sz;
}

int msu_fdzcq_empty(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);

    sem_wait(&head->q_sem);
    int empty = MSU_FDZCQ_IS_GLOBAL_EMPTY(head);
    sem_post(&head->q_sem);

    return empty;
}

int msu_fdzcq_full(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);

    sem_wait(&head->q_sem);
    int full = MSU_FDZCQ_IS_GLOBAL_FULL(head);
    sem_post(&head->q_sem);

    return full;
}

static int msu_fdzcq_local_buf_empty(msu_fdzcq_handle_t q, int consumer_id)
{
    assert(q != NULL);

    int idx = msu_fdzcq_find_consumer_index(q, consumer_id);

    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);

    return MSU_FDZCQ_IS_LOCAL_EMPTY(head, idx);
}

static int msu_fdzcq_local_buf_full(msu_fdzcq_handle_t q, int consumer_id)
{
    assert(q != NULL);

    int idx = msu_fdzcq_find_consumer_index(q, consumer_id);

    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);

    return MSU_FDZCQ_IS_LOCAL_FULL(head, idx);
}

static int msu_fdzcq_compare_read_speed(msu_fdzcq_handle_t q, int consumer_id)
{
    assert(q != NULL);

    int idx = msu_fdzcq_find_consumer_index(q, consumer_id);

    return msu_fdzcq_compare_read_speed2(q, idx);
}

static uint8_t msu_fdzcq_slowest_rd_off(msu_fdzcq_handle_t q)
{
    assert(q != NULL);

    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);

    /* to the right of wr_off，the nearest one to wr_off is the slowest consumer */

    uint8_t ret = MSU_FDZCQ_INVALID_OFF;
    int min_diff = MSU_FDZCQ_MAX_CONSUMER + 1;
    for (int i = 0; i < MSU_FDZCQ_MAX_CONSUMER; i++) {

        if (head->consumer[i] != -1) {
            int diff = (int)head->rd_off_local[i] - (int)head->wr_off;

            /* diff == 0， headdata all fetched， speed is the fastest， we use a big value for compensation */
            if (diff == 0) {
                diff = MSU_FDZCQ_MAX_CONSUMER;
            } else if (diff < 0) {
                diff += head->capacity;
            }

            if (diff < min_diff) {
                min_diff = diff;
                ret = head->rd_off_local[i];
            }
        }
    }

    return ret;
}

void msu_fdbuf_ref(msu_fdzcq_handle_t q, msu_fdbuf_t *fdb)
{
    assert(q != NULL);
    assert(fdb != NULL);

    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);

    sem_wait(&head->q_sem);

    fdb->ref_count++;

    sem_post(&head->q_sem);
}

void msu_fdbuf_unref(msu_fdzcq_handle_t q, msu_fdbuf_t *fdb)
{
    assert(q != NULL);
    assert(fdb != NULL);

    msu_fdzcq_shm_head_t *head = MSU_FDZCQ_SHM_HEAD_PTR(q);

    sem_wait(&head->q_sem);

    if (q->is_producer) {
        if (fdb->ref_count < 0) {
            printf("Producer release buffer twice is not allowed\n");
            goto out;
        }
        fdb->ref_count--;
        if (fdb->ref_count == 0 || fdb->ref_count == -1) {
            if (q->fdbuf_free_cb) {
                q->fdbuf_free_cb(q, fdb);
            }
            if (fdb->ref_count == 0) {
                fdb->ref_count--; /* make sure buf free function do not called twice */
            }
        } else {
            printf("Impossible refcount detected, shouldn't happen\n");
        }
    } else {
        fdb->ref_count--;
        if (fdb->ref_count == 0) {
            if (q->fdbuf_free_cb) {
                q->fdbuf_free_cb(q, fdb);
            }
        } else if (fdb->ref_count < 0) {
            printf("Consumer release buffer twice is not allowed\n");
        }
    }

    out:
    sem_post(&head->q_sem);
}

void msu_fdbuf_dmabuf_lock(msu_fdzcq_handle_t q, msu_fdbuf_t *fdb)
{
    assert(q != NULL);
    assert(fdb != NULL);

    struct dma_buf_sync sync = { 0 };

    sync.flags = DMA_BUF_SYNC_RW | DMA_BUF_SYNC_START;
    ioctl(fdb->fd, DMA_BUF_IOCTL_SYNC, &sync);
}

void msu_fdbuf_dmabuf_unlock(msu_fdzcq_handle_t q, msu_fdbuf_t *fdb)
{
    assert(q != NULL);
    assert(fdb != NULL);

    struct dma_buf_sync sync = { 0 };

    sync.flags = DMA_BUF_SYNC_RW | DMA_BUF_SYNC_END;
    ioctl(fdb->fd, DMA_BUF_IOCTL_SYNC, &sync);
}

static void fdbuf_free_func(msu_fdzcq_handle_t q, msu_fdbuf_t *fdbuf)
{
    //printf("Freeing buf...\n");
}

static int connect_with_timeout(int sock, struct sockaddr_un *addr, struct timeval *timeout)
{
    long arg;

    if((arg = fcntl(sock, F_GETFL, NULL)) < 0) {
        printf("fcntl F_GETFL failed: %s\n", strerror(errno));
        return -1;
    }
    if(fcntl(sock, F_SETFL, arg | O_NONBLOCK) < 0) {
        printf("fcntl F_SETFL failed: %s\n", strerror(errno));
        return -1;
    }

    int res = connect(sock, (struct sockaddr *)addr, sizeof(struct sockaddr_un));
    if (res < 0) {
        if (errno == EINPROGRESS) {
            do {
                fd_set write_set;
                FD_ZERO(&write_set);
                FD_SET(sock, &write_set);
                res = select(sock + 1, NULL, &write_set, NULL, timeout);

                if (res < 0 && errno != EINTR) {
                    printf("Error connecting %d - %s\n", errno, strerror(errno));
                    return -1;
                } else if (res > 0) {
                    socklen_t lon = sizeof(int);
                    int valopt;
                    if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &valopt, &lon) < 0) {
                        printf("getsockopt failed: %s\n", strerror(errno));
                        return -1;
                    }
                    if (valopt) {
                        printf("Error in delayed connection() %d - %s\n", valopt, strerror(valopt));
                        return -1;
                    }
                    break;
                } else {
                    printf("Timeout in select() - Cancelling!\n");
                    return -1;
                }
            } while (1);
        } else {
            printf("connection failed: %s\n", strerror(errno));
            return -1;
        }
    }

    if (fcntl(sock, F_SETFL, arg) < 0) {
        printf("fcntl F_SETFL failed: %s\n", strerror(errno));
        return -1;
    }

    return 0;
}

static int get_fd_from_producer_locked(msu_fdzcq_handle_t q, uint8_t offset)
{
    consumer_block_sock_sendn(q->sock, &offset, 1);

    uint8_t nouse;
    int fd = -1;
    ssize_t size = sock_fd_read(q->sock, &nouse, sizeof(nouse), &fd);

    if (size <= 0) {
        return -1;
    }

    return fd;
}

/*
 * send exact n bytes to the socket, except timeout
 *
 * return:
 *  -1              : error
 *  >=0, < bufsize  : timeout
 *  bufsize         : success
 */
static ssize_t consumer_block_sock_sendn(int sock, void *buf, ssize_t bufsize)
{
    ssize_t sent = 0;

    while (sent < bufsize) {
        ssize_t n = send(sock, (uint8_t *)buf + sent, bufsize - sent, 0);
        if (n == -1) {
            if (errno == EINTR) {
                continue;
            } else if (errno == EWOULDBLOCK) {
                /* timeout, return sent */
                break;
            }
            return -1;
        } else {
            sent += n;
        }
    }

    return sent;
}

/*
 * read exact n bytes to the socket, except timeout.
 *
 * return:
 *  -1              : error
 *  >=0, < bufsize  : timeout
 *  bufsize         : success
 */
static ssize_t consumer_block_sock_readn(int sock, void *buf, ssize_t bufsize)
{
    ssize_t nread = 0;

    while (nread < bufsize) {
        ssize_t n = read(sock, (uint8_t *)buf + nread, bufsize - nread);
        if (n == -1) {
            if (errno == EINTR) {
                continue;
            } else if (errno == EWOULDBLOCK) {
                /* timeout, return nread */
                break;
            }
            return -1;
        } else {
            nread += n;
        }
    }

    return nread;
}

static ssize_t sock_fd_read(int sock, void *buf, ssize_t bufsize, int *fd)
{
    ssize_t     size;

    if (fd) {
        struct msghdr   msg;
        struct iovec    iov;
        union {
            struct cmsghdr  cmsghdr;
            char            control[CMSG_SPACE(sizeof(int))];
        } cmsgu;
        struct cmsghdr     *cmsg;

        iov.iov_base        = buf;
        iov.iov_len         = bufsize;

        msg.msg_name        = NULL;
        msg.msg_namelen     = 0;
        msg.msg_iov         = &iov;
        msg.msg_iovlen      = 1;
        msg.msg_control     = cmsgu.control;
        msg.msg_controllen  = sizeof(cmsgu.control);

        size = recvmsg(sock, &msg, 0);
        if (size < 0) {
            printf("recvmsg failed: %s\n", strerror(errno));
            return -1;
        }

        cmsg = CMSG_FIRSTHDR(&msg);
        if (cmsg && cmsg->cmsg_len == CMSG_LEN(sizeof(int))) {
            if (cmsg->cmsg_level != SOL_SOCKET) {
                printf("invalid cmsg_level %d\n", cmsg->cmsg_level);
                return -1;
            }
            if (cmsg->cmsg_type != SCM_RIGHTS) {
                printf("invalid cmsg_type %d\n", cmsg->cmsg_type);
                return -1;
            }

            *fd = *((int *)CMSG_DATA(cmsg));
        } else {
            *fd = -1;
        }
    } else {
        size = read(sock, buf, bufsize);
        if (size < 0) {
            printf("read failed: %s\n", strerror(errno));
            return -1;
        }
    }

    return size;
}

/* if fd == -1, do not transfer fd */
static ssize_t sock_fd_write(int sock, void *buf, ssize_t buflen, int fd)
{
    ssize_t         size;
    struct msghdr   msg;
    struct iovec    iov;
    union {
        struct cmsghdr  cmsghdr;
        char            control[CMSG_SPACE(sizeof(int))];
    } cmsgu;
    struct cmsghdr  *cmsg;

    iov.iov_base    = buf;
    iov.iov_len     = buflen;

    msg.msg_name    = NULL;
    msg.msg_namelen = 0;
    msg.msg_iov     = &iov;
    msg.msg_iovlen  = 1;

    if (fd != -1) {
        msg.msg_control     = cmsgu.control;
        msg.msg_controllen  = sizeof(cmsgu.control);

        cmsg                = CMSG_FIRSTHDR(&msg);
        cmsg->cmsg_len      = CMSG_LEN(sizeof(int));
        cmsg->cmsg_level    = SOL_SOCKET;
        cmsg->cmsg_type     = SCM_RIGHTS;

        *((int *) CMSG_DATA(cmsg)) = fd;
    } else {
        msg.msg_control     = NULL;
        msg.msg_controllen  = 0;
        //printf("not passing fd\n");
    }

    size = sendmsg(sock, &msg, 0);

    if (size < 0) {
        printf("sendmsg failed: %s\n", strerror(errno));
    }

    return size;
}
