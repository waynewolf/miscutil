#define _GNU_SOURCE
#include <unistd.h>
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <locale.h>
#include <assert.h>
#include <semaphore.h>
#include <sys/mman.h>
#include <glib.h>
#include "fdzcq.h"

static void test_fdzcq_create_and_destroy()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(10, NULL, NULL);
    g_assert_nonnull(q);

    msu_fdzcq_destroy(q);

    q = msu_fdzcq_create(10, NULL, NULL);
    g_assert_nonnull(q);

    {
        /* consumer in the same process, acquire and release */
        msu_fdzcq_handle_t q2 = msu_fdzcq_acquire(NULL, NULL);
        g_assert_nonnull(q2);

        msu_fdzcq_release(q2);
    }

    pid_t pid = fork();
    if (pid > 0) {
        int ret = waitpid(pid, NULL, 0);
        g_assert(ret > 0);

        msu_fdzcq_destroy(q);
    } else if (pid == 0) {
        /* consumer in the different process, acquire and release */
        msu_fdzcq_handle_t q3 = msu_fdzcq_acquire(NULL, NULL);
        g_assert_nonnull(q3);

        msu_fdzcq_release(q3);
        exit(0);
    }
}

static void test_fdzcq_size_of_empty_queue()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(10, NULL, NULL);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 0);
    g_assert_true(msu_fdzcq_empty(q));
    g_assert_false(msu_fdzcq_full(q));

    msu_fdzcq_destroy(q);
}

static void test_fdzcq_register_and_deregister_consumer()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(8, NULL, NULL);

    int consumer_id1 = msu_fdzcq_register_consumer(q);
    g_assert_true(consumer_id1 >= 0);

    int consumer_id2 = msu_fdzcq_register_consumer(q);
    g_assert_true(consumer_id2 >= 0);

    int consumer_id3 = msu_fdzcq_register_consumer(q);
    g_assert_true(consumer_id3 >= 0);

    g_assert_true(consumer_id1 != consumer_id2);
    g_assert_true(consumer_id1 != consumer_id3);
    g_assert_true(consumer_id2 != consumer_id3);

    msu_fdzcq_deregister_consumer(q, consumer_id1);

    int consumer[MSU_FDZCQ_MAX_CONSUMER];
    g_assert_cmpint(msu_fdzcq_enumerate_consumers(q, consumer), ==, 2);

    /* 2 consumers left */

    int consumer_id4 = msu_fdzcq_register_consumer(q);
    g_assert_true(consumer_id4 >= 0);

    int consumer_id5 = msu_fdzcq_register_consumer(q);
    g_assert_true(consumer_id5 >= 0);

    g_assert_true(consumer_id4 != consumer_id5);

    g_assert_cmpint(msu_fdzcq_enumerate_consumers(q, consumer), ==, 4);

    /* already reach the maximum consumer limit */
    int consumer_id6 = msu_fdzcq_register_consumer(q);
    g_assert_cmpint(consumer_id6, ==, -1);

    msu_fdzcq_destroy(q);
}

static void test_fdzcq_sp_produce_without_consume()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(10, NULL, NULL);

    for (int i = 1; i < 100; i++) {
        g_assert_cmpint(msu_fdzcq_produce(q, i), ==, MSU_FDZCQ_STATUS_OK);
    }

    msu_fdzcq_destroy(q);
}

static void test_fdzcq_sp_produce_without_consume_but_with_consumer()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(10, NULL, NULL);

    msu_fdzcq_register_consumer(q);

    for (int i = 1; i < 100; i++) {
        g_assert_cmpint(msu_fdzcq_produce(q, i), ==, MSU_FDZCQ_STATUS_OK);
    }

    msu_fdzcq_destroy(q);
}

static void test_fdzcq_sp_produce_and_consume()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(3, NULL, NULL);

    int consumer_id = msu_fdzcq_register_consumer(q);

    int data = 1;

    {
        /* 1. produce 1, consume 1 */
        g_assert_true(msu_fdzcq_produce(q, data) == MSU_FDZCQ_STATUS_OK);

        g_assert_cmpint(msu_fdzcq_size(q), ==, 1);
        g_assert_false(msu_fdzcq_empty(q));
        g_assert_false(msu_fdzcq_full(q));

        msu_fdbuf_t *fdbuf = NULL;
        g_assert_true(msu_fdzcq_consume(q, consumer_id, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);

        g_assert_cmpint(fdbuf->fd, ==, data);

        msu_fdbuf_unref(q, fdbuf);

        g_assert_cmpint(msu_fdzcq_size(q), ==, 0);

        g_assert_true(msu_fdzcq_empty(q));
        g_assert_false(msu_fdzcq_full(q));
    }

    {
        /* 2. produce 2, consume 1 */
        int data2 = 2;
        int data3 = 3;

        g_assert_true(msu_fdzcq_produce(q, data2) == MSU_FDZCQ_STATUS_OK);
        g_assert_true(msu_fdzcq_produce(q, data3) == MSU_FDZCQ_STATUS_OK);

        g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

        msu_fdbuf_t *fdbuf2 = NULL;
        g_assert_true(msu_fdzcq_consume(q, consumer_id, &fdbuf2, NULL) == MSU_FDZCQ_STATUS_OK);

        g_assert_cmpint(fdbuf2->fd, ==, data2);

        msu_fdbuf_unref(q, fdbuf2);

        g_assert_cmpint(msu_fdzcq_size(q), ==, 1);
    }

    msu_fdzcq_destroy(q);
}

static void test_fdzcq_sp_produce_fast_and_consume_slow()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(4, NULL, NULL);

    int consumer_id = msu_fdzcq_register_consumer(q);

    for (int i = 1; i < 10; i++) {
        g_assert_true(msu_fdzcq_produce(q, i) == MSU_FDZCQ_STATUS_OK);
    }

    /* one slot is empty */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 3);
    g_assert_false(msu_fdzcq_empty(q));
    g_assert_true(msu_fdzcq_full(q));

    /* 7, 8, 9 in q now */
    msu_fdbuf_t *fdbuf = NULL;
    g_assert_true(msu_fdzcq_consume(q, consumer_id, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(fdbuf->fd, ==, 7);

    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    msu_fdzcq_destroy(q);

    g_assert_true(TRUE);
}

static void test_fdzcq_sp_produce_slow_and_multiple_consume()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(4, NULL, NULL);

    int consumer_id1 = msu_fdzcq_register_consumer(q);
    int consumer_id2 = msu_fdzcq_register_consumer(q);
    int consumer_id3 = msu_fdzcq_register_consumer(q);
    int consumer_id4 = msu_fdzcq_register_consumer(q);

    g_assert_true(consumer_id1 >= 0);
    g_assert_true(consumer_id2 >= 0);
    g_assert_true(consumer_id3 >= 0);
    g_assert_true(consumer_id4 >= 0);

    msu_fdbuf_t *fdbuf = NULL;

    /* produce 1, consume 4 in 4 different consumer */
    int data = 1;

    g_assert_true(msu_fdzcq_produce(q, data) == MSU_FDZCQ_STATUS_OK);

    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    /* buffer will only disappear after all consumers fetched buffer */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 0);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf, NULL) == MSU_FDZCQ_STATUS_NO_BUF);

    /* produce 2, consume 4 in 4 different consumers */
    int data1 = 1, data2 = 2;
    g_assert_true(msu_fdzcq_produce(q, data1) == MSU_FDZCQ_STATUS_OK);

    g_assert_true(msu_fdzcq_produce(q, data2) == MSU_FDZCQ_STATUS_OK);

    /* fetch the first buffer */
    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    /* all consumer fetched "data1“， buffer count-- */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    /* fetch the second buffer */
    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    /* two buffers are fetched */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 0);

    /* no buf */
    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf, NULL) == MSU_FDZCQ_STATUS_NO_BUF);

    /* produce 4, consume 4 in 4 different consumer */
    g_assert_true(msu_fdzcq_produce(q, 1) == MSU_FDZCQ_STATUS_OK);
    g_assert_true(msu_fdzcq_produce(q, 2) == MSU_FDZCQ_STATUS_OK);
    g_assert_true(msu_fdzcq_produce(q, 3) == MSU_FDZCQ_STATUS_OK);

    /* overwrite data 1, cause we have only 3 (4-1) meaningful buffers */
    g_assert_true(msu_fdzcq_produce(q, 4) == MSU_FDZCQ_STATUS_OK);

    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    /* first 3 consumers fetched all the buffers, the fourth is slow, no buffer has been fetched */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 3);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 0);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf, NULL) == MSU_FDZCQ_STATUS_NO_BUF);

    msu_fdzcq_destroy(q);

    g_assert_true(TRUE);
}

static void test_fdzcq_sp_produce_and_multiple_consumer_join_in_the_middle()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(4, NULL, NULL);

    msu_fdbuf_t *fdbuf = NULL;

    g_assert_true(msu_fdzcq_produce(q, 1) == MSU_FDZCQ_STATUS_OK);

    int consumer_id1 = msu_fdzcq_register_consumer(q);

    g_assert_true(msu_fdzcq_produce(q, 2) == MSU_FDZCQ_STATUS_OK);

    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 1);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    int consumer_id2 = msu_fdzcq_register_consumer(q);

    g_assert_true(msu_fdzcq_produce(q, 3) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    /*  consumer 2 has not yet fetched buffer， size is not changed */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    /* consumer 1,2 have fetched buffer "producer #1"， so size - 1 */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    int consumer_id3 = msu_fdzcq_register_consumer(q);
    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 3);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 0);

    msu_fdzcq_destroy(q);
}

static void test_fdzcq_sp_produce_and_multiple_consumer_register_and_deregister()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(4, NULL, NULL);
    msu_fdbuf_t *fdbuf = NULL;

    g_assert_true(msu_fdzcq_produce(q, 1) == MSU_FDZCQ_STATUS_OK);

    int consumer_id1 = msu_fdzcq_register_consumer(q);

    g_assert_true(msu_fdzcq_produce(q, 2) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 1);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    msu_fdzcq_deregister_consumer(q, consumer_id1);

    g_assert_true(msu_fdzcq_produce(q, 3) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    int consumer_id2 = msu_fdzcq_register_consumer(q);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 2);
    msu_fdbuf_unref(q, fdbuf);

    /* only one consumer， so buf -1 after consume */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    int consumer_id3 = msu_fdzcq_register_consumer(q);

    g_assert_true(msu_fdzcq_produce(q, 4) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 3);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 3);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 4);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf, NULL) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 4);
    msu_fdbuf_unref(q, fdbuf);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 0);

    msu_fdzcq_deregister_consumer(q, consumer_id2);
    msu_fdzcq_deregister_consumer(q, consumer_id3);

    int consumers[MSU_FDZCQ_MAX_CONSUMER];
    g_assert_cmpint(msu_fdzcq_enumerate_consumers(q, consumers), ==, 0);

    msu_fdzcq_destroy(q);
}

static void test_fdzcq_mp_shm_header()
{
    pid_t pid = fork();

    if (pid > 0) {
        msu_fdzcq_handle_t q = msu_fdzcq_create(4, NULL, NULL);

        for (int i = 1; i < 4; i ++) {
            g_assert_true(msu_fdzcq_produce(q, i) == MSU_FDZCQ_STATUS_OK);
        }

        int ret = waitpid(pid, NULL, 0);
        g_assert(ret > 0);

        msu_fdzcq_destroy(q);
    } else if (pid == 0) {
        sleep(1);
        msu_fdzcq_handle_t q = msu_fdzcq_acquire(NULL, NULL);

        void *shm_addr = (void *)(*(uint64_t *)q);

        uint8_t capacity = *((uint8_t *)shm_addr + 0);
        uint8_t wr_off = *((uint8_t *)shm_addr + 1);
        uint8_t rd_off = *((uint8_t *)shm_addr + 2);

        g_assert_cmpint(capacity, ==, 4);
        g_assert_cmpint(wr_off, ==, 3);
        g_assert_cmpint(rd_off, ==, 0);

        int consumer = msu_fdzcq_register_consumer(q);

        msu_fdbuf_t *fdbuf = NULL;
        g_assert_cmpint(msu_fdzcq_consume(q, consumer, &fdbuf, NULL), ==, MSU_FDZCQ_STATUS_OK);

        capacity = *((uint8_t *)shm_addr + 0);
        wr_off = *((uint8_t *)shm_addr + 1);
        rd_off = *((uint8_t *)shm_addr + 2);

        g_assert_cmpint(capacity, ==, 4);
        g_assert_cmpint(wr_off, ==, 3);
        g_assert_cmpint(rd_off, ==, 1);

        msu_fdbuf_unref(q, fdbuf);

        msu_fdzcq_deregister_consumer(q, consumer);

        msu_fdzcq_release(q);

        exit(0);
    }
}

static void test_fdzcq_mp_produce_consume()
{
    pid_t pid = fork();

    if (pid > 0) {
        msu_fdzcq_handle_t q = msu_fdzcq_create(4, NULL, NULL);

        for (int i = 1; i < 100; i ++) {
            g_assert_true(msu_fdzcq_produce(q, i) == MSU_FDZCQ_STATUS_OK);
        }

        int ret = waitpid(pid, NULL, 0);
        g_assert(ret > 0);

        msu_fdzcq_destroy(q);
    } else if (pid == 0) {
        /* child process wait for parent process to create fdzcq */
        sleep(1);

        msu_fdzcq_handle_t q = msu_fdzcq_acquire(NULL, NULL);

        int consumer1 = msu_fdzcq_register_consumer(q);
        g_assert_true(consumer1 >= 0);

        int consumer2 = msu_fdzcq_register_consumer(q);
        g_assert_true(consumer2 >= 0);

        g_assert_true(consumer1 != consumer2);

        msu_fdbuf_t *fdbuf = NULL;
        g_assert_cmpint(msu_fdzcq_consume(q, consumer1, &fdbuf, NULL), ==, MSU_FDZCQ_STATUS_OK);
        g_assert_cmpint(fdbuf->fd, ==, 97);
        msu_fdbuf_unref(q, fdbuf);

        g_assert_cmpint(msu_fdzcq_consume(q, consumer1, &fdbuf, NULL), ==, MSU_FDZCQ_STATUS_OK);
        g_assert_cmpint(fdbuf->fd, ==, 98);
        msu_fdbuf_unref(q, fdbuf);

        g_assert_cmpint(msu_fdzcq_consume(q, consumer2, &fdbuf, NULL), ==, MSU_FDZCQ_STATUS_OK);
        g_assert_cmpint(fdbuf->fd, ==, 97);
        msu_fdbuf_unref(q, fdbuf);

        g_assert_cmpint(msu_fdzcq_consume(q, consumer1, &fdbuf, NULL), ==, MSU_FDZCQ_STATUS_OK);
        g_assert_cmpint(fdbuf->fd, ==, 99);
        msu_fdbuf_unref(q, fdbuf);

        /* 98, 99 remain in q */
        g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

        g_assert_cmpint(msu_fdzcq_consume(q, consumer2, &fdbuf, NULL), ==, MSU_FDZCQ_STATUS_OK);
        g_assert_cmpint(fdbuf->fd, ==, 98);
        msu_fdbuf_unref(q, fdbuf);

        g_assert_cmpint(msu_fdzcq_consume(q, consumer2, &fdbuf, NULL), ==, MSU_FDZCQ_STATUS_OK);
        g_assert_cmpint(fdbuf->fd, ==, 99);
        msu_fdbuf_unref(q, fdbuf);

        g_assert_true(msu_fdzcq_empty(q));

        msu_fdzcq_release(q);

        exit(0);
    }
}

static void test_fdzcq_mp_release_without_deregister()
{
    pid_t pid = fork();

    if (pid > 0) {
        msu_fdzcq_handle_t q = msu_fdzcq_create(4, NULL, NULL);

        for (int i = 1; i < 100; i ++) {
            g_assert_true(msu_fdzcq_produce(q, i) == MSU_FDZCQ_STATUS_OK);
        }

        int ret = waitpid(pid, NULL, 0);
        g_assert(ret > 0);

        msu_fdzcq_destroy(q);
    } else if (pid == 0) {
        /* child process wait for parent process to create fdzcq */
        sleep(1);

        msu_fdzcq_handle_t q = msu_fdzcq_acquire(NULL, NULL);

        int consumer1 = msu_fdzcq_register_consumer(q);
        g_assert_true(consumer1 >= 0);

        /* release without calling deregister */
        msu_fdzcq_release(q);

        q = msu_fdzcq_acquire(NULL, NULL);

        consumer1 = msu_fdzcq_register_consumer(q);
        g_assert_true(consumer1 >= 0);

        int consumer2 = msu_fdzcq_register_consumer(q);
        g_assert_true(consumer2 >= 0);

        g_assert_true(consumer1 != consumer2);

        msu_fdbuf_t *fdbuf = NULL;
        g_assert_cmpint(msu_fdzcq_consume(q, consumer1, &fdbuf, NULL), ==, MSU_FDZCQ_STATUS_OK);
        g_assert_cmpint(fdbuf->fd, ==, 97);
        msu_fdbuf_unref(q, fdbuf);

        g_assert_cmpint(msu_fdzcq_consume(q, consumer1, &fdbuf, NULL), ==, MSU_FDZCQ_STATUS_OK);
        g_assert_cmpint(fdbuf->fd, ==, 98);
        msu_fdbuf_unref(q, fdbuf);

        g_assert_cmpint(msu_fdzcq_consume(q, consumer2, &fdbuf, NULL), ==, MSU_FDZCQ_STATUS_OK);
        g_assert_cmpint(fdbuf->fd, ==, 97);
        msu_fdbuf_unref(q, fdbuf);

        g_assert_cmpint(msu_fdzcq_consume(q, consumer1, &fdbuf, NULL), ==, MSU_FDZCQ_STATUS_OK);
        g_assert_cmpint(fdbuf->fd, ==, 99);
        msu_fdbuf_unref(q, fdbuf);

        /* 98, 99 remain in q */
        g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

        g_assert_cmpint(msu_fdzcq_consume(q, consumer2, &fdbuf, NULL), ==, MSU_FDZCQ_STATUS_OK);
        g_assert_cmpint(fdbuf->fd, ==, 98);
        msu_fdbuf_unref(q, fdbuf);

        g_assert_cmpint(msu_fdzcq_consume(q, consumer2, &fdbuf, NULL), ==, MSU_FDZCQ_STATUS_OK);
        g_assert_cmpint(fdbuf->fd, ==, 99);
        msu_fdbuf_unref(q, fdbuf);

        g_assert_true(msu_fdzcq_empty(q));

        msu_fdzcq_release(q);

        exit(0);
    }
}

static void test_fdzcq_mp_release_buffer_check_refcount()
{
    pid_t pid = fork();

    if (pid > 0) {
        msu_fdzcq_handle_t q = msu_fdzcq_create(4, NULL, NULL);

        for (int i = 1; i < 4; i ++) {
            g_assert_true(msu_fdzcq_produce(q, i) == MSU_FDZCQ_STATUS_OK);
        }

        int ret = waitpid(pid, NULL, 0);
        g_assert(ret > 0);

        msu_fdzcq_destroy(q);
    } else if (pid == 0) {
        /* child process wait for parent process to create fdzcq */
        sleep(1);

        msu_fdzcq_handle_t q = msu_fdzcq_acquire(NULL, NULL);
        g_assert_nonnull(q);

        int consumer1 = msu_fdzcq_register_consumer(q);

        g_assert_true(consumer1 >= 0);

        int consumer2 = msu_fdzcq_register_consumer(q);
        g_assert_true(consumer2 >= 0);

        g_assert_true(consumer1 != consumer2);

        msu_fdbuf_t *fdbuf1 = NULL;
        msu_fdbuf_t *fdbuf2 = NULL;

        g_assert_cmpint(msu_fdzcq_consume(q, consumer1, &fdbuf1, NULL), ==, MSU_FDZCQ_STATUS_OK);
        g_assert_cmpint(fdbuf1->fd, ==, 1);
        g_assert_cmpint(fdbuf1->ref_count, ==, 1);

        g_assert_cmpint(msu_fdzcq_consume(q, consumer2, &fdbuf2, NULL), ==, MSU_FDZCQ_STATUS_OK);
        g_assert_cmpint(fdbuf2->fd, ==, 1);
        g_assert_cmpint(fdbuf2->ref_count, ==, 2);
        g_assert_cmpint(fdbuf1->ref_count, ==, 2);

        msu_fdbuf_unref(q, fdbuf2);
        g_assert_cmpint(fdbuf2->ref_count, ==, 1);
        g_assert_cmpint(fdbuf1->ref_count, ==, 1);

        msu_fdbuf_unref(q, fdbuf1);

        g_assert_cmpint(fdbuf2->ref_count, ==, 0);
        g_assert_cmpint(fdbuf1->ref_count, ==, 0);

        /* unref a buffer which has already been released */
        msu_fdbuf_unref(q, fdbuf1);
        msu_fdbuf_unref(q, fdbuf1);

        msu_fdzcq_release(q);

        exit(0);
    }
}

static int to_release_buffer;

static void producer_release_cb(msu_fdzcq_handle_t q, msu_fdbuf_t *fdbuf)
{
    g_assert_cmpint(to_release_buffer, ==, fdbuf->fd);
}

static void consumer_release_cb(msu_fdzcq_handle_t q, msu_fdbuf_t *fdbuf)
{
    printf("consumer_release_cb %d\n", fdbuf->fd);
}

static void test_fdzcq_mp_producer_release_buffer_because_of_no_consumer()
{
    pid_t pid = fork();

    if (pid > 0) {
        msu_fdzcq_handle_t q = msu_fdzcq_create(4, producer_release_cb, NULL);

        for (int i = 1; i < 4; i ++) {
            g_assert_true(msu_fdzcq_produce(q, i) == MSU_FDZCQ_STATUS_OK);
        }

        to_release_buffer = 1;
        g_assert_true(msu_fdzcq_produce(q, 4) == MSU_FDZCQ_STATUS_OK);

        to_release_buffer = 2;
        g_assert_true(msu_fdzcq_produce(q, 5) == MSU_FDZCQ_STATUS_OK);

        to_release_buffer = 3;
        g_assert_true(msu_fdzcq_produce(q, 6) == MSU_FDZCQ_STATUS_OK);

        to_release_buffer = 4;
        g_assert_true(msu_fdzcq_produce(q, 7) == MSU_FDZCQ_STATUS_OK);

        to_release_buffer = 5;
        g_assert_true(msu_fdzcq_produce(q, 8) == MSU_FDZCQ_STATUS_OK);

        /* now buffer 3, 4, 5 remain in q */
        g_assert_cmpint(msu_fdzcq_size(q), ==, 3);

        int ret = waitpid(pid, NULL, 0);
        g_assert(ret > 0);

        msu_fdzcq_destroy(q);
    } else if (pid == 0) {
        /* child process wait for parent process to create fdzcq */
        sleep(1);

        exit(0);
    }
}

static void release_memfd(msu_fdzcq_handle_t q, msu_fdbuf_t *fdbuf)
{
    g_assert_nonnull(q);
    g_assert_nonnull(fdbuf);
}

static void test_fdzcq_mp_transfer_fd_cross_process()
{
    pid_t pid = fork();

    if (pid > 0) {
        msu_fdzcq_handle_t q = msu_fdzcq_create(4, NULL, NULL);

        /* stdin, stdout, stderr, socket, shm_fd occupied, so the first opened fd is 5 */
        int fd = memfd_create("test_fdzcq_memfd", MFD_ALLOW_SEALING);
        g_assert_cmpint(fd, ==, 5);

        g_assert_false(ftruncate(fd, 10) == -1);
        char *data = (char *)mmap(NULL, 10, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        g_assert_false(data == MAP_FAILED);

        g_assert_cmpint(msu_fdzcq_produce(q, fd), ==, MSU_FDZCQ_STATUS_OK);

        memcpy(data, "0123456789",10);
        munmap(data, 10);

        uint8_t buf[1024];

        int try_count = 5;
        while (try_count--) {
            int client_sock = msu_fdzcq_producer_has_data(q);
            if (client_sock > 0) {
                msu_fdzcq_producer_handle_data(q, client_sock, buf, 1024);
                break;
            }
            usleep(1000 * 1000);
        }

        g_assert_true(try_count > 0);

        int ret = waitpid(pid, NULL, 0);
        g_assert(ret > 0);

        close(fd);
        msu_fdzcq_destroy(q);
    } else if (pid == 0) {
        /* child process wait for parent process to create fdzcq */
        sleep(1);

        msu_fdzcq_handle_t q = msu_fdzcq_acquire(release_memfd, NULL);

        int consumer_id = msu_fdzcq_register_consumer(q);
        g_assert_true(consumer_id >= 0);

        int dupfd5 = dup(0);
        int dupfd6 = dup(1);

        int fd = -1;
        msu_fdbuf_t *fdbuf = NULL;
        g_assert_cmpint(msu_fdzcq_consume(q, consumer_id, &fdbuf, &fd), ==, MSU_FDZCQ_STATUS_OK);

        g_assert_cmpint(fdbuf->fd, ==, 5);

        /* stdin, stdout, stderr, sock_fd, shm_fd, dupfd5, dupfd6 occupied */
        g_assert_cmpint(fd, ==, 7);

        char *data = (char *)mmap(NULL, 10, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        g_assert_true(data != MAP_FAILED);

        g_assert_true(memcmp(data, "0123456789", 10) == 0);

        munmap(data, 10);
        close(fd);

        close(dupfd5);
        close(dupfd6);

        msu_fdbuf_unref(q, fdbuf);

        msu_fdzcq_destroy(q);

        exit(0);
    }
}

int main(int argc, char *argv[])
{
    g_test_init(&argc, &argv, NULL);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_create_and_destroy",
                    test_fdzcq_create_and_destroy);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_size_of_empty_queue",
                    test_fdzcq_size_of_empty_queue);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_register_and_deregister_consumer",
                    test_fdzcq_register_and_deregister_consumer);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_sp_produce_without_consume",
                    test_fdzcq_sp_produce_without_consume);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_sp_produce_without_consume_but_with_consumer",
                    test_fdzcq_sp_produce_without_consume_but_with_consumer);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_sp_produce_and_consume",
                    test_fdzcq_sp_produce_and_consume);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_sp_produce_fast_and_consume_slow",
                    test_fdzcq_sp_produce_fast_and_consume_slow);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_sp_produce_slow_and_multiple_consume",
                    test_fdzcq_sp_produce_slow_and_multiple_consume);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_sp_produce_and_multiple_consumer_join_in_the_middle",
                    test_fdzcq_sp_produce_and_multiple_consumer_join_in_the_middle);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_sp_produce_and_multiple_consumer_register_and_deregister",
                    test_fdzcq_sp_produce_and_multiple_consumer_register_and_deregister);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_mp_shm_header",
                    test_fdzcq_mp_shm_header);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_mp_produce_consume",
                    test_fdzcq_mp_produce_consume);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_mp_release_without_deregister",
                    test_fdzcq_mp_release_without_deregister);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_mp_release_buffer_check_refcount",
                    test_fdzcq_mp_release_buffer_check_refcount);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_mp_producer_release_buffer_because_of_no_consumer",
                    test_fdzcq_mp_producer_release_buffer_because_of_no_consumer);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_mp_transfer_fd_cross_process",
                    test_fdzcq_mp_transfer_fd_cross_process);

    return g_test_run();
}
