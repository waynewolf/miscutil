#include <unistd.h>
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <locale.h>
#include <glib.h>
#include "fdzcq.h"

static void test_fdzcq_create_and_destroy()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(10, NULL);
    g_assert_nonnull(q);

    msu_fdzcq_destroy(q);

    q = msu_fdzcq_create(10, NULL);
    g_assert_nonnull(q);

    {
        /* consumer in the same process, acquire and release */
        msu_fdzcq_handle_t q2 = msu_fdzcq_acquire(NULL);
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
        msu_fdzcq_handle_t q3 = msu_fdzcq_acquire(NULL);
        g_assert_nonnull(q3);

        msu_fdzcq_release(q3);
        exit(0);
    }
}

static void test_fdzcq_size_of_empty_queue()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(10, NULL);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 0);
    g_assert_true(msu_fdzcq_empty(q));
    g_assert_false(msu_fdzcq_full(q));

    msu_fdzcq_destroy(q);
}

static void test_fdzcq_register_and_deregister_consumer()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(8, NULL);

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
    msu_fdzcq_handle_t q = msu_fdzcq_create(10, NULL);

    for (int i = 1; i < 100; i++) {
        g_assert_cmpint(msu_fdzcq_produce(q, i), ==, MSU_FDZCQ_STATUS_OK);
    }

    msu_fdzcq_destroy(q);
}

static void test_fdzcq_sp_produce_without_consume_but_with_consumer()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(10, NULL);

    msu_fdzcq_register_consumer(q);

    for (int i = 1; i < 100; i++) {
        g_assert_cmpint(msu_fdzcq_produce(q, i), ==, MSU_FDZCQ_STATUS_OK);
    }

    msu_fdzcq_destroy(q);
}

static void test_fdzcq_sp_produce_and_consume()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(3, NULL);

    int consumer_id = msu_fdzcq_register_consumer(q);

    int data = 1;

    {
        /* 1. produce 1, consume 1 */
        g_assert_true(msu_fdzcq_produce(q, data) == MSU_FDZCQ_STATUS_OK);

        g_assert_cmpint(msu_fdzcq_size(q), ==, 1);
        g_assert_false(msu_fdzcq_empty(q));
        g_assert_false(msu_fdzcq_full(q));

        msu_fdbuf_t *fdbuf = NULL;
        g_assert_true(msu_fdzcq_consume(q, consumer_id, &fdbuf) == MSU_FDZCQ_STATUS_OK);

        g_assert_cmpint(fdbuf->fd, ==, data);

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
        g_assert_true(msu_fdzcq_consume(q, consumer_id, &fdbuf2) == MSU_FDZCQ_STATUS_OK);

        g_assert_cmpint(fdbuf2->fd, ==, data2);

        g_assert_cmpint(msu_fdzcq_size(q), ==, 1);
    }

    msu_fdzcq_destroy(q);
}

static void test_fdzcq_sp_produce_fast_and_consume_slow()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(4, NULL);

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
    g_assert_true(msu_fdzcq_consume(q, consumer_id, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(fdbuf->fd, ==, 7);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    msu_fdzcq_destroy(q);

    g_assert_true(TRUE);
}

static void test_fdzcq_sp_produce_slow_and_multiple_consume()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(4, NULL);

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

    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    /* buffer will only disappear after all consumers fetched buffer */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 0);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf) == MSU_FDZCQ_STATUS_NO_BUF);

    /* produce 2, consume 4 in 4 different consumers */
    int data1 = 1, data2 = 2;
    g_assert_true(msu_fdzcq_produce(q, data1) == MSU_FDZCQ_STATUS_OK);

    g_assert_true(msu_fdzcq_produce(q, data2) == MSU_FDZCQ_STATUS_OK);

    /* fetch the first buffer */
    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    /* all consumer fetched "data1“， buffer count-- */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    /* fetch the second buffer */
    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    /* two buffers are fetched */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 0);

    /* no buf */
    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf) == MSU_FDZCQ_STATUS_NO_BUF);

    /* produce 4, consume 4 in 4 different consumer */
    g_assert_true(msu_fdzcq_produce(q, 1) == MSU_FDZCQ_STATUS_OK);
    g_assert_true(msu_fdzcq_produce(q, 2) == MSU_FDZCQ_STATUS_OK);
    g_assert_true(msu_fdzcq_produce(q, 3) == MSU_FDZCQ_STATUS_OK);

    /* overwrite data 1, cause we have only 3 (4-1) meaningful buffers */
    g_assert_true(msu_fdzcq_produce(q, 4) == MSU_FDZCQ_STATUS_OK);

    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    /* first 3 consumers fetched all the buffers, the fourth is slow, no buffer has been fetched */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 3);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 0);

    g_assert_true(msu_fdzcq_consume(q, consumer_id4, &fdbuf) == MSU_FDZCQ_STATUS_NO_BUF);

    msu_fdzcq_destroy(q);

    g_assert_true(TRUE);
}

static void test_fdzcq_sp_produce_and_multiple_consumer_join_in_the_middle()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(4, NULL);

    msu_fdbuf_t *fdbuf = NULL;

    g_assert_true(msu_fdzcq_produce(q, 1) == MSU_FDZCQ_STATUS_OK);

    int consumer_id1 = msu_fdzcq_register_consumer(q);

    g_assert_true(msu_fdzcq_produce(q, 2) == MSU_FDZCQ_STATUS_OK);

    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 1);
    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    int consumer_id2 = msu_fdzcq_register_consumer(q);

    g_assert_true(msu_fdzcq_produce(q, 3) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    /*  consumer 2 has not yet fetched buffer， size is not changed */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    /* consumer 1,2 have fetched buffer "producer #1"， so size - 1 */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    int consumer_id3 = msu_fdzcq_register_consumer(q);
    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 3);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 0);

    msu_fdzcq_destroy(q);
}

static void test_fdzcq_sp_produce_and_multiple_consumer_register_and_deregister()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(4, NULL);
    msu_fdbuf_t *fdbuf = NULL;

    g_assert_true(msu_fdzcq_produce(q, 1) == MSU_FDZCQ_STATUS_OK);

    int consumer_id1 = msu_fdzcq_register_consumer(q);

    g_assert_true(msu_fdzcq_produce(q, 2) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id1, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 1);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    msu_fdzcq_deregister_consumer(q, consumer_id1);

    g_assert_true(msu_fdzcq_produce(q, 3) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    int consumer_id2 = msu_fdzcq_register_consumer(q);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 2);

    /* only one consumer， so buf -1 after consume */
    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    int consumer_id3 = msu_fdzcq_register_consumer(q);

    g_assert_true(msu_fdzcq_produce(q, 4) == MSU_FDZCQ_STATUS_OK);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 2);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 3);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 3);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 1);

    g_assert_true(msu_fdzcq_consume(q, consumer_id2, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 4);

    g_assert_true(msu_fdzcq_consume(q, consumer_id3, &fdbuf) == MSU_FDZCQ_STATUS_OK);
    g_assert_cmpint(fdbuf->fd, ==, 4);

    g_assert_cmpint(msu_fdzcq_size(q), ==, 0);

    msu_fdzcq_deregister_consumer(q, consumer_id2);
    msu_fdzcq_deregister_consumer(q, consumer_id3);

    int consumers[MSU_FDZCQ_MAX_CONSUMER];
    g_assert_cmpint(msu_fdzcq_enumerate_consumers(q, consumers), ==, 0);

    msu_fdzcq_destroy(q);
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

    return g_test_run();
}
