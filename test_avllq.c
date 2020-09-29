#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <locale.h>
#include <glib.h>
#include "avllq.h"

static void test_avllq_create_and_destroy()
{
    msu_avllq_handle_t q = msu_avllq_create(10, 1000);
    g_assert_nonnull(q);

    msu_avllq_destroy(q);

    msu_avllq_handle_t q2 = msu_avllq_create(5, 1000);
    g_assert_nonnull(q2);

    msu_avllq_destroy(q2);
}

static void test_avllq_size_of_empty_queue()
{
    msu_avllq_handle_t q = msu_avllq_create(10, 1000);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 0);
    g_assert_true(msu_avllq_buf_empty(q));
    g_assert_false(msu_avllq_buf_full(q));

    msu_avllq_destroy(q);
}

static void test_avllq_register_and_deregister_consumer()
{
    msu_avllq_handle_t q = msu_avllq_create(8, 1000);

    int consumer_id1 = msu_avllq_register_consumer(q);
    g_assert_true(consumer_id1 >= 0);

    int consumer_id2 = msu_avllq_register_consumer(q);
    g_assert_true(consumer_id2 >= 0);

    int consumer_id3 = msu_avllq_register_consumer(q);
    g_assert_true(consumer_id3 >= 0);

    g_assert_true(consumer_id1 != consumer_id2);
    g_assert_true(consumer_id1 != consumer_id3);
    g_assert_true(consumer_id2 != consumer_id3);

    msu_avllq_deregister_consumer(q, consumer_id1);

    int consumer[MSU_AVLLQ_MAX_CONSUMER];
    g_assert_cmpint(msu_avllq_enumerate_consumers(q, consumer), ==, 2);

    /* 2 consumers left */

    int consumer_id4 = msu_avllq_register_consumer(q);
    g_assert_true(consumer_id4 >= 0);

    int consumer_id5 = msu_avllq_register_consumer(q);
    g_assert_true(consumer_id5 >= 0);

    g_assert_true(consumer_id4 != consumer_id5);

    g_assert_cmpint(msu_avllq_enumerate_consumers(q, consumer), ==, 4);

    int consumer_id6 = msu_avllq_register_consumer(q);
    g_assert_true(consumer_id6 == -1);

    msu_avllq_destroy(q);
}

static void test_avllq_st_produce_without_consume()
{
    msu_avllq_handle_t q = msu_avllq_create(10, 1000);

    char data[16];
    for (int i = 0; i < 100; i++) {
        sprintf(data, "producer #%d", i);
        g_assert_cmpint(msu_avllq_produce2(q, data, strlen(data), 0), ==, MSU_AVLLQ_STATUS_OK);
    }

    msu_avllq_destroy(q);
}

static void test_avllq_st_produce_without_consume_but_with_consumer()
{
    msu_avllq_handle_t q = msu_avllq_create(10, 1000);

    msu_avllq_register_consumer(q);

    char data[16];
    for (int i = 0; i < 100; i++) {
        sprintf(data, "producer #%d", i);
        g_assert_cmpint(msu_avllq_produce2(q, data, strlen(data), 0), ==, MSU_AVLLQ_STATUS_OK);
    }

    msu_avllq_destroy(q);
}

static void test_avllq_st_produce_and_consume()
{
    msu_avllq_handle_t q = msu_avllq_create(3, 1000);

    int consumer_id = msu_avllq_register_consumer(q);

    /* 1. produce 1, consume 1 */
    const char *data = "some data";
    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 0) == MSU_AVLLQ_STATUS_OK);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);
    g_assert_false(msu_avllq_buf_empty(q));
    g_assert_false(msu_avllq_buf_full(q));

    msu_avllq_item_t item;
    g_assert_true(msu_avllq_consume(q, consumer_id, &item) == MSU_AVLLQ_STATUS_OK);

    g_assert_cmpint(item.len, ==, strlen(data));
    g_assert_cmpint(memcmp(item.data, data, item.len), ==, 0);

    msu_avllq_item_release(&item);
    g_assert_true(TRUE);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 0);

    g_assert_true(msu_avllq_buf_empty(q));
    g_assert_false(msu_avllq_buf_full(q));


    /* 2. produce 2, consume 1 */

    const char *data2 = "another data";
    const char *data3 = "third data";

    g_assert_true(msu_avllq_produce2(q, data2, strlen(data2), 0) == MSU_AVLLQ_STATUS_OK);
    g_assert_true(msu_avllq_produce2(q, data3, strlen(data3), 0) == MSU_AVLLQ_STATUS_OK);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 2);

    msu_avllq_item_t item2;
    g_assert_true(msu_avllq_consume(q, consumer_id, &item2) == MSU_AVLLQ_STATUS_OK);

    g_assert_cmpint(item2.len, ==, strlen(data2));
    g_assert_cmpint(memcmp(item2.data, data2, item2.len), ==, 0);

    msu_avllq_item_release(&item2);
    g_assert_true(TRUE);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    msu_avllq_destroy(q);
}

static void test_avllq_st_produce_fast_and_consume_slow()
{
    msu_avllq_handle_t q = msu_avllq_create(4, 1000);

    int consumer_id = msu_avllq_register_consumer(q);

    char data[256];

    for (int i = 0; i < 10; i++) {
        sprintf(data, "producer #%d", i);
        g_assert_true(msu_avllq_produce2(q, data, strlen(data), 0) == MSU_AVLLQ_STATUS_OK);
    }

    /* one slot is empty */
    g_assert_cmpint(msu_avllq_buf_size(q), ==, 3);
    g_assert_false(msu_avllq_buf_empty(q));
    g_assert_true(msu_avllq_buf_full(q));

    msu_avllq_item_t item;
    g_assert_true(msu_avllq_consume(q, consumer_id, &item) == MSU_AVLLQ_STATUS_OK);

    g_assert_cmpint(item.len, ==, strlen("producer #0"));
    g_assert_cmpint(memcmp(item.data, "producer #7", item.len), ==, 0);

    msu_avllq_item_release(&item);
    g_assert_true(TRUE);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 2);

    msu_avllq_destroy(q);

    g_assert_true(TRUE);
}

static void test_avllq_st_produce_slow_and_multiple_consume()
{
    msu_avllq_handle_t q = msu_avllq_create(4, 1000);

    int consumer_id1 = msu_avllq_register_consumer(q);
    int consumer_id2 = msu_avllq_register_consumer(q);
    int consumer_id3 = msu_avllq_register_consumer(q);
    int consumer_id4 = msu_avllq_register_consumer(q);

    g_assert_true(consumer_id1 >= 0);
    g_assert_true(consumer_id2 >= 0);
    g_assert_true(consumer_id3 >= 0);
    g_assert_true(consumer_id4 >= 0);

    char data[256];
    msu_avllq_item_t item;

    /* produce 1, consume 4 in 4 different consumer */
    sprintf(data, "producer #0");

    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 0) == MSU_AVLLQ_STATUS_OK);

    g_assert_true(msu_avllq_consume(q, consumer_id1, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    g_assert_true(msu_avllq_consume(q, consumer_id2, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    g_assert_true(msu_avllq_consume(q, consumer_id3, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    g_assert_true(msu_avllq_consume(q, consumer_id4, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    /* buffer will only disappear after all consumers fetchedc buffer */
    g_assert_cmpint(msu_avllq_buf_size(q), ==, 0);

    g_assert_true(msu_avllq_consume(q, consumer_id4, &item) == MSU_AVLLQ_STATUS_NO_BUF);

    /* produce 2, consume 4 in 4 different consumers */
    sprintf(data, "producer #0");
    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 0) == MSU_AVLLQ_STATUS_OK);

    sprintf(data, "producer #1");
    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 1) == MSU_AVLLQ_STATUS_OK);

    /* fetch the first buffer */
    g_assert_true(msu_avllq_consume(q, consumer_id1, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 2);

    g_assert_true(msu_avllq_consume(q, consumer_id2, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 2);

    g_assert_true(msu_avllq_consume(q, consumer_id3, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 2);

    g_assert_true(msu_avllq_consume(q, consumer_id4, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    /* al consumer fetched "producer #0“， buffer count-- */
    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    /* fetch the second buffer */
    g_assert_true(msu_avllq_consume(q, consumer_id1, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    g_assert_true(msu_avllq_consume(q, consumer_id2, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    g_assert_true(msu_avllq_consume(q, consumer_id3, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    g_assert_true(msu_avllq_consume(q, consumer_id4, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    /* two buffers are fetched */
    g_assert_cmpint(msu_avllq_buf_size(q), ==, 0);

    /* no buf */
    g_assert_true(msu_avllq_consume(q, consumer_id4, &item) == MSU_AVLLQ_STATUS_NO_BUF);

    /* produce 4, consume 4 in 4 different consumer */
    sprintf(data, "producer #0");
    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 0) == MSU_AVLLQ_STATUS_OK);

    sprintf(data, "producer #1");
    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 1) == MSU_AVLLQ_STATUS_OK);

    sprintf(data, "producer #2");
    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 1) == MSU_AVLLQ_STATUS_OK);

    /* overwrite producer #0 */
    sprintf(data, "producer #3");
    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 1) == MSU_AVLLQ_STATUS_OK);

    g_assert_true(msu_avllq_consume(q, consumer_id1, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);
    g_assert_true(msu_avllq_consume(q, consumer_id1, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);
    g_assert_true(msu_avllq_consume(q, consumer_id1, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_true(msu_avllq_consume(q, consumer_id2, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);
    g_assert_true(msu_avllq_consume(q, consumer_id2, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);
    g_assert_true(msu_avllq_consume(q, consumer_id2, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_true(msu_avllq_consume(q, consumer_id3, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);
    g_assert_true(msu_avllq_consume(q, consumer_id3, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);
    g_assert_true(msu_avllq_consume(q, consumer_id3, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    /* first 3 consumers fetched all the buffers, the fourth is slow, no buffer has been fetched */
    g_assert_cmpint(msu_avllq_buf_size(q), ==, 3);

    g_assert_true(msu_avllq_consume(q, consumer_id4, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 2);

    g_assert_true(msu_avllq_consume(q, consumer_id4, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    g_assert_true(msu_avllq_consume(q, consumer_id4, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 0);

    g_assert_true(msu_avllq_consume(q, consumer_id4, &item) == MSU_AVLLQ_STATUS_NO_BUF);

    msu_avllq_destroy(q);

    g_assert_true(TRUE);
}

static void test_avllq_st_produce_and_multiple_consumer_join_in_the_middle()
{
    msu_avllq_handle_t q = msu_avllq_create(4, 1000);

    char data[256];
    msu_avllq_item_t item;

    sprintf(data, "producer #0");
    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 0) == MSU_AVLLQ_STATUS_OK);

    int consumer_id1 = msu_avllq_register_consumer(q);

    sprintf(data, "producer #1");
    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 0) == MSU_AVLLQ_STATUS_OK);

    g_assert_true(msu_avllq_consume(q, consumer_id1, &item) == MSU_AVLLQ_STATUS_OK);
    g_assert_cmpint(item.len, ==, strlen("producer #0"));
    g_assert_cmpint(memcmp(item.data, "producer #0", strlen("producer #0")), ==, 0);
    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    msu_avllq_item_release(&item);

    int consumer_id2 = msu_avllq_register_consumer(q);

    sprintf(data, "producer #2");
    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 0) == MSU_AVLLQ_STATUS_OK);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 2);

    g_assert_true(msu_avllq_consume(q, consumer_id1, &item) == MSU_AVLLQ_STATUS_OK);

    /*  consumer 2 has not yet fetched buffer， size is not changed */
    g_assert_cmpint(msu_avllq_buf_size(q), ==, 2);
    msu_avllq_item_release(&item);

    g_assert_true(msu_avllq_consume(q, consumer_id2, &item) == MSU_AVLLQ_STATUS_OK);

    /* consumer 1,2 have fetched buffer "producer #1"， so size - 1 */
    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);
    msu_avllq_item_release(&item);

    int consumer_id3 = msu_avllq_register_consumer(q);
    g_assert_true(msu_avllq_consume(q, consumer_id1, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    g_assert_true(msu_avllq_consume(q, consumer_id2, &item) == MSU_AVLLQ_STATUS_OK);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    g_assert_true(msu_avllq_consume(q, consumer_id3, &item) == MSU_AVLLQ_STATUS_OK);
    g_assert_cmpint(memcmp(item.data, "producer #2", strlen("producer #2")), ==, 0);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 0);

    msu_avllq_destroy(q);
}

static void test_avllq_st_produce_and_multiple_consumer_register_and_deregister()
{
    msu_avllq_handle_t q = msu_avllq_create(4, 1000);

    char data[256];
    msu_avllq_item_t item;

    sprintf(data, "producer #0");
    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 0) == MSU_AVLLQ_STATUS_OK);

    int consumer_id1 = msu_avllq_register_consumer(q);

    sprintf(data, "producer #1");
    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 0) == MSU_AVLLQ_STATUS_OK);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 2);

    g_assert_true(msu_avllq_consume(q, consumer_id1, &item) == MSU_AVLLQ_STATUS_OK);
    g_assert_cmpint(memcmp(item.data, "producer #0", strlen("producer #0")), ==, 0);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    msu_avllq_deregister_consumer(q, consumer_id1);

    sprintf(data, "producer #2");
    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 0) == MSU_AVLLQ_STATUS_OK);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 2);

    int consumer_id2 = msu_avllq_register_consumer(q);

    g_assert_true(msu_avllq_consume(q, consumer_id2, &item) == MSU_AVLLQ_STATUS_OK);
    g_assert_cmpint(memcmp(item.data, "producer #1", strlen("producer #1")), ==, 0);
    msu_avllq_item_release(&item);

    /* only one consumer， so buf -1 after consume */
    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    int consumer_id3 = msu_avllq_register_consumer(q);

    sprintf(data, "producer #3");
    g_assert_true(msu_avllq_produce2(q, data, strlen(data), 0) == MSU_AVLLQ_STATUS_OK);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 2);

    g_assert_true(msu_avllq_consume(q, consumer_id2, &item) == MSU_AVLLQ_STATUS_OK);
    g_assert_cmpint(memcmp(item.data, "producer #2", strlen("producer #2")), ==, 0);
    msu_avllq_item_release(&item);

    g_assert_true(msu_avllq_consume(q, consumer_id3, &item) == MSU_AVLLQ_STATUS_OK);
    g_assert_cmpint(memcmp(item.data, "producer #2", strlen("producer #2")), ==, 0);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 1);

    g_assert_true(msu_avllq_consume(q, consumer_id2, &item) == MSU_AVLLQ_STATUS_OK);
    g_assert_cmpint(memcmp(item.data, "producer #3", strlen("producer #3")), ==, 0);
    msu_avllq_item_release(&item);
    g_assert_true(msu_avllq_consume(q, consumer_id3, &item) == MSU_AVLLQ_STATUS_OK);
    g_assert_cmpint(memcmp(item.data, "producer #3", strlen("producer #3")), ==, 0);
    msu_avllq_item_release(&item);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 0);

    msu_avllq_deregister_consumer(q, consumer_id2);
    msu_avllq_deregister_consumer(q, consumer_id3);

    int consumers[MSU_AVLLQ_MAX_CONSUMER];
    g_assert_cmpint(msu_avllq_enumerate_consumers(q, consumers), ==, 0);

    msu_avllq_destroy(q);
}

static gpointer test_avllq_mt_produce_fast_and_consume_slow_producer(gpointer data)
{
    msu_avllq_handle_t q = (msu_avllq_handle_t)data;

    char buf[16];
    for (int i = 0; i < 30; i++) {
        sprintf(buf, "data #%d", i);
        g_assert_true(msu_avllq_produce2(q, buf, strlen(buf), 0) == MSU_AVLLQ_STATUS_OK);
        usleep(1000);
    }

    return NULL;
}

static gpointer test_avllq_mt_produce_fast_and_consume_slow_consumer(gpointer data)
{
    msu_avllq_handle_t q = (msu_avllq_handle_t)data;

    int consumer_id = msu_avllq_register_consumer(q);

    msu_avllq_item_t item;
    char buf[16];

    int success_count = 0;
    for (int i = 0; i < 30; i++) {
        msu_avllq_status_t status = msu_avllq_consume(q, consumer_id, &item);

        if (status == MSU_AVLLQ_STATUS_OK) {
            sprintf(buf, "data #%d", success_count++);
            g_assert_cmpint(memcmp(item.data, buf, strlen(buf)), ==, 0);
            msu_avllq_item_release(&item);
        }

        usleep(3000);
    }

    msu_avllq_deregister_consumer(q, consumer_id);

    return NULL;
}

static void test_avllq_mt_produce_fast_and_consume_slow()
{
    msu_avllq_handle_t q = msu_avllq_create(40, 1000);

    GThread *producer_thread = g_thread_new("producer", test_avllq_mt_produce_fast_and_consume_slow_producer, q);
    GThread *consumer_thread = g_thread_new("consumer", test_avllq_mt_produce_fast_and_consume_slow_consumer, q);

    g_thread_join(producer_thread);
    g_thread_join(consumer_thread);

    msu_avllq_destroy(q);
}

struct producer_consumer_data_t {
    msu_avllq_handle_t      q;
    int                     start_flag;
};

static gpointer test_avllq_mt_produce_slow_and_multiple_consume_producer(gpointer data)
{
    struct producer_consumer_data_t *pcd = (struct producer_consumer_data_t *)data;

    while (pcd->start_flag < 2) {
        usleep(10000);
    }

    char buf[16];
    for (int i = 0; i < 45; i++) {
        sprintf(buf, "data #%d", i);
        g_assert_true(msu_avllq_produce2(pcd->q, buf, strlen(buf), 0) == MSU_AVLLQ_STATUS_OK);
        usleep(3000);
    }

    return NULL;
}

static gpointer test_avllq_mt_produce_slow_and_multiple_consume_consumer(gpointer data)
{
    struct producer_consumer_data_t *pcd = (struct producer_consumer_data_t *)data;

    int consumer_id = msu_avllq_register_consumer(pcd->q);

    pcd->start_flag++;

    msu_avllq_item_t item;
    char buf[16];

    int success_count = 0;
    for (int i = 0; i < 45; i++) {
        msu_avllq_status_t status = msu_avllq_consume(pcd->q, consumer_id, &item);

        if (status == MSU_AVLLQ_STATUS_OK) {
            sprintf(buf, "data #%d", success_count++);

            if (memcmp(item.data, buf, strlen(buf)) != 0) {
                g_assert_true(FALSE);
            }
            msu_avllq_item_release(&item);
        }

        usleep(1000);
    }

    msu_avllq_deregister_consumer(pcd->q, consumer_id);

    return NULL;
}

static void test_avllq_mt_produce_slow_and_multiple_consume()
{
    msu_avllq_handle_t q = msu_avllq_create(50, 1000);

    struct producer_consumer_data_t data;
    data.q = q;
    data.start_flag = 0;

    GThread *producer_thread = g_thread_new("producer", test_avllq_mt_produce_slow_and_multiple_consume_producer, &data);
    GThread *consumer_thread1 = g_thread_new("consumer1", test_avllq_mt_produce_slow_and_multiple_consume_consumer, &data);
    GThread *consumer_thread2 = g_thread_new("consumer2", test_avllq_mt_produce_slow_and_multiple_consume_consumer, &data);

    g_thread_join(producer_thread);
    g_thread_join(consumer_thread1);
    g_thread_join(consumer_thread2);

    msu_avllq_destroy(q);
}

static void test_avllq_no_producer_buf_malloc()
{
    int max_item_size = 10 * 1024 * 1024;

    msu_avllq_handle_t q = msu_avllq_create(4, max_item_size);

    int consumer_id = msu_avllq_register_consumer(q);

    char *data = (char *)g_malloc(max_item_size);

    for (int i = 0; i < 10; i++) {
        memset(data, (char)i, max_item_size);

        g_assert_true(msu_avllq_produce2(q, data, max_item_size, 0) == MSU_AVLLQ_STATUS_OK);
    }

    msu_avllq_item_t item;
    g_assert_true(msu_avllq_consume(q, consumer_id, &item) == MSU_AVLLQ_STATUS_OK);

    g_assert_cmpint(item.len, ==, max_item_size);

    /* 3 valid data remain in queue，the oldest is 7 */
    for (int i = 0; i < max_item_size; i++) {
        g_assert_cmpint(((char *)item.data)[i], ==, 7);
    }

    msu_avllq_item_release(&item);
    g_assert_true(TRUE);

    g_assert_cmpint(msu_avllq_buf_size(q), ==, 2);

    msu_avllq_destroy(q);

    g_free(data);

    g_assert_true(TRUE);
}

int main(int argc, char *argv[])
{
    g_test_init(&argc, &argv, NULL);

    g_test_add_func("/miscutil/avllq/test_avllq_create_and_destroy",
                    test_avllq_create_and_destroy);

    g_test_add_func("/miscutil/avllq/test_avllq_size_of_empty_queue",
                    test_avllq_size_of_empty_queue);

    g_test_add_func("/miscutil/avllq/test_avllq_register_and_deregister_consumer",
                    test_avllq_register_and_deregister_consumer);

    g_test_add_func("/miscutil/avllq/test_avllq_st_produce_without_consume",
                    test_avllq_st_produce_without_consume);

    g_test_add_func("/miscutil/avllq/test_avllq_st_produce_without_consume_but_with_consumer",
                    test_avllq_st_produce_without_consume_but_with_consumer);

    g_test_add_func("/miscutil/avllq/test_avllq_st_produce_and_consume",
                    test_avllq_st_produce_and_consume);

    g_test_add_func("/miscutil/avllq/test_avllq_st_produce_fast_and_consume_slow",
                    test_avllq_st_produce_fast_and_consume_slow);

    g_test_add_func("/miscutil/avllq/test_avllq_st_produce_slow_and_multiple_consume",
                    test_avllq_st_produce_slow_and_multiple_consume);

    g_test_add_func("/miscutil/avllq/test_avllq_st_produce_and_multiple_consumer_join_in_the_middle",
                    test_avllq_st_produce_and_multiple_consumer_join_in_the_middle);

    g_test_add_func("/miscutil/avllq/test_avllq_st_produce_and_multiple_consumer_register_and_deregister",
                    test_avllq_st_produce_and_multiple_consumer_register_and_deregister);

    g_test_add_func("/miscutil/avllq/test_avllq_mt_produce_fast_and_consume_slow",
                    test_avllq_mt_produce_fast_and_consume_slow);

    g_test_add_func("/miscutil/avllq/test_avllq_mt_produce_slow_and_multiple_consume",
                    test_avllq_mt_produce_slow_and_multiple_consume);

    g_test_add_func("/miscutil/avllq/test_avllq_no_producer_buf_malloc",
                    test_avllq_no_producer_buf_malloc);

    return g_test_run();
}
