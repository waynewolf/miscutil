#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <locale.h>
#include <glib.h>
#include "fdzcq.h"

static void test_fdzcq_create_and_destroy()
{
    msu_fdzcq_handle_t q = msu_fdzcq_create(10);
    g_assert_nonnull(q);

    msu_fdzcq_destroy(q);

    q = msu_fdzcq_create(10);
    g_assert_nonnull(q);

    msu_fdzcq_destroy(q);
}

int main(int argc, char *argv[])
{
    g_test_init(&argc, &argv, NULL);

    g_test_add_func("/miscutil/fdzcq/test_fdzcq_create_and_destroy",
                    test_fdzcq_create_and_destroy);

    return g_test_run();
}
