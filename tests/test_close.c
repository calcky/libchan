#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "libchan.h"

static int g_failures = 0;
#define CHECK(c) do { if (!(c)) { fprintf(stderr,"FAIL %s:%d: %s\n",__FILE__,__LINE__,#c); g_failures++; } } while(0)
#define CHECK_EQ(a,b) CHECK((a)==(b))

static void test_send_after_close(void) {
    chan_t *ch = chan_create(sizeof(int), 4);
    CHECK_EQ(chan_close(ch), CHAN_OK);
    int v = 1;
    CHECK_EQ(chan_send(ch, &v),     CHAN_ERR_CLOSED);
    CHECK_EQ(chan_try_send(ch, &v), CHAN_ERR_CLOSED);
    chan_destroy(ch);
}

static void test_double_close(void) {
    chan_t *ch = chan_create(sizeof(int), 2);
    CHECK_EQ(chan_close(ch), CHAN_OK);
    CHECK_EQ(chan_close(ch), CHAN_ERR_CLOSED); /* idempotent */
    CHECK(chan_is_closed(ch));
    chan_destroy(ch);
}

/* Drain buffered data after close (Go semantics) */
static void test_drain_after_close(void) {
    chan_t *ch = chan_create(sizeof(int), 4);
    for (int i = 0; i < 3; i++) chan_send(ch, &i);
    chan_close(ch);

    for (int i = 0; i < 3; i++) {
        int v = -1;
        CHECK_EQ(chan_recv(ch, &v), CHAN_OK);
        CHECK_EQ(v, i);
    }
    int v;
    CHECK_EQ(chan_recv(ch, &v), CHAN_ERR_CLOSED);
    chan_destroy(ch);
}

/* Receiver blocked on empty channel, then close wakes it */
static void *blocking_recv(void *arg) {
    chan_t *ch = arg;
    int v;
    chan_err_t *r = malloc(sizeof *r);
    *r = chan_recv(ch, &v);
    return r;
}

static void test_close_wakes_receiver(void) {
    chan_t *ch = chan_create(sizeof(int), 0);
    pthread_t t;
    pthread_create(&t, NULL, blocking_recv, ch);

    struct timespec ts = { .tv_nsec = 20000000L }; /* 20ms */
    nanosleep(&ts, NULL);

    chan_close(ch);

    chan_err_t *r;
    pthread_join(t, (void **)&r);
    CHECK_EQ(*r, CHAN_ERR_CLOSED);
    free(r);
    chan_destroy(ch);
}

/* Sender blocked on full channel, then close wakes it */
static void *blocking_send(void *arg) {
    chan_t *ch = arg;
    int v = 1;
    chan_err_t *r = malloc(sizeof *r);
    *r = chan_send(ch, &v);
    return r;
}

static void test_close_wakes_sender(void) {
    chan_t *ch = chan_create(sizeof(int), 1);
    int v = 0;
    chan_send(ch, &v); /* fill the buffer */

    pthread_t t;
    pthread_create(&t, NULL, blocking_send, ch);

    struct timespec ts = { .tv_nsec = 20000000L };
    nanosleep(&ts, NULL);

    chan_close(ch);

    chan_err_t *r;
    pthread_join(t, (void **)&r);
    CHECK_EQ(*r, CHAN_ERR_CLOSED);
    free(r);
    chan_destroy(ch);
}

int main(void) {
    test_send_after_close();
    test_double_close();
    test_drain_after_close();
    test_close_wakes_receiver();
    test_close_wakes_sender();

    if (g_failures) { fprintf(stderr, "%d failed\n", g_failures); return 1; }
    printf("test_close: all passed\n");
    return 0;
}
