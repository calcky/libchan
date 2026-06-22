#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include "libchan.h"

static int g_failures = 0;
#define CHECK(c) do { if (!(c)) { fprintf(stderr,"FAIL %s:%d: %s\n",__FILE__,__LINE__,#c); g_failures++; } } while(0)
#define CHECK_EQ(a,b) CHECK((a)==(b))

static void test_try_send_full(void) {
    chan_t *ch = chan_create(sizeof(int), 2);
    int v = 1;
    CHECK_EQ(chan_send(ch, &v), CHAN_OK);
    CHECK_EQ(chan_send(ch, &v), CHAN_OK);
    CHECK_EQ(chan_try_send(ch, &v), CHAN_ERR_WOULDBLOCK);
    chan_destroy(ch);
}

static void test_try_recv_empty(void) {
    chan_t *ch = chan_create(sizeof(int), 2);
    int v;
    CHECK_EQ(chan_try_recv(ch, &v), CHAN_ERR_WOULDBLOCK);
    chan_destroy(ch);
}

static void test_try_unbuffered_no_partner(void) {
    chan_t *ch = chan_create(sizeof(int), 0);
    int v = 1;
    CHECK_EQ(chan_try_send(ch, &v), CHAN_ERR_WOULDBLOCK);
    CHECK_EQ(chan_try_recv(ch, &v), CHAN_ERR_WOULDBLOCK);
    chan_destroy(ch);
}

/* Timeout: sender times out waiting for receiver on unbuffered channel */
static void test_send_timeout(void) {
    chan_t *ch = chan_create(sizeof(int), 0);
    int v = 7;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    chan_err_t e = chan_send_timeout(ch, &v, 50000000LL); /* 50ms */
    clock_gettime(CLOCK_MONOTONIC, &t1);
    CHECK_EQ(e, CHAN_ERR_TIMEOUT);
    long elapsed_ms = (t1.tv_sec - t0.tv_sec) * 1000L
                    + (t1.tv_nsec - t0.tv_nsec) / 1000000L;
    /* Should be at least 40ms and at most 500ms */
    CHECK(elapsed_ms >= 40 && elapsed_ms < 500);
    chan_destroy(ch);
}

static void test_recv_timeout(void) {
    chan_t *ch = chan_create(sizeof(int), 4);
    int v;
    chan_err_t e = chan_recv_timeout(ch, &v, 50000000LL);
    CHECK_EQ(e, CHAN_ERR_TIMEOUT);
    chan_destroy(ch);
}

/* Unblocked by partner arriving: sender blocks then a late receiver arrives */
struct unblock_arg { chan_t *ch; int delay_ms; };

static void *late_receiver(void *arg) {
    struct unblock_arg *a = arg;
    struct timespec ts = { .tv_sec = 0, .tv_nsec = a->delay_ms * 1000000L };
    nanosleep(&ts, NULL);
    int v;
    chan_recv(a->ch, &v);
    return NULL;
}

static void test_sender_unblocked(void) {
    chan_t *ch = chan_create(sizeof(int), 0);
    struct unblock_arg arg = { ch, 50 };
    pthread_t t;
    pthread_create(&t, NULL, late_receiver, &arg);
    int v = 99;
    chan_err_t e = chan_send_timeout(ch, &v, 500000000LL); /* 500ms */
    CHECK_EQ(e, CHAN_OK);
    pthread_join(t, NULL);
    chan_destroy(ch);
}

int main(void) {
    test_try_send_full();
    test_try_recv_empty();
    test_try_unbuffered_no_partner();
    test_send_timeout();
    test_recv_timeout();
    test_sender_unblocked();

    if (g_failures) { fprintf(stderr, "%d failed\n", g_failures); return 1; }
    printf("test_blocking: all passed\n");
    return 0;
}
