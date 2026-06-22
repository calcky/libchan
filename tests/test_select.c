#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "libchan.h"

static int g_failures = 0;
#define CHECK(c) do { if (!(c)) { fprintf(stderr,"FAIL %s:%d: %s\n",__FILE__,__LINE__,#c); g_failures++; } } while(0)
#define CHECK_EQ(a,b) CHECK((a)==(b))

/* select picks the ready channel */
static void test_select_ready(void) {
    chan_t *a = chan_create(sizeof(int), 1);
    chan_t *b = chan_create(sizeof(int), 1);
    int va = 10, vb = 20, out = 0;

    chan_send(a, &va);

    chan_select_case_t cases[2] = {
        { a, CHAN_OP_RECV, &out, CHAN_OK },
        { b, CHAN_OP_RECV, &out, CHAN_OK },
    };
    int w = chan_select_try(cases, 2);
    CHECK_EQ(w, 0);
    CHECK_EQ(out, 10);
    (void)vb;
    chan_destroy(a);
    chan_destroy(b);
}

/* select_try returns -1 when nothing is ready */
static void test_select_try_none_ready(void) {
    chan_t *a = chan_create(sizeof(int), 1);
    chan_t *b = chan_create(sizeof(int), 1);
    int out;
    chan_select_case_t cases[2] = {
        { a, CHAN_OP_RECV, &out, CHAN_OK },
        { b, CHAN_OP_RECV, &out, CHAN_OK },
    };
    CHECK_EQ(chan_select_try(cases, 2), -1);
    chan_destroy(a);
    chan_destroy(b);
}

/* Fairness: when both channels are ready, each gets selected ~50% of the time */
static void test_select_fairness(void) {
    chan_t *a = chan_create(sizeof(int), 1);
    chan_t *b = chan_create(sizeof(int), 1);
    int counts[2] = {0, 0};
    int val = 1, out;

    for (int i = 0; i < 2000; i++) {
        chan_send(a, &val);
        chan_send(b, &val);
        chan_select_case_t cases[2] = {
            { a, CHAN_OP_RECV, &out, CHAN_OK },
            { b, CHAN_OP_RECV, &out, CHAN_OK },
        };
        int w = chan_select_try(cases, 2);
        CHECK(w == 0 || w == 1);
        if (w >= 0) counts[w]++;
        /* Drain the un-selected channel */
        chan_try_recv(a, &out);
        chan_try_recv(b, &out);
    }
    /* Each should be chosen at least 30% of the time */
    CHECK(counts[0] > 600 && counts[1] > 600);
    chan_destroy(a);
    chan_destroy(b);
}

/* select blocks until a sender arrives */
struct sel_sender_arg { chan_t *ch; int delay_ms; int val; };
static void *sel_sender(void *arg) {
    struct sel_sender_arg *a = arg;
    struct timespec ts = { .tv_nsec = a->delay_ms * 1000000L };
    nanosleep(&ts, NULL);
    chan_send(a->ch, &a->val);
    return NULL;
}

static void test_select_blocking(void) {
    chan_t *a = chan_create(sizeof(int), 0);
    chan_t *b = chan_create(sizeof(int), 0);
    struct sel_sender_arg arg = { b, 30, 77 };
    pthread_t t;
    pthread_create(&t, NULL, sel_sender, &arg);

    int out = 0;
    chan_select_case_t cases[2] = {
        { a, CHAN_OP_RECV, &out, CHAN_OK },
        { b, CHAN_OP_RECV, &out, CHAN_OK },
    };
    int w = chan_select(cases, 2);
    CHECK_EQ(w, 1);
    CHECK_EQ(out, 77);
    pthread_join(t, NULL);
    chan_destroy(a);
    chan_destroy(b);
}

/* select_timeout returns -1 on expiry */
static void test_select_timeout(void) {
    chan_t *a = chan_create(sizeof(int), 0);
    int out;
    chan_select_case_t cases[1] = {{ a, CHAN_OP_RECV, &out, CHAN_OK }};
    int w = chan_select_timeout(cases, 1, 30000000LL); /* 30ms */
    CHECK_EQ(w, -1);
    CHECK_EQ(cases[0].result, CHAN_ERR_TIMEOUT);
    chan_destroy(a);
}

/* select detects closed channel */
static void test_select_closed(void) {
    chan_t *a = chan_create(sizeof(int), 0);
    chan_t *b = chan_create(sizeof(int), 0);
    chan_close(b);

    int out;
    chan_select_case_t cases[2] = {
        { a, CHAN_OP_RECV, &out, CHAN_OK },
        { b, CHAN_OP_RECV, &out, CHAN_OK },
    };
    int w = chan_select_try(cases, 2);
    CHECK_EQ(w, 1);
    CHECK_EQ(cases[1].result, CHAN_ERR_CLOSED);
    chan_destroy(a);
    chan_destroy(b);
}

int main(void) {
    test_select_ready();
    test_select_try_none_ready();
    test_select_fairness();
    test_select_blocking();
    test_select_timeout();
    test_select_closed();

    if (g_failures) { fprintf(stderr, "%d failed\n", g_failures); return 1; }
    printf("test_select: all passed\n");
    return 0;
}
