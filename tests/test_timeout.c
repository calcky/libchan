/*
 * test_timeout.c — timeout, non-blocking, and error/edge-path coverage.
 *
 * Exercises the branches the throughput tests never hit: real timeouts firing,
 * try_* on full/empty, NULL/zero-size argument validation, introspection,
 * reference counting, close idempotency / send-after-close / drain-after-close,
 * select non-blocking + timeout paths, strerror, and large elements.
 */
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>
#include "libchan.h"

static int g_failures = 0;
#define CHECK(c) do { if (!(c)) { fprintf(stderr,"FAIL %s:%d: %s\n",__FILE__,__LINE__,#c); g_failures++; } } while(0)

#define MS (1000LL * 1000LL)

static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static void test_invalid_args(void) {
    int v = 1;
    CHECK(chan_send(NULL, &v) == CHAN_ERR_INVALID);
    CHECK(chan_recv(NULL, &v) == CHAN_ERR_INVALID);
    CHECK(chan_create(0, 4) == NULL);          /* zero element size */
    chan_t *ch = chan_create(sizeof(int), 4);
    CHECK(chan_send(ch, NULL) == CHAN_ERR_INVALID);
    CHECK(chan_recv(ch, NULL) == CHAN_ERR_INVALID);
    CHECK(chan_select(NULL, 2) == -1);
    chan_destroy(ch);
    chan_destroy(NULL);                         /* must not crash */
}

static void test_try_and_introspection(void) {
    chan_t *ch = chan_create(sizeof(int), 2);
    int v, a = 1, b = 2, c = 3;
    CHECK(chan_cap(ch) == 2);
    CHECK(chan_len(ch) == 0);
    CHECK(chan_try_recv(ch, &v) == CHAN_ERR_WOULDBLOCK);     /* empty */
    CHECK(chan_try_send(ch, &a) == CHAN_OK);
    CHECK(chan_try_send(ch, &b) == CHAN_OK);
    CHECK(chan_try_send(ch, &c) == CHAN_ERR_WOULDBLOCK);     /* full */
    CHECK(chan_len(ch) == 2);
    CHECK(chan_try_recv(ch, &v) == CHAN_OK && v == 1);       /* FIFO */
    CHECK(chan_try_recv(ch, &v) == CHAN_OK && v == 2);
    CHECK(chan_try_recv(ch, &v) == CHAN_ERR_WOULDBLOCK);
    chan_destroy(ch);
}

static void test_timeouts_fire(void) {
    int v = 7;

    chan_t *ch = chan_create(sizeof(int), 1);
    int64_t t0 = now_ns();
    CHECK(chan_recv_timeout(ch, &v, 30 * MS) == CHAN_ERR_TIMEOUT);  /* empty */
    CHECK(now_ns() - t0 >= 20 * MS);                                /* actually waited */
    CHECK(chan_send(ch, &v) == CHAN_OK);                            /* fill */
    CHECK(chan_send_timeout(ch, &v, 25 * MS) == CHAN_ERR_TIMEOUT);  /* full */
    CHECK(chan_recv(ch, &v) == CHAN_OK);
    chan_destroy(ch);

    chan_t *u = chan_create(sizeof(int), 0);                        /* unbuffered */
    CHECK(chan_recv_timeout(u, &v, 20 * MS) == CHAN_ERR_TIMEOUT);   /* no sender */
    CHECK(chan_send_timeout(u, &v, 20 * MS) == CHAN_ERR_TIMEOUT);   /* no receiver */
    chan_destroy(u);
}

/* A blocking recv_timeout that SUCCEEDS because a sender arrives first. */
static chan_t *g_rendez;
static void *late_sender(void *arg) {
    (void)arg;
    struct timespec ts = { 0, 10 * MS };
    nanosleep(&ts, NULL);
    int v = 42;
    chan_send(g_rendez, &v);
    return NULL;
}
static void test_timeout_success(void) {
    g_rendez = chan_create(sizeof(int), 0);
    pthread_t t;
    pthread_create(&t, NULL, late_sender, NULL);
    int v = 0;
    CHECK(chan_recv_timeout(g_rendez, &v, 2000 * MS) == CHAN_OK && v == 42);
    pthread_join(t, NULL);
    chan_destroy(g_rendez);
}

static void test_close_semantics(void) {
    chan_t *ch = chan_create(sizeof(int), 4);
    int a = 1, b = 2, v;
    chan_send(ch, &a);
    chan_send(ch, &b);

    CHECK(chan_close(ch) == CHAN_OK);
    CHECK(chan_close(ch) == CHAN_ERR_CLOSED);      /* idempotent */
    CHECK(chan_is_closed(ch));
    CHECK(chan_send(ch, &a) == CHAN_ERR_CLOSED);   /* send after close */
    CHECK(chan_try_send(ch, &a) == CHAN_ERR_CLOSED);
    /* buffered data still drains after close (Go semantics) */
    CHECK(chan_recv(ch, &v) == CHAN_OK && v == 1);
    CHECK(chan_recv(ch, &v) == CHAN_OK && v == 2);
    CHECK(chan_recv(ch, &v) == CHAN_ERR_CLOSED);   /* then closed */
    CHECK(chan_try_recv(ch, &v) == CHAN_ERR_CLOSED);
    chan_destroy(ch);
}

static void test_retain(void) {
    chan_t *ch = chan_create(sizeof(int), 1);
    chan_t *ch2 = chan_retain(ch);
    CHECK(ch2 == ch);
    int v = 5;
    CHECK(chan_send(ch, &v) == CHAN_OK);
    chan_destroy(ch);                              /* refcount 2 -> 1, alive */
    int out = 0;
    CHECK(chan_recv(ch2, &out) == CHAN_OK && out == 5);
    chan_destroy(ch2);                             /* -> 0, freed */
    CHECK(chan_retain(NULL) == NULL);
}

static void test_select_edges(void) {
    chan_t *a = chan_create(sizeof(int), 1);
    chan_t *b = chan_create(sizeof(int), 1);
    int va = 0, vb = 0;
    chan_select_case_t cs[2] = {
        { a, CHAN_OP_RECV, &va, CHAN_OK },
        { b, CHAN_OP_RECV, &vb, CHAN_OK },
    };
    CHECK(chan_select_try(cs, 2) == -1);                  /* nothing ready */
    CHECK(chan_select_timeout(cs, 2, 20 * MS) == -1);     /* still nothing → timeout */

    int x = 9;
    chan_send(b, &x);                                     /* make case 1 ready */
    int w = chan_select_try(cs, 2);
    CHECK(w == 1 && cs[1].result == CHAN_OK && vb == 9);
    chan_destroy(a);
    chan_destroy(b);
}

/* select with a SEND case: ready when the channel has room, not when full. */
static void test_select_send(void) {
    chan_t *ch = chan_create(sizeof(int), 1);
    int out = 0, s1 = 11, s2 = 22;
    chan_select_case_t cs[1] = { { ch, CHAN_OP_SEND, &s1, CHAN_OK } };
    CHECK(chan_select_try(cs, 1) == 0);                   /* room → send executes */
    CHECK(chan_select_try(cs, 1) == -1);                  /* full → not ready */
    CHECK(chan_recv(ch, &out) == CHAN_OK && out == 11);
    cs[0].data = &s2;
    CHECK(chan_select_timeout(cs, 1, 50 * MS) == 0);      /* room again → sends */
    CHECK(chan_recv(ch, &out) == CHAN_OK && out == 22);
    chan_destroy(ch);
}

/* blocking chan_select that is unblocked by a sender on another thread. */
static chan_t *g_sel;
static void *sel_sender(void *arg) {
    (void)arg;
    struct timespec ts = { 0, 10 * MS };
    nanosleep(&ts, NULL);
    int v = 77;
    chan_send(g_sel, &v);
    return NULL;
}
static void test_select_blocking(void) {
    g_sel = chan_create(sizeof(int), 0);                  /* unbuffered */
    chan_t *other = chan_create(sizeof(int), 0);
    int v = 0, dummy = 0;
    chan_select_case_t cs[2] = {
        { g_sel,  CHAN_OP_RECV, &v,     CHAN_OK },
        { other,  CHAN_OP_RECV, &dummy, CHAN_OK },
    };
    pthread_t t;
    pthread_create(&t, NULL, sel_sender, NULL);
    int w = chan_select(cs, 2);                           /* blocks until sender wakes it */
    CHECK(w == 0 && cs[0].result == CHAN_OK && v == 77);
    pthread_join(t, NULL);
    chan_destroy(g_sel);
    chan_destroy(other);
}

static void test_strerror(void) {
    chan_err_t codes[] = {
        CHAN_OK, CHAN_ERR_CLOSED, CHAN_ERR_TIMEOUT,
        CHAN_ERR_WOULDBLOCK, CHAN_ERR_INVALID, CHAN_ERR_NOMEM,
    };
    for (size_t i = 0; i < sizeof(codes) / sizeof(codes[0]); i++) {
        const char *s = chan_strerror(codes[i]);
        CHECK(s != NULL && s[0] != '\0');
    }
    CHECK(chan_strerror((chan_err_t)999) != NULL);        /* unknown code */
}

static void test_large_elem(void) {
    typedef struct { char buf[200]; int tag; } big_t;
    chan_t *ch = chan_create(sizeof(big_t), 4);
    big_t in;
    memset(&in, 0, sizeof in);
    in.tag = 1234;
    strcpy(in.buf, "hello");
    CHECK(chan_send(ch, &in) == CHAN_OK);
    big_t out;
    CHECK(chan_recv(ch, &out) == CHAN_OK);
    CHECK(out.tag == 1234 && strcmp(out.buf, "hello") == 0);
    chan_destroy(ch);
}

int main(void) {
    test_invalid_args();
    test_try_and_introspection();
    test_timeouts_fire();
    test_timeout_success();
    test_close_semantics();
    test_retain();
    test_select_edges();
    test_select_send();
    test_select_blocking();
    test_strerror();
    test_large_elem();

    if (g_failures) { fprintf(stderr, "%d failed\n", g_failures); return 1; }
    printf("test_timeout: all passed\n");
    return 0;
}
