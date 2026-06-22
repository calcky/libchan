/* Minimal test harness */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_failures = 0;

#define CHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        g_failures++; \
    } \
} while (0)

#define CHECK_EQ(a, b) CHECK((a) == (b))

#include "libchan.h"

/* ---- Basic buffered channel ---- */
static void test_buffered_send_recv(void) {
    chan_t *ch = chan_create(sizeof(int), 4);
    CHECK(ch != NULL);
    CHECK_EQ(chan_cap(ch), (size_t)4);
    CHECK_EQ(chan_len(ch), (size_t)0);

    for (int i = 0; i < 4; i++) {
        CHECK_EQ(chan_send(ch, &i), CHAN_OK);
    }
    CHECK_EQ(chan_len(ch), (size_t)4);

    for (int i = 0; i < 4; i++) {
        int v = -1;
        CHECK_EQ(chan_recv(ch, &v), CHAN_OK);
        CHECK_EQ(v, i);
    }
    CHECK_EQ(chan_len(ch), (size_t)0);
    chan_destroy(ch);
}

/* ---- Ring buffer wrap-around ---- */
static void test_ring_wraparound(void) {
    chan_t *ch = chan_create(sizeof(int), 3);
    int v;

    /* Fill, drain, fill, drain to exercise wrap */
    for (int round = 0; round < 3; round++) {
        for (int i = 0; i < 3; i++) CHECK_EQ(chan_send(ch, &i), CHAN_OK);
        for (int i = 0; i < 3; i++) {
            CHECK_EQ(chan_recv(ch, &v), CHAN_OK);
            CHECK_EQ(v, i);
        }
    }
    chan_destroy(ch);
}

/* ---- Unbuffered: two threads ---- */
#include <pthread.h>

static chan_t *g_ub_ch;

static void *ub_sender(void *arg) {
    int val = 42;
    chan_send(g_ub_ch, &val);
    return NULL;
}

static void test_unbuffered(void) {
    g_ub_ch = chan_create(sizeof(int), 0);
    CHECK(g_ub_ch != NULL);
    CHECK_EQ(chan_cap(g_ub_ch), (size_t)0);

    pthread_t t;
    pthread_create(&t, NULL, ub_sender, NULL);

    int v = 0;
    CHECK_EQ(chan_recv(g_ub_ch, &v), CHAN_OK);
    CHECK_EQ(v, 42);

    pthread_join(t, NULL);
    chan_destroy(g_ub_ch);
}

/* ---- chan_len / chan_cap ---- */
static void test_introspection(void) {
    chan_t *ch = chan_create(sizeof(double), 8);
    double d = 1.0;
    chan_send(ch, &d);
    chan_send(ch, &d);
    CHECK_EQ(chan_len(ch), (size_t)2);
    CHECK_EQ(chan_cap(ch), (size_t)8);
    chan_destroy(ch);
}

/* ---- Large element ---- */
static void test_large_elem(void) {
    typedef struct { char buf[256]; int id; } big_t;
    chan_t *ch = chan_create(sizeof(big_t), 2);
    big_t s = {{0}, 99};
    memset(s.buf, 0xAB, sizeof s.buf);
    CHECK_EQ(chan_send(ch, &s), CHAN_OK);
    big_t r = {{0}, 0};
    CHECK_EQ(chan_recv(ch, &r), CHAN_OK);
    CHECK_EQ(r.id, 99);
    CHECK(memcmp(r.buf, s.buf, sizeof s.buf) == 0);
    chan_destroy(ch);
}

int main(void) {
    test_buffered_send_recv();
    test_ring_wraparound();
    test_unbuffered();
    test_introspection();
    test_large_elem();

    if (g_failures) {
        fprintf(stderr, "%d test(s) failed\n", g_failures);
        return 1;
    }
    printf("test_basic: all passed\n");
    return 0;
}
