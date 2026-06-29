/*
 * test_burst.c — correctness of the public non-blocking burst API
 * (chan_try_send_burst / chan_try_recv_burst).
 *
 * Covers:
 *   1. single-thread basics: FIFO, clamp at capacity, empty/full → 0,
 *      unbuffered → 0, closed send → 0, drain-after-close on recv;
 *   2. interop with single-element ops (burst send → single recv and back);
 *   3. MPMC stress: burst producers + burst consumers, exactly once;
 *   4. wake interop: burst producers feeding BLOCKING single-element receivers
 *      — a burst must wake a parked receiver (no lost wakeup), the whole point
 *      of mirroring the single-element fast-path wake protocol.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include "libchan.h"

static int g_failures = 0;
#define CHECK(c) do { if (!(c)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); g_failures++; } \
} while (0)

/* ---- 1. single-thread basics ----------------------------------------- */

static void test_basics(void) {
    chan_t *ch = chan_create(sizeof(int), 8);   /* capacity 8 */
    int buf[16];
    for (int i = 0; i < 16; i++) buf[i] = i;

    /* Ask for 16 into an empty cap-8 ring → clamped to 8. */
    CHECK(chan_try_send_burst(ch, buf, 16) == 8);
    CHECK(chan_len(ch) == 8);
    /* Full → 0. */
    CHECK(chan_try_send_burst(ch, buf, 4) == 0);

    /* Dequeue more than available → clamped to 8, FIFO 0..7. */
    int out[16];
    CHECK(chan_try_recv_burst(ch, out, 16) == 8);
    for (int i = 0; i < 8; i++) CHECK(out[i] == i);
    /* Empty → 0. */
    CHECK(chan_try_recv_burst(ch, out, 4) == 0);

    /* n == 0 / NULL are no-ops. */
    CHECK(chan_try_send_burst(ch, buf, 0) == 0);
    CHECK(chan_try_recv_burst(ch, out, 0) == 0);
    CHECK(chan_try_send_burst(ch, NULL, 4) == 0);

    /* Drain-after-close: leftover items still come out, then 0. */
    CHECK(chan_try_send_burst(ch, buf, 5) == 5);
    chan_close(ch);
    CHECK(chan_try_send_burst(ch, buf, 1) == 0);          /* closed send → 0 */
    CHECK(chan_try_recv_burst(ch, out, 16) == 5);         /* drains the 5 */
    for (int i = 0; i < 5; i++) CHECK(out[i] == i);
    CHECK(chan_try_recv_burst(ch, out, 4) == 0);          /* empty + closed → 0 */
    chan_destroy(ch);

    /* Unbuffered → always 0. */
    chan_t *u = chan_create(sizeof(int), 0);
    CHECK(chan_try_send_burst(u, buf, 4) == 0);
    CHECK(chan_try_recv_burst(u, out, 4) == 0);
    chan_destroy(u);
}

/* ---- 2. interop with single-element ops ------------------------------ */

static void test_single_interop(void) {
    chan_t *ch = chan_create(sizeof(int), 16);
    int buf[8];
    for (int i = 0; i < 8; i++) buf[i] = 100 + i;

    /* burst send, single recv: FIFO preserved. */
    CHECK(chan_try_send_burst(ch, buf, 8) == 8);
    for (int i = 0; i < 8; i++) {
        int v;
        CHECK(chan_try_recv(ch, &v) == CHAN_OK);
        CHECK(v == 100 + i);
    }

    /* single send, burst recv. */
    for (int i = 0; i < 6; i++) { int v = 200 + i; CHECK(chan_try_send(ch, &v) == CHAN_OK); }
    int out[8];
    CHECK(chan_try_recv_burst(ch, out, 8) == 6);
    for (int i = 0; i < 6; i++) CHECK(out[i] == 200 + i);
    chan_destroy(ch);
}

/* ---- 3. MPMC burst stress (exactly once) ----------------------------- */

#define N_PROD   4
#define N_CONS   4
#define PER_PROD 50000
#define TOTAL    (N_PROD * PER_PROD)
#define BURST    16

static chan_t      *g_ch;
static _Atomic int  g_recv_count;
static _Atomic int  g_seen[TOTAL];

static void *burst_producer(void *arg) {
    int base = (int)(long)arg * PER_PROD;
    int sent = 0, buf[BURST];
    while (sent < PER_PROD) {
        int want = PER_PROD - sent < BURST ? PER_PROD - sent : BURST;
        for (int i = 0; i < want; i++) buf[i] = base + sent + i;
        sent += (int)chan_try_send_burst(g_ch, buf, (size_t)want);   /* retry remainder */
    }
    return NULL;
}

static void *burst_consumer(void *arg) {
    (void)arg;
    int buf[BURST];
    while (atomic_load_explicit(&g_recv_count, memory_order_relaxed) < TOTAL) {
        size_t g = chan_try_recv_burst(g_ch, buf, BURST);
        for (size_t i = 0; i < g; i++) {
            int v = buf[i];
            CHECK(v >= 0 && v < TOTAL);
            CHECK(atomic_fetch_add_explicit(&g_seen[v], 1, memory_order_relaxed) == 0);
            atomic_fetch_add_explicit(&g_recv_count, 1, memory_order_relaxed);
        }
    }
    return NULL;
}

static void test_mpmc_stress(void) {
    g_ch = chan_create(sizeof(int), 1024);
    atomic_store(&g_recv_count, 0);
    for (int i = 0; i < TOTAL; i++) atomic_store(&g_seen[i], 0);

    pthread_t prod[N_PROD], cons[N_CONS];
    for (int i = 0; i < N_CONS; i++) pthread_create(&cons[i], NULL, burst_consumer, NULL);
    for (int i = 0; i < N_PROD; i++) pthread_create(&prod[i], NULL, burst_producer, (void *)(long)i);
    for (int i = 0; i < N_PROD; i++) pthread_join(prod[i], NULL);
    for (int i = 0; i < N_CONS; i++) pthread_join(cons[i], NULL);

    CHECK(atomic_load(&g_recv_count) == TOTAL);
    for (int i = 0; i < TOTAL; i++) CHECK(atomic_load(&g_seen[i]) == 1);
    chan_destroy(g_ch);
}

/* ---- 4. wake interop: burst send → BLOCKING single recv -------------- */
/* A blocking receiver parks on an empty ring; a burst send must wake it.
 * This exercises the lost-wakeup protocol the burst path mirrors. */

#define WAKE_TOTAL 200000

static chan_t *g_wch;

static void *blocking_consumer(void *arg) {
    long *sum = arg;
    int v;
    while (chan_recv(g_wch, &v) == CHAN_OK) *sum += v;
    return NULL;
}

static void test_blocking_wake(void) {
    g_wch = chan_create(sizeof(int), 64);   /* small ring → receiver parks often */
    long sum = 0, expect = 0;
    pthread_t c;
    pthread_create(&c, NULL, blocking_consumer, &sum);

    int buf[8];
    for (int i = 0; i < 8; i++) buf[i] = 1;
    int sent = 0;
    while (sent < WAKE_TOTAL) {
        int want = WAKE_TOTAL - sent < 8 ? WAKE_TOTAL - sent : 8;
        size_t k = chan_try_send_burst(g_wch, buf, (size_t)want);
        sent += (int)k;   /* spin-retry remainder when the small ring is full */
    }
    expect = WAKE_TOTAL;   /* every element has value 1, exactly WAKE_TOTAL of them */
    chan_close(g_wch);
    pthread_join(c, NULL);

    CHECK(sum == expect);   /* every element delivered exactly once */
    chan_destroy(g_wch);
}

int main(void) {
    test_basics();
    test_single_interop();
    test_mpmc_stress();
    test_blocking_wake();

    if (g_failures) { fprintf(stderr, "%d failed\n", g_failures); return 1; }
    printf("test_burst: all passed\n");
    return 0;
}
