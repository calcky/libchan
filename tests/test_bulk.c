/*
 * test_bulk.c — correctness of the MPMC ring bulk API
 * (ring_lf_enqueue_burst / ring_lf_dequeue_burst).
 *
 * Covers:
 *   1. single-thread fill/drain with a run that WRAPS the power-of-2 boundary,
 *      checking FIFO order and exact counts;
 *   2. partial-burst clamping (free/avail < requested → return clamped count,
 *      0 when full/empty);
 *   3. MPMC stress: several burst producers + burst consumers, every message
 *      delivered exactly once.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include "ring_lf.h"

static int g_failures = 0;
#define CHECK(c) do { if (!(c)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); g_failures++; } \
} while (0)

/* ---- 1. single-thread wrap correctness -------------------------------- */

static void test_wrap_fifo(void) {
    chan_ring_lf_t r;
    CHECK(ring_lf_init(&r, 8, sizeof(int)));   /* capacity rounds to 8 */

    int next_send = 0, next_recv = 0;
    /* Drive the head/tail well past one wrap: many rounds of
     * enqueue 5 / dequeue 5 on a capacity-8 ring forces wrapped runs. */
    for (int round = 0; round < 100; round++) {
        int sbuf[5];
        for (int i = 0; i < 5; i++) sbuf[i] = next_send + i;
        uint32_t k = ring_lf_enqueue_burst(&r, sbuf, 5);
        CHECK(k == 5);                 /* always room: <=3 left from prev round */
        next_send += (int)k;

        int rbuf[5];
        uint32_t g = ring_lf_dequeue_burst(&r, rbuf, 5);
        CHECK(g == 5);
        for (uint32_t i = 0; i < g; i++) {
            CHECK(rbuf[i] == next_recv);   /* strict FIFO across wrap */
            next_recv++;
        }
    }
    CHECK(next_send == next_recv);
    ring_lf_destroy(&r);
}

/* ---- 2. partial clamp / full / empty ---------------------------------- */

static void test_clamp(void) {
    chan_ring_lf_t r;
    CHECK(ring_lf_init(&r, 8, sizeof(int)));   /* capacity 8 */

    int buf[16];
    for (int i = 0; i < 16; i++) buf[i] = i;

    /* Ask for 16 into an empty cap-8 ring → clamped to 8. */
    uint32_t k = ring_lf_enqueue_burst(&r, buf, 16);
    CHECK(k == 8);
    CHECK(ring_lf_count(&r) == 8);

    /* Now full → enqueue returns 0. */
    CHECK(ring_lf_enqueue_burst(&r, buf, 4) == 0);

    /* Dequeue more than available → clamped to 8, FIFO 0..7. */
    int out[16];
    uint32_t g = ring_lf_dequeue_burst(&r, out, 16);
    CHECK(g == 8);
    for (uint32_t i = 0; i < g; i++) CHECK(out[i] == (int)i);

    /* Empty → dequeue returns 0. */
    CHECK(ring_lf_dequeue_burst(&r, out, 4) == 0);

    /* n == 0 is a no-op both ways. */
    CHECK(ring_lf_enqueue_burst(&r, buf, 0) == 0);
    CHECK(ring_lf_dequeue_burst(&r, out, 0) == 0);

    ring_lf_destroy(&r);
}

/* ---- 3. MPMC burst stress --------------------------------------------- */

#define N_PROD 4
#define N_CONS 4
#define PER_PROD 50000
#define TOTAL (N_PROD * PER_PROD)
#define BURST 16

static chan_ring_lf_t g_ring;
static _Atomic int g_recv_count;
static _Atomic int g_seen[TOTAL];   /* each id marked exactly once */

static void *burst_producer(void *arg) {
    int id = (int)(long)arg;
    int base = id * PER_PROD;
    int sent = 0;
    int buf[BURST];
    while (sent < PER_PROD) {
        uint32_t want = PER_PROD - sent < BURST ? (uint32_t)(PER_PROD - sent) : BURST;
        for (uint32_t i = 0; i < want; i++) buf[i] = base + sent + (int)i;
        uint32_t k = ring_lf_enqueue_burst(&g_ring, buf, want);
        sent += (int)k;   /* k may be < want when ring is full; retry the rest */
    }
    return NULL;
}

static void *burst_consumer(void *arg) {
    (void)arg;
    int buf[BURST];
    for (;;) {
        if (atomic_load_explicit(&g_recv_count, memory_order_relaxed) >= TOTAL)
            break;
        uint32_t g = ring_lf_dequeue_burst(&g_ring, buf, BURST);
        for (uint32_t i = 0; i < g; i++) {
            int v = buf[i];
            CHECK(v >= 0 && v < TOTAL);
            int prev = atomic_fetch_add_explicit(&g_seen[v], 1, memory_order_relaxed);
            CHECK(prev == 0);   /* delivered exactly once */
            atomic_fetch_add_explicit(&g_recv_count, 1, memory_order_relaxed);
        }
    }
    return NULL;
}

static void test_mpmc_stress(void) {
    CHECK(ring_lf_init(&g_ring, 1024, sizeof(int)));
    atomic_store(&g_recv_count, 0);
    for (int i = 0; i < TOTAL; i++) atomic_store(&g_seen[i], 0);

    pthread_t prod[N_PROD], cons[N_CONS];
    for (int i = 0; i < N_CONS; i++)
        pthread_create(&cons[i], NULL, burst_consumer, NULL);
    for (int i = 0; i < N_PROD; i++)
        pthread_create(&prod[i], NULL, burst_producer, (void *)(long)i);

    for (int i = 0; i < N_PROD; i++) pthread_join(prod[i], NULL);
    for (int i = 0; i < N_CONS; i++) pthread_join(cons[i], NULL);

    CHECK(atomic_load(&g_recv_count) == TOTAL);
    for (int i = 0; i < TOTAL; i++) CHECK(atomic_load(&g_seen[i]) == 1);
    ring_lf_destroy(&g_ring);
}

int main(void) {
    test_wrap_fifo();
    test_clamp();
    test_mpmc_stress();

    if (g_failures) { fprintf(stderr, "%d failed\n", g_failures); return 1; }
    printf("test_bulk: all passed\n");
    return 0;
}
