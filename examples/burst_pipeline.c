/*
 * burst_pipeline.c — Batch send/recv vs single-element throughput
 *
 * Demonstrates chan_try_send_burst / chan_try_recv_burst: move up to N contiguous
 * elements in one CAS+commit, amortising cross-core fence cost (see doc/benchmarks.md).
 *
 *   producer ──► [buffered channel, cap=BURST*4] ──► consumer
 *
 * Phase A: single chan_try_send / chan_try_recv per item.
 * Phase B: burst size BURST on both sides, same total item count.
 *
 * Both phases use the same SPSC channel and busy-poll loops (no park), so the
 * gap is mostly batching overhead, not scheduling.
 */
#include <stdio.h>
#include <pthread.h>
#include <time.h>

#include "libchan.h"

#define TOTAL   2000000
#define BURST   64
#define CAP     (BURST * 4)

static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

typedef struct { chan_t *ch; double *elapsed_ns; } prod_arg_t;

static void *producer_single(void *arg) {
    prod_arg_t *a = arg;
    int64_t t0 = now_ns();
    for (int i = 0; i < TOTAL; i++) {
        while (chan_try_send(a->ch, &i) != CHAN_OK)
            ;
    }
    *a->elapsed_ns = (double)(now_ns() - t0);
    return NULL;
}

static void *consumer_single(void *arg) {
    chan_t *ch = arg;
    int v;
    for (int i = 0; i < TOTAL; ) {
        if (chan_try_recv(ch, &v) == CHAN_OK)
            i++;
    }
    return NULL;
}

static void *producer_burst(void *arg) {
    prod_arg_t *a = arg;
    int buf[BURST];
    int sent = 0;
    int64_t t0 = now_ns();
    while (sent < TOTAL) {
        int n = TOTAL - sent;
        if (n > BURST) n = BURST;
        for (int i = 0; i < n; i++) buf[i] = sent + i;
        size_t moved;
        while ((moved = chan_try_send_burst(a->ch, buf, (size_t)n)) == 0)
            ;
        sent += (int)moved;
    }
    *a->elapsed_ns = (double)(now_ns() - t0);
    return NULL;
}

static void *consumer_burst(void *arg) {
    chan_t *ch = arg;
    int out[BURST];
    int got = 0;
    while (got < TOTAL) {
        int want = TOTAL - got;
        if (want > BURST) want = BURST;
        size_t moved;
        while ((moved = chan_try_recv_burst(ch, out, (size_t)want)) == 0)
            ;
        got += (int)moved;
    }
    return NULL;
}

static void run_phase(const char *label,
                      void *(*prod_fn)(void *), void *(*cons_fn)(void *)) {
    chan_t *ch = chan_create_spsc(sizeof(int), CAP);
    double prod_ns = 0.0;
    prod_arg_t pa = { ch, &prod_ns };

    pthread_t pt, ct;
    pthread_create(&ct, NULL, cons_fn, ch);
    pthread_create(&pt, NULL, prod_fn, &pa);
    pthread_join(pt, NULL);
    pthread_join(ct, NULL);

    double mops = (double)TOTAL / (prod_ns / 1e3);   /* items per µs → Mops/s */
    printf("  %-22s  producer %.1f ms  %.2f M items/s\n",
           label, prod_ns / 1e6, mops);

    chan_destroy(ch);
}

int main(void) {
    printf("burst_pipeline — SPSC cap=%d, %d items, burst=%d\n\n",
           CAP, TOTAL, BURST);
    run_phase("single try_send/recv", producer_single, consumer_single);
    run_phase("burst send/recv",      producer_burst,  consumer_burst);
    printf("\nBurst batches up to %d slots per ring CAS; expect higher Mops/s.\n", BURST);
    return 0;
}
