/*
 * bench_bulk.c — quantify the bulk-API amortisation on the MPMC ring.
 *
 * The 4-cursor rte_ring per-op cost is dominated by cross-core cache-line
 * traffic: each op does a CAS on a head cursor, an acquire-read of the opposite
 * (other-core-written) cursor, and a Phase-3 commit on a tail cursor.  Bulk
 * enqueue/dequeue reserve k slots in ONE CAS and do ONE Phase-3 commit, so that
 * fixed traffic is amortised over k elements — per-element cost should fall
 * roughly ~1/k until the memcpy itself dominates.
 *
 * We report ns/element and Mops/s for batch sizes 1/8/32/128, both
 * single-thread (pure amortisation, no contention) and 2P+2C (amortisation
 * under real cross-core bouncing).  Batch size 1 via the bulk API ≈ the
 * single-element ring_lf_push/pop baseline.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include "ring_lf.h"

static inline int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

#define RING_CAP  1024
#define ELEM_SZ   sizeof(int64_t)

/* ---- single-thread: fill then drain, measuring each phase -------------- */

static double st_throughput(uint32_t batch, long total) {
    chan_ring_lf_t r;
    ring_lf_init(&r, RING_CAP, ELEM_SZ);
    int64_t *buf = malloc((size_t)batch * ELEM_SZ);
    for (uint32_t i = 0; i < batch; i++) buf[i] = (int64_t)i;

    int64_t t0 = now_ns();
    long done = 0;
    while (done < total) {
        /* fill the ring in bursts, then drain it in bursts — keeps the ring
         * hot and exercises wrap, while staying single-threaded. */
        long produced = 0;
        while (produced < RING_CAP) {
            uint32_t k = ring_lf_enqueue_burst(&r, buf, batch);
            if (k == 0) break;
            produced += (long)k;
        }
        long got = 0;
        while (got < produced) {
            uint32_t g = ring_lf_dequeue_burst(&r, buf, batch);
            if (g == 0) break;
            got += (long)g;
        }
        done += got;
    }
    int64_t elapsed = now_ns() - t0;
    free(buf);
    ring_lf_destroy(&r);
    /* one "op" = one element through enqueue + dequeue */
    return (double)done / ((double)elapsed / 1e9) / 1e6;
}

/* ---- 2P+2C contended -------------------------------------------------- */

typedef struct {
    chan_ring_lf_t *r;
    uint32_t        batch;
    long            target;
    _Atomic long    sent;
    _Atomic long    recvd;
} mt_state_t;

static void *mt_producer(void *arg) {
    mt_state_t *s = arg;
    int64_t *buf = malloc((size_t)s->batch * ELEM_SZ);
    memset(buf, 0, (size_t)s->batch * ELEM_SZ);
    for (;;) {
        long claim = atomic_fetch_add_explicit(&s->sent, s->batch, memory_order_relaxed);
        if (claim >= s->target) break;
        /* Cap the final batch so the total enqueued is EXACTLY target — no
         * overshoot, so consumers (which stop at recvd==target) can never exit
         * while a producer is still blocked on a full ring. */
        uint32_t want = s->batch;
        if (claim + (long)want > s->target) want = (uint32_t)(s->target - claim);
        uint32_t off = 0;
        while (off < want) {
            uint32_t k = ring_lf_enqueue_burst(s->r, buf + off, want - off);
            off += k;   /* retry remainder when full */
        }
    }
    free(buf);
    return NULL;
}

static void *mt_consumer(void *arg) {
    mt_state_t *s = arg;
    int64_t *buf = malloc((size_t)s->batch * ELEM_SZ);
    for (;;) {
        if (atomic_load_explicit(&s->recvd, memory_order_relaxed) >= s->target) break;
        uint32_t g = ring_lf_dequeue_burst(s->r, buf, s->batch);
        if (g) atomic_fetch_add_explicit(&s->recvd, (long)g, memory_order_relaxed);
    }
    free(buf);
    return NULL;
}

static double mt_throughput(uint32_t batch, long total, int np) {
    chan_ring_lf_t r;
    ring_lf_init(&r, RING_CAP, ELEM_SZ);
    mt_state_t s = { .r = &r, .batch = batch, .target = total };
    atomic_init(&s.sent, 0);
    atomic_init(&s.recvd, 0);

    pthread_t *th = malloc(sizeof(pthread_t) * (size_t)(np * 2));
    int64_t t0 = now_ns();
    for (int i = 0; i < np; i++) pthread_create(&th[i], NULL, mt_producer, &s);
    for (int i = 0; i < np; i++) pthread_create(&th[np + i], NULL, mt_consumer, &s);
    /* Producers enqueue exactly `target` elements; consumers stop once they
     * have dequeued `target`.  Both terminate on their own — just join. */
    for (int i = 0; i < np * 2; i++) pthread_join(th[i], NULL);
    int64_t elapsed = now_ns() - t0;

    free(th);
    ring_lf_destroy(&r);
    return (double)total / ((double)elapsed / 1e9) / 1e6;
}

int main(void) {
    static const uint32_t batches[] = { 1, 8, 32, 128 };
    const int nb = (int)(sizeof(batches) / sizeof(batches[0]));
    const long ST_TOTAL = 20000000L;
    const long MT_TOTAL = 8000000L;

    printf("MPMC ring bulk-API amortisation  (cap=%d, elem=%zuB)\n", RING_CAP, ELEM_SZ);
    printf("one op = one element through enqueue + dequeue\n\n");

    printf("%-8s  %14s  %14s\n", "batch", "1T Mops/s", "1T ns/elem");
    printf("%-8s  %14s  %14s\n", "-----", "---------", "----------");
    for (int i = 0; i < nb; i++) {
        /* warm */
        st_throughput(batches[i], 1000000L);
        double mops = st_throughput(batches[i], ST_TOTAL);
        printf("%-8u  %14.2f  %14.2f\n", batches[i], mops, 1000.0 / mops);
    }

    printf("\n%-8s  %14s  %14s\n", "batch", "2P2C Mops/s", "2P2C ns/elem");
    printf("%-8s  %14s  %14s\n", "-----", "-----------", "------------");
    for (int i = 0; i < nb; i++) {
        mt_throughput(batches[i], 1000000L, 2);   /* warm */
        double mops = mt_throughput(batches[i], MT_TOTAL, 2);
        printf("%-8u  %14.2f  %14.2f\n", batches[i], mops, 1000.0 / mops);
    }

    printf("\nNote: batch=1 ≈ single-element ring_lf_push/pop baseline;\n");
    printf("      larger batch → amortizes CAS + peer-cursor read + Phase-3 commit\n");
    printf("      per element, until the memcpy itself dominates.\n");
    return 0;
}
