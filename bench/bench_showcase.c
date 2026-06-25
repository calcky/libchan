/*
 * bench_showcase.c — libchan performance showcase benchmark (performance ladder + A/B tiers)
 *
 * Purpose: showcase libchan performance externally. The core narrative is a
 * "performance ladder" that lets readers see at a glance that libchan's
 * lock-free fast path is close to the hardware floor, and that the slow parts
 * (park) are OS overhead, not the library's.
 *
 *   Tier A · almost no park (measures "how fast the channel itself is"):
 *     1. bare memcpy          —— hardware floor
 *     2. atomic_fetch_add     —— lock-free primitive lower bound
 *     3. lock-free ring pure queue —— the queue data structure itself (no chan)
 *     4. chan try_send/recv   —— + channel semantics (no wait)
 *     5. chan SPSC cross-core steady-state —— true concurrency, lock-free fast
 *        path with busy-poll and no park (cross-core handoff)
 *
 *   Tier B · always park (measures the end-to-end latency of "the channel as a
 *   synchronization primitive", including OS scheduling):
 *     6. chan SPSC blocking cap=1024
 *     7. chan unbuffered rendezvous
 *     8. chan MPMC 4P+4C cap=1024
 *
 * Measurement: each data point runs BENCH_REPEAT times, reporting the [median]
 * and [min] (min ≈ interference-free lower bound). Timed with CLOCK_MONOTONIC,
 * warmup always excluded.
 *
 * Note: Tier B numbers are heavily affected by OS scheduling and are a
 * trend-only reference; serious measurement needs native Linux + core pinning
 * (see bench/run_showcase.sh). Jitter is significant on WSL2.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdatomic.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <sched.h>

#include "libchan.h"
#include "ring_lf.h"   /* pure queue layer, from src/ (CMake already adds src to include) */

/* ── timing utilities ─────────────────────────────────────────────── */
static inline int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* ── median-of-K measurement skeleton ─────────────────────────────────
 * bench_fn runs one full round and returns that round's ns/op. run_point runs
 * K times, sorts, and reports the median and min, converting Mops/s from the
 * median. */
#define BENCH_REPEAT 7

typedef double (*bench_fn)(void *ctx);

static int cmp_double(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

static void run_point(const char *label, bench_fn fn, void *ctx) {
    double s[BENCH_REPEAT];
    for (int i = 0; i < BENCH_REPEAT; i++) s[i] = fn(ctx);
    qsort(s, BENCH_REPEAT, sizeof(double), cmp_double);
    double med = s[BENCH_REPEAT / 2];
    double mn  = s[0];
    double mops = 1e3 / med;   /* ns/op → Mops/s: (1/med) op/ns × 1e9 / 1e6 */
    printf("%-38s  %9.2f  %9.2f  %9.2f\n", label, med, mn, mops);
}

/* ── Tier A ───────────────────────────────────────────────────── */

#define A_ITERS 5000000L
#define A_WARMUP 200000L

static double bench_memcpy(void *ctx) {
    (void)ctx;
    int src = 42, dst = 0;
    for (long i = 0; i < A_WARMUP; i++) { memcpy(&dst, &src, sizeof(int)); }
    int64_t t0 = now_ns();
    for (long i = 0; i < A_ITERS; i++) {
        memcpy(&dst, &src, sizeof(int));
        __asm__ volatile("" ::: "memory");
    }
    int64_t dt = now_ns() - t0;
    (void)dst;
    return (double)dt / A_ITERS;
}

static double bench_atomic(void *ctx) {
    (void)ctx;
    _Atomic long c = 0;
    for (long i = 0; i < A_WARMUP; i++) atomic_fetch_add_explicit(&c, 1, memory_order_relaxed);
    int64_t t0 = now_ns();
    for (long i = 0; i < A_ITERS; i++) atomic_fetch_add_explicit(&c, 1, memory_order_relaxed);
    int64_t dt = now_ns() - t0;
    return (double)dt / A_ITERS;
}

/* lock-free ring pure queue: single-thread push one / pop one, entirely
 * bypassing chan and never parking. */
static double bench_ring_pure(void *ctx) {
    (void)ctx;
    chan_ring_lf_t r;
    ring_lf_init(&r, 1024, sizeof(int));
    int v = 7, out;
    for (long i = 0; i < A_WARMUP; i++) { ring_lf_push(&r, &v); ring_lf_pop(&r, &out); }
    int64_t t0 = now_ns();
    for (long i = 0; i < A_ITERS; i++) { ring_lf_push(&r, &v); ring_lf_pop(&r, &out); }
    int64_t dt = now_ns() - t0;
    ring_lf_destroy(&r);
    (void)out;
    return (double)dt / A_ITERS;
}

/* chan try_send + try_recv: adds channel semantics (closed check, waiter-count
 * gate, etc.), but no waiting. */
static double bench_chan_try(void *ctx) {
    (void)ctx;
    chan_t *ch = chan_create(sizeof(int), 1024);
    int v = 1, out;
    for (long i = 0; i < A_WARMUP; i++) { chan_try_send(ch, &v); chan_try_recv(ch, &out); }
    int64_t t0 = now_ns();
    for (long i = 0; i < A_ITERS; i++) { chan_try_send(ch, &v); chan_try_recv(ch, &out); }
    int64_t dt = now_ns() - t0;
    chan_destroy(ch);
    (void)out;
    return (double)dt / A_ITERS;
}

/* chan SPSC steady-state (Tier A's star): both threads use try_ + spin, never
 * calling blocking send/recv, so the waiter count stays 0, send/recv always hit
 * the lock-free fast path, and nobody parks. Measures pure fast-path throughput
 * under true concurrency.
 * (Using a blocking recv would park when the ring is occasionally empty →
 *  recv_waiter_cnt>0 → kicking the producer off the fast path too, which would
 *  then measure the park path, see Tier B.) */
#define SS_CAP    4096
#define SS_MSGS   8000000L

typedef struct { chan_t *ch; _Atomic int done; } ss_ctx_t;

static void *ss_consumer(void *arg) {
    ss_ctx_t *s = arg;
    int out;
    for (;;) {
        if (chan_try_recv(s->ch, &out) == CHAN_OK) continue;     /* got one, keep going */
        if (atomic_load_explicit(&s->done, memory_order_acquire)) {
            /* producer finished: drain clean, then exit */
            if (chan_try_recv(s->ch, &out) == CHAN_OK) continue;
            break;
        }
        __asm__ volatile("" ::: "memory");                       /* spin */
    }
    return NULL;
}

static double bench_chan_steady(void *ctx) {
    bool spsc = *(const bool *)ctx;
    chan_t *ch = spsc ? chan_create_spsc(sizeof(int), SS_CAP)   /* cursor-caching fast path */
                      : chan_create(sizeof(int), SS_CAP);        /* MPMC: cursor bounces per message */
    ss_ctx_t s = { ch, 0 };
    pthread_t c;
    pthread_create(&c, NULL, ss_consumer, &s);

    int v = 0;
    int64_t t0 = now_ns();
    for (long i = 0; i < SS_MSGS; i++) {
        while (chan_try_send(ch, &v) != CHAN_OK)                 /* spin if full, no park */
            __asm__ volatile("" ::: "memory");
        v++;
    }
    int64_t dt = now_ns() - t0;

    atomic_store_explicit(&s.done, 1, memory_order_release);
    pthread_join(c, NULL);
    chan_destroy(ch);
    return (double)dt / SS_MSGS;
}

/* ── Tier B: blocking path (includes park + OS scheduling) ──────────────── */

#define B_MSGS 2000000L

typedef struct { chan_t *ch; long k; } prod_t;

static void *b_consumer(void *arg) {
    chan_t *ch = arg;
    int out;
    while (chan_recv(ch, &out) == CHAN_OK) { }
    return NULL;
}
static void *b_producer(void *arg) {
    prod_t *p = arg;
    int v = 0;
    for (long i = 0; i < p->k; i++) { chan_send(p->ch, &v); v++; }
    return NULL;
}

/* generic blocking measurement: np producers each send k messages, nc consumers
 * drain until close. Returns the average ns per message. cfg passed via ctx. */
typedef struct { int cap, np, nc; long total; bool spsc; } bcfg_t;

static double bench_blocking(void *ctx) {
    bcfg_t *c = ctx;
    long k = c->total / c->np;
    long total = k * c->np;
    chan_t *ch = c->spsc ? chan_create_spsc(sizeof(int), (size_t)c->cap)
                         : chan_create(sizeof(int), (size_t)c->cap);
    pthread_t pt[16], ct[16];
    prod_t pa[16];

    for (int i = 0; i < c->nc; i++) pthread_create(&ct[i], NULL, b_consumer, ch);
    int64_t t0 = now_ns();
    for (int i = 0; i < c->np; i++) {
        pa[i] = (prod_t){ ch, k };
        pthread_create(&pt[i], NULL, b_producer, &pa[i]);
    }
    for (int i = 0; i < c->np; i++) pthread_join(pt[i], NULL);
    chan_close(ch);
    for (int i = 0; i < c->nc; i++) pthread_join(ct[i], NULL);
    int64_t dt = now_ns() - t0;
    chan_destroy(ch);
    return (double)dt / total;
}

int main(void) {
    setvbuf(stdout, NULL, _IONBF, 0);

    printf("libchan performance showcase benchmark  (median/min over %d runs per point)\n", BENCH_REPEAT);
    printf("single operation = the equivalent work of one send + one recv\n\n");

    printf("=== Tier A · performance ladder (almost no park, measures the channel itself) ===\n");
    printf("%-38s  %9s  %9s  %9s\n", "scenario", "med ns", "min ns", "Mops/s");
    printf("%-38s  %9s  %9s  %9s\n", "------------------------------------",
           "--------", "--------", "--------");
    run_point("1. bare memcpy (hardware floor)",        bench_memcpy,           NULL);
    run_point("2. atomic_fetch_add (lock-free bound)", bench_atomic,           NULL);
    run_point("3. lock-free ring pure queue",           bench_ring_pure,        NULL);
    run_point("4. chan try_send/recv (no wait)", bench_chan_try,         NULL);
    bool steady_mpmc = false, steady_spsc = true;
    run_point("5. chan MPMC cross-core steady-state (cache-coherence wall)", bench_chan_steady, &steady_mpmc);
    run_point("6. chan SPSC cross-core steady-state (cursor caching breaks the wall)", bench_chan_steady, &steady_spsc);

    printf("\n=== Tier B · blocking latency (includes park + OS scheduling, trend-only reference) ===\n");
    printf("%-38s  %9s  %9s  %9s\n", "scenario", "med ns", "min ns", "Mops/s");
    printf("%-38s  %9s  %9s  %9s\n", "------------------------------------",
           "--------", "--------", "--------");
    bcfg_t b6 = { 1024, 1, 1, B_MSGS, true  };   /* true SPSC blocking */
    bcfg_t b7 = { 0,    1, 1, B_MSGS, false };
    bcfg_t b8 = { 1024, 4, 4, B_MSGS, false };
    run_point("7. chan SPSC blocking cap=1024",       bench_blocking, &b6);
    run_point("8. chan unbuffered rendezvous",        bench_blocking, &b7);
    run_point("9. chan MPMC 4P+4C cap=1024",      bench_blocking, &b8);

    printf("\nNote: Tier A send/recv hit the lock-free fast path and nobody sleeps, reflecting the channel's own overhead;\n");
    printf("      in Tier B each operation may park, measuring end-to-end synchronization latency including OS scheduling.\n");
    return 0;
}
