/*
 * bench/crosslang/libchan/bench.c
 *
 * Cross-language comparison benchmark — libchan (C) side
 *
 * Methodology: fixed message count (not fixed duration); measure the wall-clock
 * time to complete all sends and receives.
 *   - Each producer sends a fixed K messages; each consumer keeps receiving until
 *     the channel closes -> sent and received counts are exactly equal.
 *   - Timing: from when producers start sending until all consumers have finished
 *     (the channel is fully drained).
 *   - Throughput = total messages / wall-clock seconds / 1e6 (Mops/s).
 *
 * Duration calibration: first run once for a fixed time with a small message count
 *   to estimate throughput, then use that estimate to calibrate the real measurement's
 *   message count to the target duration (~1.5s, not too short), avoiding wildly
 *   different durations across scenarios.
 *
 * Three variants (each emits one CSV row):
 *   direct — producer chan_send / consumer chan_recv (core path, exact, no loss)
 *   spsc   — same as direct, but the channel is created via chan_create_spsc
 *            (single-producer single-consumer fast path: cursor caching + no per-op
 *            fence). Only meaningful for, and emitted for, the 1P1C scenarios.
 *   select — producer/consumer each run one 2-case chan_select (with a dummy second
 *            case that is never ready), matching Go select / Rust select!.
 *            Note: select has a known tiny counting deviation under MPMC (>=2P+2C)
 *            (about 0.01%, see the Select known-limitations section in doc/design.md);
 *            it does not affect the throughput order of magnitude.
 *
 * Output (CSV): lang,np,nc,cap,mops    lang in {libchan_direct, libchan_spsc, libchan_select}
 */

#define _GNU_SOURCE
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>

#include "libchan.h"

#define TARGET_SEC   1.5      /* target duration for the real measurement */
#define CALIB_MSGS   200000L  /* small message count used for calibration */
#define MIN_MSGS     200000L
#define MAX_MSGS     80000000L

static inline int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static chan_t *g_dummy;   /* unbuffered, no sender, never closed during the run -> select's second case is never ready */

typedef struct { chan_t *ch; long k; }              prod_arg_t;
typedef struct { chan_t *ch; _Atomic long *recvd; } cons_arg_t;

/* ── direct variant ──────────────────────────────────────────────────────── */
static void *producer_direct(void *arg) {
    prod_arg_t *a = arg;
    int v = 0;
    for (long i = 0; i < a->k; i++) { chan_send(a->ch, &v); v++; }
    return NULL;
}
static void *consumer_direct(void *arg) {
    cons_arg_t *a = arg;
    int v;
    long c = 0;
    while (chan_recv(a->ch, &v) == CHAN_OK) c++;
    atomic_fetch_add_explicit(a->recvd, c, memory_order_relaxed);
    return NULL;
}

/* ── select variant ──────────────────────────────────────────────────────── */
static void *producer_select(void *arg) {
    prod_arg_t *a = arg;
    int v = 0, dummy = 0;
    for (long i = 0; i < a->k; i++) {
        chan_select_case_t cs[2] = {
            { a->ch,   CHAN_OP_SEND, &v,     CHAN_OK },
            { g_dummy, CHAN_OP_RECV, &dummy, CHAN_OK },
        };
        chan_select(cs, 2);
        v++;
    }
    return NULL;
}
static void *consumer_select(void *arg) {
    cons_arg_t *a = arg;
    int v, dummy;
    long c = 0;
    for (;;) {
        chan_select_case_t cs[2] = {
            { a->ch,   CHAN_OP_RECV, &v,     CHAN_OK },
            { g_dummy, CHAN_OP_RECV, &dummy, CHAN_OK },
        };
        int w = chan_select(cs, 2);
        if (w == 0 && cs[0].result == CHAN_OK)     { c++; continue; }
        if (cs[0].result == CHAN_ERR_CLOSED)        break;
    }
    atomic_fetch_add_explicit(a->recvd, c, memory_order_relaxed);
    return NULL;
}

/* ── Run one round with a fixed message count, return wall-clock nanoseconds; optionally verify the exact count ── */
static int64_t run_msgs(int np, int nc, int cap, long total,
                        void *(*pf)(void *), void *(*cf)(void *),
                        bool check, bool spsc, long *got_out) {
    long k = total / np;
    total = k * np;

    chan_t *ch = spsc ? chan_create_spsc(sizeof(int), (size_t)cap)
                      : chan_create(sizeof(int), (size_t)cap);
    _Atomic long recvd;
    atomic_init(&recvd, 0);

    prod_arg_t pa[64];
    cons_arg_t ca[64];
    pthread_t  pt[64], ct[64];

    for (int i = 0; i < nc; i++) {
        ca[i] = (cons_arg_t){ ch, &recvd };
        pthread_create(&ct[i], NULL, cf, &ca[i]);
    }
    int64_t t0 = now_ns();
    for (int i = 0; i < np; i++) {
        pa[i] = (prod_arg_t){ ch, k };
        pthread_create(&pt[i], NULL, pf, &pa[i]);
    }
    for (int i = 0; i < np; i++) pthread_join(pt[i], NULL);
    chan_close(ch);
    for (int i = 0; i < nc; i++) pthread_join(ct[i], NULL);
    int64_t dt = now_ns() - t0;

    long got = atomic_load_explicit(&recvd, memory_order_relaxed);
    if (got_out) *got_out = got;
    if (check && got != total) {
        /* direct must be exact; select MPMC has a known tiny deviation, so only warn. */
        fprintf(stderr, "  [warn] np=%d nc=%d cap=%d: expected %ld received %ld (diff %+ld)\n",
                np, nc, cap, total, got, got - total);
    }
    chan_destroy(ch);
    return dt;
}

/* ── Calibrate + real measurement, return Mops/s ──────────────────────────── */
static double measure(int np, int nc, int cap,
                      void *(*pf)(void *), void *(*cf)(void *), bool check, bool spsc) {
    /* 1) Calibration: estimate throughput with a small message count */
    int64_t cdt = run_msgs(np, nc, cap, CALIB_MSGS, pf, cf, false, spsc, NULL);
    double calib_total = (double)((CALIB_MSGS / np) * np);
    double rate = calib_total / ((double)cdt / 1e9);     /* msgs/sec */

    /* 2) Calibrate the real message count to the target duration */
    long msgs = (long)(rate * TARGET_SEC);
    if (msgs < MIN_MSGS) msgs = MIN_MSGS;
    if (msgs > MAX_MSGS) msgs = MAX_MSGS;

    /* 3) Real measurement */
    long got = 0;
    int64_t dt = run_msgs(np, nc, cap, msgs, pf, cf, check, spsc, &got);
    long total = (msgs / np) * np;
    return (double)total / ((double)dt / 1e9) / 1e6;
}

int main(void) {
    g_dummy = chan_create(sizeof(int), 0);

    static const int sc[][3] = {
        {1,1,0}, {1,1,64}, {1,1,1024}, {2,2,1024}, {4,4,1024}, {8,8,1024}
    };
    int nsc = (int)(sizeof(sc) / sizeof(sc[0]));

    for (int i = 0; i < nsc; i++) {
        int np = sc[i][0], nc = sc[i][1], cap = sc[i][2];
        double d = measure(np, nc, cap, producer_direct, consumer_direct, true, false);
        printf("libchan_direct,%d,%d,%d,%.3f\n", np, nc, cap, d);
        fflush(stdout);
    }
    /* SPSC fast path: only meaningful for single-producer single-consumer [buffered]
     * channels (contract: 1P+1C; when cap==0, chan_create_spsc is equivalent to
     * chan_create with no fast path, so skip it to avoid misleading results). Reuses
     * direct's send/recv functions (chan_send/chan_recv dispatch internally on ch->spsc). */
    for (int i = 0; i < nsc; i++) {
        int np = sc[i][0], nc = sc[i][1], cap = sc[i][2];
        if (np != 1 || nc != 1 || cap == 0) continue;
        double d = measure(np, nc, cap, producer_direct, consumer_direct, true, true);
        printf("libchan_spsc,%d,%d,%d,%.3f\n", np, nc, cap, d);
        fflush(stdout);
    }
    for (int i = 0; i < nsc; i++) {
        int np = sc[i][0], nc = sc[i][1], cap = sc[i][2];
        double s = measure(np, nc, cap, producer_select, consumer_select, false, false);
        printf("libchan_select,%d,%d,%d,%.3f\n", np, nc, cap, s);
        fflush(stdout);
    }

    chan_destroy(g_dummy);
    return 0;
}
