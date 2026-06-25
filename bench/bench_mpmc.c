/*
 * bench_mpmc.c
 *
 * Asymmetric N producer + M consumer throughput benchmark (fixed-duration
 * measurement).
 *
 * Each (P, C, cap) combination runs for a fixed duration (MEASURE_MS),
 * counts the completed send/recv operations, and converts
 * min(sends, recvs) into Mops/s.
 * Under high contention this naturally shows up as low throughput rather than
 * hanging.
 *
 * Stop mechanism:
 *   1. set stop=true (each thread checks before every operation)
 *   2. chan_close() (wakes all threads blocked in chan_send/chan_recv)
 *   Both handle CHAN_ERR_CLOSED to ensure threads can exit cleanly.
 */

#define _GNU_SOURCE
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

#include "libchan.h"

/* ------------------------------------------------------------------ */
/* parameters                                                          */
/* ------------------------------------------------------------------ */

#define WARMUP_MS   400
#define MEASURE_MS  1500

static const int producers[] = { 1, 2, 4, 8 };
static const int consumers[] = { 1, 2, 4, 8 };
static const int caps[]      = { 0, 64, 1024 };

#define N_P   (int)(sizeof(producers)/sizeof(producers[0]))
#define N_C   (int)(sizeof(consumers)/sizeof(consumers[0]))
#define N_CAP (int)(sizeof(caps)/sizeof(caps[0]))

/* ------------------------------------------------------------------ */
/* timing                                                              */
/* ------------------------------------------------------------------ */

static inline int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

static inline void sleep_ms(int ms) {
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

/* ------------------------------------------------------------------ */
/* thread arguments                                                    */
/* ------------------------------------------------------------------ */

typedef struct {
    chan_t        *ch;
    _Atomic bool  *stop;
    _Atomic long  *ops;
} targs_t;

static void *producer_fn(void *arg) {
    targs_t *a = arg;
    int v = 0;
    long cnt = 0;
    while (!atomic_load_explicit(a->stop, memory_order_relaxed)) {
        chan_err_t e = chan_send(a->ch, &v);
        if (e != CHAN_OK) break;
        cnt++;
        v++;
    }
    atomic_fetch_add_explicit(a->ops, cnt, memory_order_relaxed);
    return NULL;
}

static void *consumer_fn(void *arg) {
    targs_t *a = arg;
    int v;
    long cnt = 0;
    while (!atomic_load_explicit(a->stop, memory_order_relaxed)) {
        chan_err_t e = chan_recv(a->ch, &v);
        if (e != CHAN_OK) break;
        cnt++;
    }
    atomic_fetch_add_explicit(a->ops, cnt, memory_order_relaxed);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* single test: returns Mops/s                                         */
/* ------------------------------------------------------------------ */

static double run_once(int np, int nc, int cap) {
    chan_t *ch = chan_create(sizeof(int), (size_t)cap);

    _Atomic bool stop   = false;
    _Atomic long psent  = 0;
    _Atomic long precvd = 0;
    atomic_init(&stop,   false);
    atomic_init(&psent,  0);
    atomic_init(&precvd, 0);

    targs_t pa = { ch, &stop, &psent  };
    targs_t ca = { ch, &stop, &precvd };

    int nthr = np + nc;
    pthread_t *thr = malloc(sizeof(pthread_t) * (size_t)nthr);

    for (int i = 0; i < np; i++) pthread_create(&thr[i],    NULL, producer_fn, &pa);
    for (int i = 0; i < nc; i++) pthread_create(&thr[np+i], NULL, consumer_fn, &ca);

    /* warmup */
    sleep_ms(WARMUP_MS);

    /* reset counters, start measuring */
    atomic_store_explicit(&psent,  0, memory_order_relaxed);
    atomic_store_explicit(&precvd, 0, memory_order_relaxed);
    sleep_ms(MEASURE_MS);

    /* stop: set the stop flag first, then close to wake blocked threads */
    atomic_store_explicit(&stop, true, memory_order_seq_cst);
    chan_close(ch);

    for (int i = 0; i < nthr; i++) pthread_join(thr[i], NULL);

    long s = atomic_load_explicit(&psent,  memory_order_relaxed);
    long r = atomic_load_explicit(&precvd, memory_order_relaxed);
    long pairs = s < r ? s : r;

    free(thr);
    chan_destroy(ch);
    return (double)pairs / (MEASURE_MS / 1000.0) / 1e6;
}

/* ------------------------------------------------------------------ */
/* main                                                                 */
/* ------------------------------------------------------------------ */

int main(void) {
    printf("libchan N×M producer-consumer throughput benchmark\n");
    printf("warmup=%dms  measure=%dms  unit=Mops/s (completed send+recv pairs)\n\n",
           WARMUP_MS, MEASURE_MS);

    for (int ci = 0; ci < N_CAP; ci++) {
        int cap = caps[ci];
        char cap_str[32];
        if (cap == 0) snprintf(cap_str, sizeof(cap_str), "0 (unbuffered)");
        else          snprintf(cap_str, sizeof(cap_str), "%d", cap);

        printf("capacity = %s\n", cap_str);
        printf("%8s", "");
        for (int j = 0; j < N_C; j++) printf("  %6dC", consumers[j]);
        printf("\n%8s", "");
        for (int j = 0; j < N_C; j++) printf("  ------");
        printf("\n");

        for (int pi = 0; pi < N_P; pi++) {
            int np = producers[pi];
            printf("  %4dP |", np);
            for (int cj = 0; cj < N_C; cj++) {
                double mops = run_once(np, consumers[cj], cap);
                printf("  %6.2f", mops);
            }
            printf("\n");
        }
        printf("\n");
    }
    return 0;
}
