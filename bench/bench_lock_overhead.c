/*
 * bench_lock_overhead.c
 *
 * Quantify the impact of locking on channel throughput by comparing the
 * following implementations:
 *   1. baseline: lock-free raw memory copy (pure memcpy, single-threaded)
 *   2. atomic counter: just one atomic_fetch_add, no mutex (represents the
 *      theoretical lock-free upper bound)
 *   3. uncontended mutex: a single thread repeatedly lock->op->unlock
 *      (measures the mutex cost itself)
 *   4. libchan try_send/try_recv pair (real channel, single-threaded path
 *      with no waiting)
 *   5. libchan blocking send/recv (two threads, exercises real park/unpark,
 *      represents the full path)
 *
 * All timing excludes warmup and uses CLOCK_MONOTONIC nanosecond precision.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdatomic.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <sched.h>

#include "libchan.h"

/* ---- timing utilities ---- */

static inline int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static inline double ns_per_op(int64_t elapsed_ns, long ops) {
    return (double)elapsed_ns / (double)ops;
}

static inline double mops_per_sec(int64_t elapsed_ns, long ops) {
    return (double)ops / ((double)elapsed_ns / 1e9) / 1e6;
}

#define ITERS      5000000L
#define WARMUP     200000L

/* ---- 1. baseline: raw memcpy ---- */
static void bench_memcpy(void) {
    int src = 42, dst = 0;
    /* warmup */
    for (long i = 0; i < WARMUP; i++) memcpy(&dst, &src, sizeof(int));

    int64_t t0 = now_ns();
    for (long i = 0; i < ITERS; i++) {
        memcpy(&dst, &src, sizeof(int));
        __asm__ volatile("" ::: "memory"); /* prevent the compiler from eliminating the loop */
    }
    int64_t elapsed = now_ns() - t0;
    printf("%-40s  %8.2f ns/op  %8.2f Mops/s\n",
           "1. raw memcpy (baseline)",
           ns_per_op(elapsed, ITERS), mops_per_sec(elapsed, ITERS));
    (void)dst;
}

/* ---- 2. atomic counter (theoretical lock-free upper bound) ---- */
static void bench_atomic(void) {
    _Atomic long counter = 0;
    for (long i = 0; i < WARMUP; i++)
        atomic_fetch_add_explicit(&counter, 1, memory_order_relaxed);

    int64_t t0 = now_ns();
    for (long i = 0; i < ITERS; i++)
        atomic_fetch_add_explicit(&counter, 1, memory_order_relaxed);
    int64_t elapsed = now_ns() - t0;
    printf("%-40s  %8.2f ns/op  %8.2f Mops/s\n",
           "2. atomic_fetch_add relaxed (lock-free upper bound)",
           ns_per_op(elapsed, ITERS), mops_per_sec(elapsed, ITERS));
}

/* ---- 3. uncontended mutex: lock->nop->unlock ---- */
static void bench_mutex_uncontended(void) {
    pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
    int x = 0;
    for (long i = 0; i < WARMUP; i++) {
        pthread_mutex_lock(&mu);
        x++;
        pthread_mutex_unlock(&mu);
    }

    int64_t t0 = now_ns();
    for (long i = 0; i < ITERS; i++) {
        pthread_mutex_lock(&mu);
        x++;
        pthread_mutex_unlock(&mu);
    }
    int64_t elapsed = now_ns() - t0;
    printf("%-40s  %8.2f ns/op  %8.2f Mops/s\n",
           "3. mutex lock+nop+unlock (uncontended)",
           ns_per_op(elapsed, ITERS), mops_per_sec(elapsed, ITERS));
    pthread_mutex_destroy(&mu);
    (void)x;
}

/* ---- 4. contended mutex: 2 threads alternate holding the lock ---- */
typedef struct { pthread_mutex_t *mu; long iters; _Atomic long ready; } contend_arg_t;

static void *contend_worker(void *arg) {
    contend_arg_t *a = arg;
    atomic_fetch_add(&a->ready, 1);
    while (atomic_load(&a->ready) < 2) sched_yield();
    for (long i = 0; i < a->iters; i++) {
        pthread_mutex_lock(a->mu);
        /* critical section: simulate an extremely short operation */
        __asm__ volatile("" ::: "memory");
        pthread_mutex_unlock(a->mu);
    }
    return NULL;
}

static void bench_mutex_contended(void) {
    pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
    contend_arg_t arg = { &mu, ITERS / 2, 0 };

    pthread_t t;
    pthread_create(&t, NULL, contend_worker, &arg);

    atomic_fetch_add(&arg.ready, 1);
    while (atomic_load(&arg.ready) < 2) sched_yield();

    int64_t t0 = now_ns();
    for (long i = 0; i < ITERS / 2; i++) {
        pthread_mutex_lock(&mu);
        __asm__ volatile("" ::: "memory");
        pthread_mutex_unlock(&mu);
    }
    int64_t elapsed = now_ns() - t0;

    pthread_join(t, NULL);
    pthread_mutex_destroy(&mu);

    printf("%-40s  %8.2f ns/op  %8.2f Mops/s\n",
           "4. mutex lock+nop+unlock (2 threads contended)",
           ns_per_op(elapsed, ITERS / 2), mops_per_sec(elapsed, ITERS / 2));
}

/* ---- 5. libchan try_send+try_recv (single thread, no waiting) ---- */
static void bench_chan_try_spsc(void) {
    chan_t *ch = chan_create(sizeof(int), 1024);
    int v = 1, out;

    /* warmup */
    for (long i = 0; i < WARMUP; i++) {
        chan_try_send(ch, &v);
        chan_try_recv(ch, &out);
    }

    int64_t t0 = now_ns();
    for (long i = 0; i < ITERS; i++) {
        chan_try_send(ch, &v);
        chan_try_recv(ch, &out);
    }
    int64_t elapsed = now_ns() - t0;
    printf("%-40s  %8.2f ns/op  %8.2f Mops/s\n",
           "5. chan try_send+try_recv cap=1024 (single thread)",
           ns_per_op(elapsed, ITERS), mops_per_sec(elapsed, ITERS));
    chan_destroy(ch);
    (void)out;
}

/* ---- 6. libchan blocking send/recv (two threads, full path) ---- */
typedef struct { chan_t *ch; long iters; } pair_arg_t;

static void *recv_worker(void *arg) {
    pair_arg_t *a = arg;
    int out;
    for (long i = 0; i < a->iters; i++)
        chan_recv(a->ch, &out);
    return NULL;
}

static void bench_chan_blocking(size_t cap, int nthreads, const char *label) {
    chan_t *ch = chan_create(sizeof(int), cap);
    long per_thread = ITERS / nthreads;
    pair_arg_t arg = { ch, per_thread };

    pthread_t *rcv = malloc(nthreads * sizeof(pthread_t));
    for (int i = 0; i < nthreads; i++)
        pthread_create(&rcv[i], NULL, recv_worker, &arg);

    int v = 7;
    int64_t t0 = now_ns();
    for (long i = 0; i < per_thread * nthreads; i++)
        chan_send(ch, &v);
    int64_t elapsed = now_ns() - t0;

    for (int i = 0; i < nthreads; i++) pthread_join(rcv[i], NULL);
    free(rcv);
    chan_destroy(ch);

    char buf[80];
    snprintf(buf, sizeof buf, "%s", label);
    printf("%-40s  %8.2f ns/op  %8.2f Mops/s\n",
           buf, ns_per_op(elapsed, per_thread * nthreads),
           mops_per_sec(elapsed, per_thread * nthreads));
}

/* ---- 7. estimate: mutex share of the total chan latency ---- */
static void print_analysis(void) {
    puts("\n---- lock overhead analysis ----");
    puts("Each send+recv involves 2 lock/unlock pairs (send holds the lock + recv holds the lock).");
    puts("An uncontended mutex is about 10-20 ns each, 2 of them ~= 20-40 ns.");
    puts("See the data above for the full chan try_send+try_recv (single thread) path.");
    puts("lock share ~= (2 x mutex_uncontended_ns) / chan_try_ns x 100%.");
    puts("The real blocking path additionally includes: waiter enqueue, park_wait, wakeup, memcpy, etc.");
}

int main(void) {
    printf("CPU: 13th Gen Intel Core i7-13700H (WSL2, LIBCHAN_SPIN_LIMIT=40)\n");
    printf("single op definition: send 1 int + recv 1 int (one full data exchange)\n");
    printf("%-40s  %10s  %12s\n", "scenario", "ns/op", "Mops/s");
    printf("%-40s  %10s  %12s\n",
           "----------------------------------------",
           "----------", "------------");

    bench_memcpy();
    bench_atomic();
    bench_mutex_uncontended();
    bench_mutex_contended();
    bench_chan_try_spsc();
    bench_chan_blocking(1024, 1, "6a. chan send+recv cap=1024 (1+1 threads)");
    bench_chan_blocking(0,    1, "6b. chan send+recv unbuffered (1+1 threads)");
    bench_chan_blocking(1024, 2, "6c. chan send+recv cap=1024 (2+2 threads)");
    bench_chan_blocking(1024, 4, "6d. chan send+recv cap=1024 (4+4 threads)");

    print_analysis();
    return 0;
}
