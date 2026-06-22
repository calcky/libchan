/*
 * bench_lock_overhead.c
 *
 * 通过对比以下四种实现，量化锁对 channel 吞吐的影响：
 *   1. 基线：无锁的裸内存拷贝（纯 memcpy，单线程）
 *   2. 原子计数：仅用一个 atomic_fetch_add，无 mutex（代表 lock-free 理论上界）
 *   3. 无竞争 mutex：单线程持续 lock→操作→unlock（测 mutex 本身开销）
 *   4. libchan try_send/try_recv 对（真实 channel，单线程无等待路径）
 *   5. libchan 阻塞 send/recv（双线程，涉及真实 park/unpark，代表完整路径）
 *
 * 所有计时均排除 warmup，用 CLOCK_MONOTONIC 纳秒精度。
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

/* ---- 计时工具 ---- */

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

/* ---- 1. 基线：裸 memcpy ---- */
static void bench_memcpy(void) {
    int src = 42, dst = 0;
    /* warmup */
    for (long i = 0; i < WARMUP; i++) memcpy(&dst, &src, sizeof(int));

    int64_t t0 = now_ns();
    for (long i = 0; i < ITERS; i++) {
        memcpy(&dst, &src, sizeof(int));
        __asm__ volatile("" ::: "memory"); /* 阻止编译器完全消除循环 */
    }
    int64_t elapsed = now_ns() - t0;
    printf("%-40s  %8.2f ns/op  %8.2f Mops/s\n",
           "1. 裸 memcpy（基线）",
           ns_per_op(elapsed, ITERS), mops_per_sec(elapsed, ITERS));
    (void)dst;
}

/* ---- 2. 原子计数（lock-free 理论上界） ---- */
static void bench_atomic(void) {
    _Atomic long counter = 0;
    for (long i = 0; i < WARMUP; i++)
        atomic_fetch_add_explicit(&counter, 1, memory_order_relaxed);

    int64_t t0 = now_ns();
    for (long i = 0; i < ITERS; i++)
        atomic_fetch_add_explicit(&counter, 1, memory_order_relaxed);
    int64_t elapsed = now_ns() - t0;
    printf("%-40s  %8.2f ns/op  %8.2f Mops/s\n",
           "2. atomic_fetch_add relaxed（lock-free 上界）",
           ns_per_op(elapsed, ITERS), mops_per_sec(elapsed, ITERS));
}

/* ---- 3. 无竞争 mutex：lock→nop→unlock ---- */
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
           "3. mutex lock+nop+unlock（无竞争）",
           ns_per_op(elapsed, ITERS), mops_per_sec(elapsed, ITERS));
    pthread_mutex_destroy(&mu);
    (void)x;
}

/* ---- 4. 竞争 mutex：2 线程交替持锁 ---- */
typedef struct { pthread_mutex_t *mu; long iters; _Atomic long ready; } contend_arg_t;

static void *contend_worker(void *arg) {
    contend_arg_t *a = arg;
    atomic_fetch_add(&a->ready, 1);
    while (atomic_load(&a->ready) < 2) sched_yield();
    for (long i = 0; i < a->iters; i++) {
        pthread_mutex_lock(a->mu);
        /* 临界区：模拟极短操作 */
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
           "4. mutex lock+nop+unlock（2线程竞争）",
           ns_per_op(elapsed, ITERS / 2), mops_per_sec(elapsed, ITERS / 2));
}

/* ---- 5. libchan try_send+try_recv（单线程，无等待） ---- */
static void bench_chan_try_spsc(void) {
    chan_t *ch = chan_create(sizeof(int), 1024);
    int v = 1, out;

    /* 预热 */
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
           "5. chan try_send+try_recv cap=1024（单线程）",
           ns_per_op(elapsed, ITERS), mops_per_sec(elapsed, ITERS));
    chan_destroy(ch);
    (void)out;
}

/* ---- 6. libchan 阻塞 send/recv（双线程，完整路径） ---- */
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

/* ---- 7. 推算：mutex 在 chan 总延迟中的占比 ---- */
static void print_analysis(void) {
    puts("\n---- 锁开销分析 ----");
    puts("每次 send+recv 涉及 2 次 lock/unlock（send 持锁 + recv 持锁）。");
    puts("无竞争 mutex 约 10–20 ns/次，2 次 ≈ 20–40 ns。");
    puts("chan try_send+try_recv（单线程）完整路径见上方数据。");
    puts("lock 占比 ≈ (2 × mutex_uncontended_ns) / chan_try_ns × 100%。");
    puts("真实阻塞路径还额外包含：waiter 入队、park_wait、唤醒、memcpy 等。");
}

int main(void) {
    printf("CPU: 13th Gen Intel Core i7-13700H（WSL2，LIBCHAN_SPIN_LIMIT=40）\n");
    printf("单次操作定义：send 1 个 int + recv 1 个 int（一个完整数据交换）\n");
    printf("%-40s  %10s  %12s\n", "场景", "ns/op", "Mops/s");
    printf("%-40s  %10s  %12s\n",
           "----------------------------------------",
           "----------", "------------");

    bench_memcpy();
    bench_atomic();
    bench_mutex_uncontended();
    bench_mutex_contended();
    bench_chan_try_spsc();
    bench_chan_blocking(1024, 1, "6a. chan send+recv cap=1024（1+1线程）");
    bench_chan_blocking(0,    1, "6b. chan send+recv 无缓冲（1+1线程）");
    bench_chan_blocking(1024, 2, "6c. chan send+recv cap=1024（2+2线程）");
    bench_chan_blocking(1024, 4, "6d. chan send+recv cap=1024（4+4线程）");

    print_analysis();
    return 0;
}
