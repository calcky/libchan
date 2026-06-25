/*
 * bench_showcase.c — libchan 性能展示基准（性能阶梯 + A/B 分档）
 *
 * 目的:对外展示 libchan 性能。核心叙事是一条"性能阶梯",让读者一眼看出
 * libchan 的无锁快路径贴近硬件极限,且慢的地方(park)是 OS 的开销、不是库的。
 *
 *   A 档 · 几乎不 park（测 "channel 自己有多快"）:
 *     1. 裸 memcpy            —— 硬件极限
 *     2. atomic_fetch_add     —— 无锁原语下界
 *     3. 无锁 ring 纯队列      —— 队列数据结构本身(不经 chan)
 *     4. chan try_send/recv   —— + channel 语义(无等待)
 *     5. chan SPSC 跨核稳态  —— 真并发、busy-poll 不 park 的无锁快路径(跨核传递)
 *
 *   B 档 · 必然 park（测 "channel 作为同步原语"的端到端延迟,含 OS 调度）:
 *     6. chan SPSC 阻塞 cap=1024
 *     7. chan 无缓冲 rendezvous
 *     8. chan MPMC 4P+4C cap=1024
 *
 * 测量:每个数据点跑 BENCH_REPEAT 次,报【中位数】与【min】(min ≈ 无干扰下界)。
 * 计时用 CLOCK_MONOTONIC,均排除 warmup。
 *
 * 注意:B 档数字受 OS 调度影响大,仅量级参考;严肃测量需原生 Linux + 钉核
 * (见 bench/run_showcase.sh)。WSL2 上抖动明显。
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
#include "ring_lf.h"   /* 纯队列层,来自 src/(CMake 已把 src 加入 include) */

/* ── 计时工具 ─────────────────────────────────────────────── */
static inline int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* ── median-of-K 测量骨架 ─────────────────────────────────────
 * bench_fn 跑一整轮、返回该轮的 ns/op。run_point 跑 K 次,排序后报
 * 中位数与 min,并据中位数换算 Mops/s。 */
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

/* ── A 档 ───────────────────────────────────────────────────── */

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

/* 无锁 ring 纯队列:单线程 push 一个 / pop 一个,完全不经 chan、不 park。 */
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

/* chan try_send + try_recv:加上 channel 语义(closed 检查、waiter 计数门槛等),但无等待。 */
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

/* chan SPSC 稳态(A 档主角):两个线程都用 try_ + 自旋,从不调用阻塞 send/recv,
 * 因此 waiter 计数恒为 0、收发始终命中无锁快路径、谁都不 park。测真并发下纯快路径吞吐。
 * (用阻塞 recv 会在环偶发为空时 park → recv_waiter_cnt>0 → 把生产者也踢下快路径,
 *  那样测的就是 park 路径了,见 B 档。) */
#define SS_CAP    4096
#define SS_MSGS   8000000L

typedef struct { chan_t *ch; _Atomic int done; } ss_ctx_t;

static void *ss_consumer(void *arg) {
    ss_ctx_t *s = arg;
    int out;
    for (;;) {
        if (chan_try_recv(s->ch, &out) == CHAN_OK) continue;     /* 拿到就继续 */
        if (atomic_load_explicit(&s->done, memory_order_acquire)) {
            /* 生产者已结束:再 drain 干净后退出 */
            if (chan_try_recv(s->ch, &out) == CHAN_OK) continue;
            break;
        }
        __asm__ volatile("" ::: "memory");                       /* 空转 */
    }
    return NULL;
}

static double bench_chan_steady(void *ctx) {
    bool spsc = *(const bool *)ctx;
    chan_t *ch = spsc ? chan_create_spsc(sizeof(int), SS_CAP)   /* 游标缓存快路径 */
                      : chan_create(sizeof(int), SS_CAP);        /* MPMC：每条弹跳游标 */
    ss_ctx_t s = { ch, 0 };
    pthread_t c;
    pthread_create(&c, NULL, ss_consumer, &s);

    int v = 0;
    int64_t t0 = now_ns();
    for (long i = 0; i < SS_MSGS; i++) {
        while (chan_try_send(ch, &v) != CHAN_OK)                 /* 满则自旋,不 park */
            __asm__ volatile("" ::: "memory");
        v++;
    }
    int64_t dt = now_ns() - t0;

    atomic_store_explicit(&s.done, 1, memory_order_release);
    pthread_join(c, NULL);
    chan_destroy(ch);
    return (double)dt / SS_MSGS;
}

/* ── B 档:阻塞路径(含 park + OS 调度)──────────────────────── */

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

/* 通用阻塞测量:np 个生产者各发 k 条,nc 个消费者 drain 到 close。
 * 返回每条消息的平均 ns。cfg 通过 ctx 传入。 */
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

    printf("libchan 性能展示基准  (每点取 %d 次的 中位数/min)\n", BENCH_REPEAT);
    printf("单次操作 = 一次 send + 一次 recv 的等价工作\n\n");

    printf("=== A 档 · 性能阶梯（几乎不 park，测 channel 本身）===\n");
    printf("%-38s  %9s  %9s  %9s\n", "场景", "中位 ns", "min ns", "Mops/s");
    printf("%-38s  %9s  %9s  %9s\n", "------------------------------------",
           "--------", "--------", "--------");
    run_point("1. 裸 memcpy（硬件极限）",        bench_memcpy,           NULL);
    run_point("2. atomic_fetch_add（无锁下界）", bench_atomic,           NULL);
    run_point("3. 无锁 ring 纯队列",           bench_ring_pure,        NULL);
    run_point("4. chan try_send/recv（无等待）", bench_chan_try,         NULL);
    bool steady_mpmc = false, steady_spsc = true;
    run_point("5. chan MPMC 跨核稳态（缓存一致性墙）", bench_chan_steady, &steady_mpmc);
    run_point("6. chan SPSC 跨核稳态（游标缓存破墙）", bench_chan_steady, &steady_spsc);

    printf("\n=== B 档 · 阻塞延迟（含 park + OS 调度，仅量级参考）===\n");
    printf("%-38s  %9s  %9s  %9s\n", "场景", "中位 ns", "min ns", "Mops/s");
    printf("%-38s  %9s  %9s  %9s\n", "------------------------------------",
           "--------", "--------", "--------");
    bcfg_t b6 = { 1024, 1, 1, B_MSGS, true  };   /* 真 SPSC 阻塞 */
    bcfg_t b7 = { 0,    1, 1, B_MSGS, false };
    bcfg_t b8 = { 1024, 4, 4, B_MSGS, false };
    run_point("7. chan SPSC 阻塞 cap=1024",       bench_blocking, &b6);
    run_point("8. chan 无缓冲 rendezvous",        bench_blocking, &b7);
    run_point("9. chan MPMC 4P+4C cap=1024",      bench_blocking, &b8);

    printf("\n说明: A 档收发命中无锁快路径、谁都不睡,反映 channel 本身的开销;\n");
    printf("      B 档每次操作可能 park,测的是含 OS 调度的端到端同步延迟。\n");
    return 0;
}
