/*
 * bench/crosslang/libchan/bench.c
 *
 * 跨语言对比基准 — libchan (C) 端
 *
 * 6 个固定场景（与 Go/Rust 端完全一致）：
 *   S1: cap=0,    1P+1C  (unbuffered rendezvous)
 *   S2: cap=64,   1P+1C
 *   S3: cap=1024, 1P+1C  (SPSC 快路径)
 *   S4: cap=1024, 2P+2C
 *   S5: cap=1024, 4P+4C
 *   S6: cap=1024, 8P+8C
 *
 * 停止机制（与 Go/Rust 等价）：
 *   使用独立 stop 通道（cap=1），通过 chan_close(stop_ch) 广播停止信号。
 *   生产者和消费者都在 chan_select 中同时监听 data_ch 和 stop_ch，
 *   等价于 Go 的 `select { case ch<-v: ... case <-done: }` 和
 *   Rust 的 `select! { send(tx,v)->_ => {} recv(stop_rx)->_ => break }`。
 *
 * 公平性：
 *   三种语言的热路径每次迭代都执行 2-case select，开销量级相当。
 *
 * 输出格式（CSV，被 run_comparison.sh 解析）：
 *   lang,np,nc,cap,mops
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

#define WARMUP_MS  400
#define MEASURE_MS 1500

static inline int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}
static inline void sleep_ms(int ms) {
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

typedef struct {
    chan_t       *data_ch;
    chan_t       *stop_ch;
    _Atomic long *ops;
} targs_t;

static void *producer_fn(void *arg) {
    targs_t *a = arg;
    int v = 0, dummy = 0;
    long cnt = 0;

    chan_select_case_t cases[2];
    cases[1].ch   = a->stop_ch;
    cases[1].op   = CHAN_OP_RECV;
    cases[1].data = &dummy;

    for (;;) {
        cases[0].ch     = a->data_ch;
        cases[0].op     = CHAN_OP_SEND;
        cases[0].data   = &v;
        cases[0].result = CHAN_OK;
        cases[1].result = CHAN_OK;

        int w = chan_select(cases, 2);
        if (w == 1) break;                       /* stop_ch selected */
        if (cases[0].result != CHAN_OK) break;   /* data_ch closed   */
        cnt++; v++;
    }
    atomic_fetch_add_explicit(a->ops, cnt, memory_order_relaxed);
    return NULL;
}

static void *consumer_fn(void *arg) {
    targs_t *a = arg;
    int v = 0, dummy = 0;
    long cnt = 0;

    chan_select_case_t cases[2];
    cases[1].ch   = a->stop_ch;
    cases[1].op   = CHAN_OP_RECV;
    cases[1].data = &dummy;

    for (;;) {
        cases[0].ch     = a->data_ch;
        cases[0].op     = CHAN_OP_RECV;
        cases[0].data   = &v;
        cases[0].result = CHAN_OK;
        cases[1].result = CHAN_OK;

        int w = chan_select(cases, 2);
        if (w == 1) break;                       /* stop_ch selected */
        if (cases[0].result != CHAN_OK) break;   /* data_ch closed   */
        cnt++;
    }
    atomic_fetch_add_explicit(a->ops, cnt, memory_order_relaxed);
    return NULL;
}

static double run_once(int np, int nc, int cap) {
    chan_t *data_ch = chan_create(sizeof(int), (size_t)cap);
    chan_t *stop_ch = chan_create(sizeof(int), 1);   /* cap=1, closed to broadcast */

    _Atomic long psent = 0, precvd = 0;
    atomic_init(&psent, 0);
    atomic_init(&precvd, 0);

    targs_t pa = { data_ch, stop_ch, &psent  };
    targs_t ca = { data_ch, stop_ch, &precvd };

    int n = np + nc;
    pthread_t *thr = malloc(sizeof(pthread_t) * (size_t)n);
    for (int i = 0; i < np; i++) pthread_create(&thr[i],    NULL, producer_fn, &pa);
    for (int i = 0; i < nc; i++) pthread_create(&thr[np+i], NULL, consumer_fn, &ca);

    sleep_ms(WARMUP_MS);
    atomic_store_explicit(&psent,  0, memory_order_relaxed);
    atomic_store_explicit(&precvd, 0, memory_order_relaxed);

    sleep_ms(MEASURE_MS);

    /* 广播停止：chan_close(stop_ch) 使所有 chan_select 中的 stop_ch RECV 立即就绪
     * 等价于 Go 的 close(done) 和 Rust 的 drop(stop_tx) */
    chan_close(stop_ch);

    for (int i = 0; i < n; i++) pthread_join(thr[i], NULL);

    long s = atomic_load_explicit(&psent,  memory_order_relaxed);
    long r = atomic_load_explicit(&precvd, memory_order_relaxed);
    long pairs = s < r ? s : r;

    free(thr);
    chan_destroy(data_ch);
    chan_destroy(stop_ch);
    return (double)pairs / (MEASURE_MS / 1000.0) / 1e6;
}

int main(void) {
    static const int sc[][3] = {
        {1,1,0}, {1,1,64}, {1,1,1024}, {2,2,1024}, {4,4,1024}, {8,8,1024}
    };
    int nsc = (int)(sizeof(sc)/sizeof(sc[0]));

    for (int i = 0; i < nsc; i++) {
        int np  = sc[i][0], nc = sc[i][1], cap = sc[i][2];
        double mops = run_once(np, nc, cap);
        printf("libchan,%d,%d,%d,%.3f\n", np, nc, cap, mops);
        fflush(stdout);
    }
    return 0;
}
