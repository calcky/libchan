/*
 * bench_mpmc.c
 *
 * 非对称 N producer + M consumer 吞吐基准（固定时长测量）。
 *
 * 每个 (P, C, cap) 组合运行固定时长（MEASURE_MS），统计完成的
 * send/recv 次数，取 min(sends, recvs) 换算为 Mops/s。
 * 高竞争时自然表现为低吞吐，不会卡住。
 *
 * 停止方式：
 *   1. 置 stop=true（线程在每次操作前检查）
 *   2. chan_close()（唤醒所有阻塞在 chan_send/chan_recv 的线程）
 *   两者都处理 CHAN_ERR_CLOSED，确保线程能够正常退出。
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
/* 参数                                                                 */
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
/* 计时                                                                 */
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
/* 线程参数                                                             */
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
/* 单次测试：返回 Mops/s                                               */
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

    /* 预热 */
    sleep_ms(WARMUP_MS);

    /* 重置计数，开始测量 */
    atomic_store_explicit(&psent,  0, memory_order_relaxed);
    atomic_store_explicit(&precvd, 0, memory_order_relaxed);
    sleep_ms(MEASURE_MS);

    /* 停止：先 stop 标志再 close 以唤醒阻塞线程 */
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
    printf("libchan N×M 生产者-消费者吞吐基准\n");
    printf("预热=%dms  测量=%dms  单位=Mops/s（完成的 send+recv 对）\n\n",
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
