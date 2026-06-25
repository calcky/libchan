/*
 * bench/crosslang/libchan/bench.c
 *
 * 跨语言对比基准 — libchan (C) 端
 *
 * 方法论：固定消息数（非固定时长），测完成全部收发的墙钟时间。
 *   - 每个生产者发送固定 K 条；消费者一直收到通道关闭为止 → 收发条数精确相等。
 *   - 计时：从生产者开始发送，到所有消费者收完（通道 drain 完毕）。
 *   - 吞吐 = 总消息数 / 墙钟秒 / 1e6 (Mops/s)。
 *
 * 时长校准：先用一个小消息数定时跑一遍，估算吞吐，再据此把正式测量的消息数
 *   标定到目标时长（~1.5s，不会太短），避免不同场景时长悬殊。
 *
 * 三种变体（各输出一行 CSV）：
 *   direct — 生产者 chan_send / 消费者 chan_recv（核心路径，精确无丢失）
 *   spsc   — 同 direct，但通道由 chan_create_spsc 创建（单生产单消费快路径，
 *            游标缓存 + 无 per-op fence）。仅对 1P1C 场景有意义并输出。
 *   select — 生产者/消费者各跑一次 2-case chan_select（含一个永不就绪的 dummy
 *            第二路），对标 Go select / Rust select!。
 *            注：select 在 MPMC（≥2P+2C）下有已知的微小计数偏差（约 0.01%，
 *            见 doc/design.md 的 Select 已知限制），不影响吞吐量级。
 *
 * 输出（CSV）：lang,np,nc,cap,mops    lang ∈ {libchan_direct, libchan_spsc, libchan_select}
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

#define TARGET_SEC   1.5      /* 正式测量目标时长 */
#define CALIB_MSGS   200000L  /* 校准用的小消息数 */
#define MIN_MSGS     200000L
#define MAX_MSGS     80000000L

static inline int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static chan_t *g_dummy;   /* 无缓冲、无人发送、运行期不关闭 → select 第二路永不就绪 */

typedef struct { chan_t *ch; long k; }              prod_arg_t;
typedef struct { chan_t *ch; _Atomic long *recvd; } cons_arg_t;

/* ── direct 变体 ──────────────────────────────────────────────────────────── */
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

/* ── select 变体 ──────────────────────────────────────────────────────────── */
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

/* ── 跑一轮固定消息数，返回墙钟纳秒；可选校验精确计数 ───────────────────────── */
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
        /* direct 必须精确；select MPMC 有已知微小偏差，仅警告。 */
        fprintf(stderr, "  [warn] np=%d nc=%d cap=%d: 预期 %ld 收到 %ld (差 %+ld)\n",
                np, nc, cap, total, got, got - total);
    }
    chan_destroy(ch);
    return dt;
}

/* ── 校准 + 正式测量，返回 Mops/s ──────────────────────────────────────────── */
static double measure(int np, int nc, int cap,
                      void *(*pf)(void *), void *(*cf)(void *), bool check, bool spsc) {
    /* 1) 校准：小消息数估吞吐 */
    int64_t cdt = run_msgs(np, nc, cap, CALIB_MSGS, pf, cf, false, spsc, NULL);
    double calib_total = (double)((CALIB_MSGS / np) * np);
    double rate = calib_total / ((double)cdt / 1e9);     /* msgs/sec */

    /* 2) 标定正式消息数到目标时长 */
    long msgs = (long)(rate * TARGET_SEC);
    if (msgs < MIN_MSGS) msgs = MIN_MSGS;
    if (msgs > MAX_MSGS) msgs = MAX_MSGS;

    /* 3) 正式测量 */
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
    /* SPSC 快路径：仅对单生产单消费的【有缓冲】通道有意义（契约：1P+1C；cap==0 时
     * chan_create_spsc 等同 chan_create，无快路径，故跳过以免误导）。复用 direct 的
     * 收发函数（chan_send/chan_recv 内部按 ch->spsc 分派）。 */
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
