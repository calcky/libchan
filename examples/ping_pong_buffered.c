/*
 * ping_pong_buffered.c — 有缓冲 / 窗口化 Ping-pong 吞吐
 *
 * 对照 ping_pong.c(无缓冲)。关键洞察:
 *   严格"一来一回"即使加缓冲也快不了 —— main 每轮仍要等 pong 才能发下一个 ping,
 *   数据依赖是串行的,每轮照样 park/wake。缓冲真正的价值是让多个消息【同时在途】。
 *
 * 本 demo 用窗口化(pipelining):先灌满 WINDOW 个 ping,之后每收到一个 pong 就补一个
 * ping,始终保持 WINDOW 个消息在飞。两个线程几乎不阻塞 → 收发命中无锁快路径
 * (ring_lf_push/pop),免去每轮 park/wake,吞吐约为无缓冲 rendezvous 的 2 倍。
 *
 *   main:  灌满 WINDOW ──► [ping 缓冲] ──► ponger 回显 ──► [pong 缓冲] ──► main 收+补
 */
#include <stdio.h>
#include <pthread.h>
#include <time.h>

#include "libchan.h"

#define ROUNDS 2000000
#define WINDOW 64          /* 在途消息数 = 缓冲容量 */

static int64_t now_ns(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000000000LL + t.tv_nsec;
}

typedef struct { chan_t *ping, *pong; } arg_t;

/* ponger: 收 ping,原样回 pong,共 ROUNDS+WINDOW 次。 */
static void *ponger(void *arg) {
    arg_t *a = arg;
    int v;
    for (long i = 0; i < ROUNDS + WINDOW; i++) {
        if (chan_recv(a->ping, &v) != CHAN_OK) break;
        chan_send(a->pong, &v);
    }
    return NULL;
}

int main(void) {
    chan_t *ping = chan_create(sizeof(int), WINDOW);   /* 有缓冲 */
    chan_t *pong = chan_create(sizeof(int), WINDOW);

    pthread_t pt;
    arg_t a = { ping, pong };
    pthread_create(&pt, NULL, ponger, &a);

    printf("有缓冲 Ping-pong(窗口=%d)— 保持 %d 个消息在途,弹 %d 个往返...\n",
           WINDOW, WINDOW, ROUNDS);

    int v = 0, r;

    /* 灌满窗口:先发 WINDOW 个 ping(不计时,作为流水线预热) */
    for (int i = 0; i < WINDOW; i++) { chan_send(ping, &v); v++; }

    int64_t t0 = now_ns();
    for (long i = 0; i < ROUNDS; i++) {
        chan_recv(pong, &r);          /* 收回一个 */
        chan_send(ping, &v); v++;     /* 立即补一个,保持窗口满 */
    }
    /* 排空残留的 WINDOW 个在途消息 */
    for (int i = 0; i < WINDOW; i++) chan_recv(pong, &r);
    int64_t dt = now_ns() - t0;

    pthread_join(pt, NULL);

    double per   = (double)dt / ROUNDS;
    double mops  = 2.0 * ROUNDS / (dt / 1e3);   /* 每往返 2 次 channel 传递 */
    printf("\n总耗时 %.1f ms,%d 次往返\n", dt / 1e6, ROUNDS);
    printf("每次往返 %.0f ns,channel 操作吞吐 \xE2\x89\x88 %.2f Mops/s\n", per, mops);
    printf("(对照无缓冲 ping_pong:窗口化保持流水线满,收发命中无锁快路径,免去每轮 park/wake)\n");

    chan_destroy(ping);
    chan_destroy(pong);
    return 0;
}
