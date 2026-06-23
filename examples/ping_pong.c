/*
 * ping_pong.c — Ping-pong 延迟基准
 *
 * 两个线程用一对无缓冲通道来回弹一个 token,实测 rendezvous 往返延迟。
 *
 *   main:   send(ping) ──► ponger: recv(ping)
 *   main:   recv(pong) ◄── ponger: send(pong)      ← 一个往返 = 2 次 rendezvous
 *
 * 无缓冲(cap=0)强制每次收发都同步握手,直接命中 architecture 文档里说的
 * park/wake 路径。每次往返含 2 次 channel rendezvous,各自一次 park/unpark。
 */
#include <stdio.h>
#include <pthread.h>
#include <time.h>

#include "libchan.h"

#define ROUNDS 200000

static int64_t now_ns(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000000000LL + t.tv_nsec;
}

typedef struct { chan_t *ping, *pong; } arg_t;

/* ponger: 收 ping,原样回 pong。 */
static void *ponger(void *arg) {
    arg_t *a = arg;
    int v;
    for (int i = 0; i < ROUNDS; i++) {
        if (chan_recv(a->ping, &v) != CHAN_OK) break;
        chan_send(a->pong, &v);
    }
    return NULL;
}

int main(void) {
    chan_t *ping = chan_create(sizeof(int), 0);   /* 无缓冲 → 强制 rendezvous */
    chan_t *pong = chan_create(sizeof(int), 0);

    pthread_t pt;
    arg_t a = { ping, pong };
    pthread_create(&pt, NULL, ponger, &a);

    printf("Ping-pong 延迟基准 — 无缓冲通道来回弹 %d 次...\n", ROUNDS);

    int64_t t0 = now_ns();
    int v = 0, r;
    for (int i = 0; i < ROUNDS; i++) {
        chan_send(ping, &v);     /* 我 → 对方 */
        chan_recv(pong, &r);     /* 对方 → 我  = 一个往返 */
        v++;
    }
    int64_t dt = now_ns() - t0;

    pthread_join(pt, NULL);

    double per = (double)dt / ROUNDS;
    printf("\n总耗时 %.1f ms,%d 次往返\n", dt / 1e6, ROUNDS);
    printf("每次往返 %.0f ns  (= 2 次 channel rendezvous)\n", per);
    printf("每次 channel 操作 \xE2\x89\x88 %.0f ns,\xE2\x89\x88 %.2f Mops/s\n",
           per / 2, 2.0 * ROUNDS / (dt / 1e3));

    chan_destroy(ping);
    chan_destroy(pong);
    return 0;
}
