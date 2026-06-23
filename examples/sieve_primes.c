/*
 * sieve_primes.c — 并发质数筛 (Concurrent Prime Sieve)
 *
 * Go 最经典的 channel 演示在 libchan 上的 1:1 复刻。
 *
 * 原理: 每发现一个质数 p,就起一个 filter 线程,从上游通道读数、丢掉 p 的倍数、
 * 把幸存者转发到下游通道。通道把这些线程串成一条流水线:
 *
 *     generate ──► [filter 2] ──► [filter 3] ──► [filter 5] ──► ... ──► main
 *                    丢2的倍数     丢3的倍数     丢5的倍数
 *
 * 每级流出的第一个数必是质数。干净退出: generate 发到 LIMIT 后 chan_close,
 * close 沿流水线级联(每个 filter 收到 CLOSED 就关闭自己的下游并退出),无泄漏。
 */
#include <stdio.h>
#include <pthread.h>

#include "libchan.h"

#define LIMIT    541    /* 第 100 个质数是 541 */
#define MAXSTAGE 256

typedef struct { chan_t *in, *out; int prime; } filt_t;

/* 源头: 依次送 2,3,4,...,LIMIT,然后关闭。 */
static void *generate(void *arg) {
    chan_t *out = arg;
    for (int i = 2; i <= LIMIT; i++)
        if (chan_send(out, &i) != CHAN_OK) break;   /* 下游已关 → 停 */
    chan_close(out);
    return NULL;
}

/* 一级筛: 转发所有不是 prime 倍数的数; 上游关闭则级联关闭下游。 */
static void *filter(void *arg) {
    filt_t *f = arg;
    int i;
    while (chan_recv(f->in, &i) == CHAN_OK) {
        if (i % f->prime != 0)
            if (chan_send(f->out, &i) != CHAN_OK) break;
    }
    chan_close(f->out);   /* close 级联 */
    return NULL;
}

int main(void) {
    pthread_t th[MAXSTAGE];
    chan_t   *ch[MAXSTAGE];
    filt_t    fa[MAXSTAGE];
    int       nth = 0, nch = 0;

    chan_t *head = chan_create(sizeof(int), 1);
    ch[nch++] = head;
    pthread_create(&th[nth++], NULL, generate, head);

    printf("并发质数筛 — 每个质数一个过滤线程,channel 串成流水线\n\n");

    int count = 0, prime;
    chan_t *cur = head;
    while (chan_recv(cur, &prime) == CHAN_OK) {        /* 每级首个数即质数 */
        printf("%4d%s", prime, (++count % 10 == 0) ? "\n" : " ");
        chan_t *next = chan_create(sizeof(int), 1);
        ch[nch++] = next;
        fa[nth] = (filt_t){ cur, next, prime };
        pthread_create(&th[nth], NULL, filter, &fa[nth]);
        nth++;
        cur = next;
    }

    printf("\n\n共筛出 %d 个质数 (≤ %d),用了 %d 个线程。\n", count, LIMIT, nth);

    for (int i = 0; i < nth; i++) pthread_join(th[i], NULL);
    for (int i = 0; i < nch; i++) chan_destroy(ch[i]);
    return 0;
}
