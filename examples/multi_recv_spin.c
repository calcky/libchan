/*
 * multi_recv_spin.c
 *
 * 多路接收方案三：非阻塞自旋轮询 + 退让
 *
 * 适用场景：延迟极敏感（目标 < 10 ns）、CPU 核心可专用、数据到达频率高。
 *
 * 原理：
 *   对每路 channel 循环调用 chan_try_recv（非阻塞，快路径约 5 ns）。
 *   有数据时立即处理，reset 退让计数器；连续空转时逐级退让：
 *     阶段 1（spin < 40）  ：PAUSE 指令自旋，不让出 CPU，约 2–5 ns/次
 *     阶段 2（spin < 200） ：sched_yield 让出时间片，约 1–5 µs
 *     阶段 3（spin >= 200）：nanosleep 100 µs，彻底休眠，适合数据稀疏时
 *
 * 开销与代价：
 *   有数据时：~5 ns/recv，比 chan_select 快 6–16 倍。
 *   无数据时：若不加退让，CPU 100% 空转；加退让后延迟换 CPU。
 *   适合独占一个 CPU 核心的热点接收循环；不适合线程数 > 核心数的场景。
 *
 * 编译：
 *   gcc -O2 -o multi_recv_spin multi_recv_spin.c -lchan -lpthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <pthread.h>
#include <sched.h>
#include <time.h>
#include "libchan.h"

#define N_CHANNELS   3
#define N_PER_CH    10

/* ---- 退让策略 ---- */

static inline void spin_pause(void) {
#if defined(__x86_64__) || defined(__i386__)
    __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__)
    __asm__ volatile("yield" ::: "memory");
#endif
}

static inline void backoff(int *spin) {
    if (*spin < 40) {
        spin_pause();
    } else if (*spin < 200) {
        sched_yield();
    } else {
        struct timespec ts = { 0, 100000 }; /* 100 µs */
        nanosleep(&ts, NULL);
    }
    (*spin)++;
}

/* ---- 数据源线程 ---- */

typedef struct {
    chan_t *ch;
    int     id;
    int     n;
} sender_args_t;

static void *sender(void *arg) {
    sender_args_t *a = arg;
    for (int i = 0; i < a->n; i++) {
        int v = a->id * 100 + i;
        chan_send(a->ch, &v);
    }
    chan_close(a->ch);
    return NULL;
}

/* ---- 主循环：自旋轮询 ---- */

int main(void) {
    chan_t *chs[N_CHANNELS];
    for (int i = 0; i < N_CHANNELS; i++)
        chs[i] = chan_create(sizeof(int), 4);

    pthread_t tids[N_CHANNELS];
    sender_args_t args[N_CHANNELS];
    for (int i = 0; i < N_CHANNELS; i++) {
        args[i] = (sender_args_t){ chs[i], i, N_PER_CH };
        pthread_create(&tids[i], NULL, sender, &args[i]);
    }

    int active = N_CHANNELS;   /* 仍在产数据的 channel 数 */
    int total  = 0;
    int spin   = 0;

    while (active > 0) {
        bool got_any = false;

        for (int i = 0; i < N_CHANNELS; i++) {
            if (!chs[i]) continue;   /* 已关闭的槽位跳过 */

            int v;
            chan_err_t e = chan_try_recv(chs[i], &v);

            if (e == CHAN_OK) {
                printf("recv from ch[%d]: %d\n", i, v);
                total++;
                got_any = true;
                spin = 0;   /* 有数据，重置退让计数 */

            } else if (e == CHAN_ERR_CLOSED) {
                printf("channel %d closed\n", i);
                chs[i] = NULL;   /* 标记为已关闭，后续跳过 */
                active--;
                got_any = true;
                spin = 0;
            }
            /* CHAN_ERR_WOULDBLOCK：本轮无数据，继续扫描其他 channel */
        }

        if (!got_any)
            backoff(&spin);   /* 所有 channel 本轮均空，执行退让 */
    }

    printf("total received: %d\n", total);

    for (int i = 0; i < N_CHANNELS; i++) {
        pthread_join(tids[i], NULL);
        if (chs[i]) chan_destroy(chs[i]);
    }
    return 0;
}
