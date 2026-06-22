/*
 * multi_recv_select.c
 *
 * 多路接收方案一：chan_select
 *
 * 适用场景：n ≤ 4 路、不想引入额外线程、语义对应 Go select。
 *
 * 原理：
 *   chan_select 在持所有 channel 锁的同时扫描就绪状态，有就绪时立即执行；
 *   无就绪时在所有 channel 上各注册一个 stub waiter，共享一个 park，
 *   只睡一次，第一个就绪的 channel 唤醒主线程。
 *
 * 开销：
 *   快路径（有 channel 就绪）：O(n) 次 mutex_lock，约 30–80 ns（n=2–4）。
 *   慢路径（需等待）：注册 n 个 stub + park 一次 + 清理 O(n) 锁。
 *
 * 编译：
 *   gcc -O2 -o multi_recv_select multi_recv_select.c -lchan -lpthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "libchan.h"

#define N_SENDERS    3
#define N_PER_SENDER 4

typedef struct {
    chan_t *ch;
    int     id;
    int     start;
} sender_args_t;

static void *sender(void *arg) {
    sender_args_t *a = arg;
    for (int i = a->start; i < a->start + N_PER_SENDER; i++)
        chan_send(a->ch, &i);
    chan_close(a->ch);
    return NULL;
}

int main(void) {
    chan_t *chs[N_SENDERS];
    for (int i = 0; i < N_SENDERS; i++)
        chs[i] = chan_create(sizeof(int), 2);

    pthread_t tids[N_SENDERS];
    sender_args_t sargs[N_SENDERS];
    for (int i = 0; i < N_SENDERS; i++) {
        sargs[i] = (sender_args_t){ chs[i], i, i * 10 };
        pthread_create(&tids[i], NULL, sender, &sargs[i]);
    }

    /*
     * 每轮把仍活跃的 channel 打包成紧凑 cases 数组传给 chan_select。
     * 用 slot_id[] 记录 compact[j] 对应的原始下标，方便关闭时标记。
     */
    bool active[N_SENDERS];
    int  vals[N_SENDERS];
    for (int i = 0; i < N_SENDERS; i++) active[i] = true;

    int n_active = N_SENDERS;
    int total    = 0;

    while (n_active > 0) {
        /* 构建紧凑 cases 数组 */
        chan_select_case_t cases[N_SENDERS];
        int slot_id[N_SENDERS];   /* cases[j] 对应原始下标 slot_id[j] */
        int nc = 0;
        for (int i = 0; i < N_SENDERS; i++) {
            if (!active[i]) continue;
            cases[nc].ch   = chs[i];
            cases[nc].op   = CHAN_OP_RECV;
            cases[nc].data = &vals[i];   /* 始终写入固定槽位，无指针错位 */
            slot_id[nc]    = i;
            nc++;
        }

        int w = chan_select(cases, (size_t)nc);
        if (w < 0) break;

        int orig = slot_id[w];

        if (cases[w].result == CHAN_ERR_CLOSED) {
            printf("channel %d closed\n", orig);
            active[orig] = false;
            n_active--;
        } else {
            printf("recv from ch[%d]: %d\n", orig, vals[orig]);
            total++;
        }
    }

    printf("total received: %d\n", total);

    for (int i = 0; i < N_SENDERS; i++) {
        pthread_join(tids[i], NULL);
        chan_destroy(chs[i]);
    }
    return 0;
}
