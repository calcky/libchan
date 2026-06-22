/*
 * multi_recv_fanin.c
 *
 * 多路接收方案二：fan-in 汇聚
 *
 * 适用场景：n 路较多（>4）或吞吐优先；每路数据量大，不希望 select 的
 * O(n) lock-all 成为瓶颈。
 *
 * 原理：
 *   为每个源 channel 启一个 feeder 线程，feeder 做普通 chan_recv（走单
 *   channel 快路径，约 17 ns/op），收到后包装成带来源 id 的消息发往聚合
 *   channel。主线程只需在聚合 channel 上做一次 chan_recv，完全避开
 *   chan_select 的 O(n) 开销。
 *
 *   源 channel 全部关闭后，feeder 线程退出并发送哨兵消息，主线程
 *   收齐 n 个哨兵后关闭聚合 channel，结束循环。
 *
 * 开销：
 *   每条消息：feeder recv (~17 ns) + feeder send (~17 ns) + main recv (~17 ns)
 *   = ~51 ns，但 feeder 与 main 并行，主线程实际观测约 17 ns/item。
 *
 * 编译：
 *   gcc -O2 -o multi_recv_fanin multi_recv_fanin.c -lchan -lpthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "libchan.h"

#define N_SOURCES    4
#define N_PER_SOURCE 5

/* 聚合消息：来源编号 + 数据值；src == -1 表示该路已关闭（哨兵）*/
typedef struct {
    int src;
    int val;
} msg_t;

/* ---- sender（模拟数据源）---- */

typedef struct {
    chan_t *ch;
    int     id;
} src_args_t;

static void *source_sender(void *arg) {
    src_args_t *a = arg;
    for (int i = 0; i < N_PER_SOURCE; i++) {
        int v = a->id * 100 + i;
        chan_send(a->ch, &v);
    }
    chan_close(a->ch);
    return NULL;
}

/* ---- feeder（每路一个，转发到聚合 channel）---- */

typedef struct {
    chan_t *src_ch;
    chan_t *agg_ch;
    int     id;
} feeder_args_t;

static void *feeder(void *arg) {
    feeder_args_t *a = arg;
    int v;
    while (chan_recv(a->src_ch, &v) == CHAN_OK) {
        msg_t m = { a->id, v };
        chan_send(a->agg_ch, &m);
    }
    /* 发送哨兵，通知主线程本路已结束 */
    msg_t sentinel = { -1, a->id };
    chan_send(a->agg_ch, &sentinel);
    return NULL;
}

int main(void) {
    chan_t *src_chs[N_SOURCES];
    for (int i = 0; i < N_SOURCES; i++)
        src_chs[i] = chan_create(sizeof(int), 4);

    /* 聚合 channel：容量 = 所有源容量之和，避免 feeder 阻塞 */
    chan_t *agg = chan_create(sizeof(msg_t), N_SOURCES * 4);

    /* 启动数据源线程 */
    pthread_t src_tids[N_SOURCES];
    src_args_t src_args[N_SOURCES];
    for (int i = 0; i < N_SOURCES; i++) {
        src_args[i] = (src_args_t){ src_chs[i], i };
        pthread_create(&src_tids[i], NULL, source_sender, &src_args[i]);
    }

    /* 启动 feeder 线程 */
    pthread_t feed_tids[N_SOURCES];
    feeder_args_t feed_args[N_SOURCES];
    for (int i = 0; i < N_SOURCES; i++) {
        feed_args[i] = (feeder_args_t){ src_chs[i], agg, i };
        pthread_create(&feed_tids[i], NULL, feeder, &feed_args[i]);
    }

    /* 主线程：只需在聚合 channel 上做单路 recv */
    int closed = 0, total = 0;
    msg_t m;
    while (closed < N_SOURCES && chan_recv(agg, &m) == CHAN_OK) {
        if (m.src == -1) {
            printf("source %d closed\n", m.val);
            closed++;
            if (closed == N_SOURCES)
                chan_close(agg);
        } else {
            printf("recv from src[%d]: %d\n", m.src, m.val);
            total++;
        }
    }
    printf("total received: %d\n", total);

    for (int i = 0; i < N_SOURCES; i++) {
        pthread_join(src_tids[i], NULL);
        pthread_join(feed_tids[i], NULL);
        chan_destroy(src_chs[i]);
    }
    chan_destroy(agg);
    return 0;
}
