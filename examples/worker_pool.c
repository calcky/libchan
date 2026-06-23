/*
 * worker_pool.c — Fan-out / Fan-in 工作池
 *
 * 最实用的并行计算骨架,演示 MPMC + close 广播 + 缓冲背压。
 *
 *   提交者 ──► [jobs 通道] ──► 4 个 worker 并行计算 ──► [results 通道] ──► 收集者(fan-in)
 *
 * jobs 通道用小缓冲 → 提交快于消费时自然背压。提交完毕 chan_close(jobs),
 * worker 收完剩余任务后退出。主线程从 results 收齐 N 个结果,打印实时进度。
 */
#include <stdio.h>
#include <pthread.h>

#include "libchan.h"

#define NWORKERS 4
#define NJOBS    40

typedef struct { int id; int n; }                 job_t;
typedef struct { int job_id, worker_id, result; } res_t;

/* 一点真实 CPU 活: 数 n 以下的质数个数(试除法)。 */
static int count_primes_below(int n) {
    int c = 0;
    for (int x = 2; x < n; x++) {
        int prime = 1;
        for (int d = 2; (long)d * d <= x; d++)
            if (x % d == 0) { prime = 0; break; }
        c += prime;
    }
    return c;
}

typedef struct { chan_t *jobs, *results; int wid; } warg_t;

static void *worker(void *arg) {
    warg_t *w = arg;
    job_t j;
    while (chan_recv(w->jobs, &j) == CHAN_OK) {          /* jobs 关闭 → 退出 */
        res_t r = { j.id, w->wid, count_primes_below(j.n) };
        chan_send(w->results, &r);
    }
    return NULL;
}

int main(void) {
    chan_t *jobs    = chan_create(sizeof(job_t), 8);     /* 小缓冲 → 背压 */
    chan_t *results = chan_create(sizeof(res_t), NJOBS);

    pthread_t wth[NWORKERS];
    warg_t    wa[NWORKERS];
    for (int i = 0; i < NWORKERS; i++) {
        wa[i] = (warg_t){ jobs, results, i };
        pthread_create(&wth[i], NULL, worker, &wa[i]);
    }

    printf("Fan-out 工作池 — %d 个 worker 并行处理 %d 个任务(各数质数)\n\n",
           NWORKERS, NJOBS);

    /* 提交任务(生产) */
    for (int i = 0; i < NJOBS; i++) {
        job_t j = { i, 20000 + i * 1500 };
        chan_send(jobs, &j);
    }
    chan_close(jobs);   /* 广播收工 */

    /* 收集结果(fan-in) + 实时进度 */
    long total = 0;
    for (int got = 0; got < NJOBS; got++) {
        res_t r;
        chan_recv(results, &r);
        total += r.result;
        printf("\r进度 [%2d/%2d]  job#%-2d → worker%d: %d 个质数        ",
               got + 1, NJOBS, r.job_id, r.worker_id, r.result);
        fflush(stdout);
    }
    printf("\n\n全部完成,质数总计 = %ld\n", total);

    for (int i = 0; i < NWORKERS; i++) pthread_join(wth[i], NULL);
    chan_destroy(jobs);
    chan_destroy(results);
    return 0;
}
