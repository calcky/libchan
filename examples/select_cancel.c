/*
 * select_cancel.c — select 三路: 结果 / 取消 / 超时
 *
 * 生产环境最常见的 select 模式在 libchan 上的写法,等价于 Go 的
 *   select { case r := <-results: ...; case <-cancel: return; case <-time.After(d): ... }
 *
 * 用 chan_select_timeout 同时监听:
 *   - results 通道: worker 周期产出结果(其中一个故意慢,触发超时)
 *   - cancel  通道: controller 在 1.5s 后 chan_close 广播取消
 *   - 150ms 超时:   两者都没动静时触发
 */
#include <stdio.h>
#include <pthread.h>
#include <time.h>

#include "libchan.h"

#define MS 1000000LL

static void msleep(int ms) {
    struct timespec t = { ms / 1000, (long)(ms % 1000) * 1000000L };
    nanosleep(&t, NULL);
}

typedef struct { chan_t *results, *cancel; } warg_t;

/* worker: 周期产出递增结果;第 3 个故意慢,制造一次超时。 */
static void *worker(void *arg) {
    warg_t *w = arg;
    int v = 0;
    for (;;) {
        msleep(v == 3 ? 300 : 80);
        if (chan_send(w->results, &v) != CHAN_OK) break;   /* results 关闭 → 退出 */
        v++;
    }
    return NULL;
}

/* controller: 1.5s 后下达取消。 */
static void *controller(void *arg) {
    msleep(1500);
    chan_close((chan_t *)arg);     /* 广播取消 */
    return NULL;
}

int main(void) {
    chan_t *results = chan_create(sizeof(int), 1);
    chan_t *cancel  = chan_create(sizeof(int), 0);

    pthread_t wt, ct;
    warg_t wa = { results, cancel };
    pthread_create(&wt, NULL, worker, &wa);
    pthread_create(&ct, NULL, controller, cancel);

    printf("select 三路 — 监听 结果 / 取消 / 超时(150ms)\n\n");

    int out, dummy, got = 0, timeouts = 0;
    for (;;) {
        chan_select_case_t cases[2] = {
            { results, CHAN_OP_RECV, &out,   CHAN_OK },
            { cancel,  CHAN_OP_RECV, &dummy, CHAN_OK },
        };
        int w = chan_select_timeout(cases, 2, 150 * MS);

        if (w == 1) {                                   /* cancel 就绪(已关闭) */
            printf("  \xE2\xA8\xAF 收到取消,退出\n");
            break;
        } else if (w == 0) {                            /* 结果就绪 */
            printf("  \xE2\x9C\x93 结果 %d\n", out);
            got++;
        } else {                                        /* w == -1: 超时 */
            printf("  \xE2\x80\xA6 超时(150ms 无结果)\n");
            timeouts++;
        }
    }

    printf("\n统计: 结果 %d 个, 超时 %d 次\n", got, timeouts);

    chan_close(results);            /* 让 worker 的 send 返回 CLOSED 退出 */
    pthread_join(wt, NULL);
    pthread_join(ct, NULL);
    chan_destroy(results);
    chan_destroy(cancel);
    return 0;
}
