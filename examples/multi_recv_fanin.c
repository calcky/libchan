/*
 * multi_recv_fanin.c
 *
 * Multi-way receive, approach 2: fan-in aggregation
 *
 * Use when: many channels (>4) or throughput is the priority; each channel
 * carries a lot of data and you don't want chan_select's O(n) lock-all to
 * become the bottleneck.
 *
 * Principle:
 *   Spawn one feeder thread per source channel. Each feeder does a plain
 *   chan_recv (single-channel fast path, ~17 ns/op), then wraps the value in a
 *   message tagged with its source id and sends it to the aggregation channel.
 *   The main thread only needs a single chan_recv on the aggregation channel,
 *   completely avoiding chan_select's O(n) overhead.
 *
 *   Once all source channels are closed, the feeder threads exit after sending
 *   a sentinel message; once the main thread has collected n sentinels it
 *   closes the aggregation channel and ends the loop.
 *
 * Cost:
 *   Per message: feeder recv (~17 ns) + feeder send (~17 ns) + main recv (~17 ns)
 *   = ~51 ns, but feeders run in parallel with main, so the main thread observes
 *   roughly 17 ns/item.
 *
 * Build:
 *   gcc -O2 -o multi_recv_fanin multi_recv_fanin.c -lchan -lpthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "libchan.h"

#define N_SOURCES    4
#define N_PER_SOURCE 5

/* Aggregated message: source id + value; src == -1 means that source has closed (sentinel) */
typedef struct {
    int src;
    int val;
} msg_t;

/* ---- sender (simulated data source) ---- */

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

/* ---- feeder (one per source, forwards to the aggregation channel) ---- */

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
    /* Send a sentinel to tell the main thread this source has ended */
    msg_t sentinel = { -1, a->id };
    chan_send(a->agg_ch, &sentinel);
    return NULL;
}

int main(void) {
    chan_t *src_chs[N_SOURCES];
    for (int i = 0; i < N_SOURCES; i++)
        src_chs[i] = chan_create(sizeof(int), 4);

    /* Aggregation channel: capacity = sum of all source capacities, to avoid feeder blocking */
    chan_t *agg = chan_create(sizeof(msg_t), N_SOURCES * 4);

    /* Start the data source threads */
    pthread_t src_tids[N_SOURCES];
    src_args_t src_args[N_SOURCES];
    for (int i = 0; i < N_SOURCES; i++) {
        src_args[i] = (src_args_t){ src_chs[i], i };
        pthread_create(&src_tids[i], NULL, source_sender, &src_args[i]);
    }

    /* Start the feeder threads */
    pthread_t feed_tids[N_SOURCES];
    feeder_args_t feed_args[N_SOURCES];
    for (int i = 0; i < N_SOURCES; i++) {
        feed_args[i] = (feeder_args_t){ src_chs[i], agg, i };
        pthread_create(&feed_tids[i], NULL, feeder, &feed_args[i]);
    }

    /* Main thread: just a single-channel recv on the aggregation channel */
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
