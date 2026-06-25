/*
 * multi_recv_select.c
 *
 * Multi-way receive, approach 1: chan_select
 *
 * Use when: n ≤ 4 channels, you don't want to spawn extra threads, and you
 * want semantics matching Go's select.
 *
 * Principle:
 *   chan_select scans for ready state while holding all channel locks; if any
 *   is ready it executes immediately. If none is ready, it registers a stub
 *   waiter on every channel, sharing a single park, and sleeps once; the first
 *   channel to become ready wakes the main thread.
 *
 * Cost:
 *   Fast path (a channel is ready): O(n) mutex_locks, ~30–80 ns (n=2–4).
 *   Slow path (must wait): register n stubs + one park + O(n)-lock cleanup.
 *
 * Build:
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
     * Each round, pack the still-active channels into a compact cases array
     * and pass it to chan_select. Use slot_id[] to record the original index
     * that compact[j] corresponds to, so closures are easy to mark.
     */
    bool active[N_SENDERS];
    int  vals[N_SENDERS];
    for (int i = 0; i < N_SENDERS; i++) active[i] = true;

    int n_active = N_SENDERS;
    int total    = 0;

    while (n_active > 0) {
        /* Build the compact cases array */
        chan_select_case_t cases[N_SENDERS];
        int slot_id[N_SENDERS];   /* cases[j] maps to original index slot_id[j] */
        int nc = 0;
        for (int i = 0; i < N_SENDERS; i++) {
            if (!active[i]) continue;
            cases[nc].ch   = chs[i];
            cases[nc].op   = CHAN_OP_RECV;
            cases[nc].data = &vals[i];   /* always write to a fixed slot, no pointer aliasing */
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
