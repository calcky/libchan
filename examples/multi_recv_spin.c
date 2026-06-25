/*
 * multi_recv_spin.c
 *
 * Multi-way receive, approach 3: non-blocking spin polling + backoff
 *
 * Use when: latency is extremely sensitive (target < 10 ns), a CPU core can be
 * dedicated, and data arrives frequently.
 *
 * Principle:
 *   Loop over each channel calling chan_try_recv (non-blocking, fast path ~5 ns).
 *   On data, process immediately and reset the backoff counter; on a run of
 *   empty polls, back off in escalating stages:
 *     Stage 1 (spin < 40)  : PAUSE-instruction spin, no CPU yield, ~2–5 ns each
 *     Stage 2 (spin < 200) : sched_yield gives up the time slice, ~1–5 µs
 *     Stage 3 (spin >= 200): nanosleep 100 µs, full sleep, good when data is sparse
 *
 * Cost and trade-offs:
 *   With data: ~5 ns/recv, 6–16x faster than chan_select.
 *   Without data: with no backoff, CPU spins at 100%; backoff trades latency for CPU.
 *   Suited to a hot receive loop that owns a CPU core; not for cases where the
 *   thread count > core count.
 *
 * Build:
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

/* ---- backoff policy ---- */

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

/* ---- data source threads ---- */

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

/* ---- main loop: spin polling ---- */

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

    int active = N_CHANNELS;   /* number of channels still producing data */
    int total  = 0;
    int spin   = 0;

    while (active > 0) {
        bool got_any = false;

        for (int i = 0; i < N_CHANNELS; i++) {
            if (!chs[i]) continue;   /* skip closed slots */

            int v;
            chan_err_t e = chan_try_recv(chs[i], &v);

            if (e == CHAN_OK) {
                printf("recv from ch[%d]: %d\n", i, v);
                total++;
                got_any = true;
                spin = 0;   /* got data, reset the backoff counter */

            } else if (e == CHAN_ERR_CLOSED) {
                printf("channel %d closed\n", i);
                chs[i] = NULL;   /* mark as closed, skip from now on */
                active--;
                got_any = true;
                spin = 0;
            }
            /* CHAN_ERR_WOULDBLOCK: no data this round, keep scanning other channels */
        }

        if (!got_any)
            backoff(&spin);   /* all channels empty this round, back off */
    }

    printf("total received: %d\n", total);

    for (int i = 0; i < N_CHANNELS; i++) {
        pthread_join(tids[i], NULL);
        if (chs[i]) chan_destroy(chs[i]);
    }
    return 0;
}
