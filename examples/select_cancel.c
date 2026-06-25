/*
 * select_cancel.c — three-way select: result / cancel / timeout
 *
 * The most common production select pattern, written for libchan; equivalent to Go's
 *   select { case r := <-results: ...; case <-cancel: return; case <-time.After(d): ... }
 *
 * Uses chan_select_timeout to watch simultaneously:
 *   - results channel: worker produces results periodically (one is deliberately slow, triggering a timeout)
 *   - cancel  channel: controller broadcasts cancellation via chan_close after 1.5s
 *   - 150ms timeout:   fires when neither has activity
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

/* worker: produce increasing results periodically; the 3rd is deliberately slow, to provoke one timeout. */
static void *worker(void *arg) {
    warg_t *w = arg;
    int v = 0;
    for (;;) {
        msleep(v == 3 ? 300 : 80);
        if (chan_send(w->results, &v) != CHAN_OK) break;   /* results closed → exit */
        v++;
    }
    return NULL;
}

/* controller: issue cancellation after 1.5s. */
static void *controller(void *arg) {
    msleep(1500);
    chan_close((chan_t *)arg);     /* broadcast cancellation */
    return NULL;
}

int main(void) {
    chan_t *results = chan_create(sizeof(int), 1);
    chan_t *cancel  = chan_create(sizeof(int), 0);

    pthread_t wt, ct;
    warg_t wa = { results, cancel };
    pthread_create(&wt, NULL, worker, &wa);
    pthread_create(&ct, NULL, controller, cancel);

    printf("Three-way select — watching result / cancel / timeout (150ms)\n\n");

    int out, dummy, got = 0, timeouts = 0;
    for (;;) {
        chan_select_case_t cases[2] = {
            { results, CHAN_OP_RECV, &out,   CHAN_OK },
            { cancel,  CHAN_OP_RECV, &dummy, CHAN_OK },
        };
        int w = chan_select_timeout(cases, 2, 150 * MS);

        if (w == 1) {                                   /* cancel ready (closed) */
            printf("  \xE2\xA8\xAF cancellation received, exiting\n");
            break;
        } else if (w == 0) {                            /* result ready */
            printf("  \xE2\x9C\x93 result %d\n", out);
            got++;
        } else {                                        /* w == -1: timeout */
            printf("  \xE2\x80\xA6 timeout (150ms with no result)\n");
            timeouts++;
        }
    }

    printf("\nStats: %d results, %d timeouts\n", got, timeouts);

    chan_close(results);            /* make worker's send return CLOSED so it exits */
    pthread_join(wt, NULL);
    pthread_join(ct, NULL);
    chan_destroy(results);
    chan_destroy(cancel);
    return 0;
}
