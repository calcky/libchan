/*
 * ping_pong_buffered.c — Buffered / windowed Ping-pong throughput
 *
 * Contrast with ping_pong.c (unbuffered). The key insight:
 *   Strict "one out, one back" cannot go faster even with buffering — main
 *   still waits for each pong before sending the next ping, so the data
 *   dependency is serial and each round still parks/wakes. Buffering's real
 *   value is letting multiple messages be [in flight at once].
 *
 * This demo uses windowing (pipelining): first fill the pipe with WINDOW pings,
 * then for every pong received send one more ping, always keeping WINDOW
 * messages in flight. The two threads barely block → sends/recvs hit the
 * lock-free fast path (ring_lf_push/pop), avoiding per-round park/wake, with
 * throughput roughly 2x that of unbuffered rendezvous.
 *
 *   main:  fill WINDOW ──► [ping buffer] ──► ponger echoes ──► [pong buffer] ──► main recv+refill
 */
#include <stdio.h>
#include <pthread.h>
#include <time.h>

#include "libchan.h"

#define ROUNDS 2000000
#define WINDOW 64          /* in-flight messages = buffer capacity */

static int64_t now_ns(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000000000LL + t.tv_nsec;
}

typedef struct { chan_t *ping, *pong; } arg_t;

/* ponger: recv ping, echo it back as pong, ROUNDS+WINDOW times total. */
static void *ponger(void *arg) {
    arg_t *a = arg;
    int v;
    for (long i = 0; i < ROUNDS + WINDOW; i++) {
        if (chan_recv(a->ping, &v) != CHAN_OK) break;
        chan_send(a->pong, &v);
    }
    return NULL;
}

int main(void) {
    chan_t *ping = chan_create(sizeof(int), WINDOW);   /* buffered */
    chan_t *pong = chan_create(sizeof(int), WINDOW);

    pthread_t pt;
    arg_t a = { ping, pong };
    pthread_create(&pt, NULL, ponger, &a);

    printf("Buffered Ping-pong (window=%d) — keeping %d messages in flight, bouncing %d round-trips...\n",
           WINDOW, WINDOW, ROUNDS);

    int v = 0, r;

    /* Fill the window: send WINDOW pings first (untimed, as a pipeline warm-up) */
    for (int i = 0; i < WINDOW; i++) { chan_send(ping, &v); v++; }

    int64_t t0 = now_ns();
    for (long i = 0; i < ROUNDS; i++) {
        chan_recv(pong, &r);          /* receive one back */
        chan_send(ping, &v); v++;     /* immediately refill one, keeping the window full */
    }
    /* Drain the remaining WINDOW in-flight messages */
    for (int i = 0; i < WINDOW; i++) chan_recv(pong, &r);
    int64_t dt = now_ns() - t0;

    pthread_join(pt, NULL);

    double per   = (double)dt / ROUNDS;
    double mops  = 2.0 * ROUNDS / (dt / 1e3);   /* 2 channel transfers per round-trip */
    printf("\nTotal time %.1f ms, %d round-trips\n", dt / 1e6, ROUNDS);
    printf("Per round-trip %.0f ns, channel op throughput \xE2\x89\x88 %.2f Mops/s\n", per, mops);
    printf("(vs unbuffered ping_pong: windowing keeps the pipeline full, sends/recvs hit the lock-free fast path, avoiding per-round park/wake)\n");

    chan_destroy(ping);
    chan_destroy(pong);
    return 0;
}
