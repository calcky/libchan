/*
 * ping_pong.c — Ping-pong latency benchmark
 *
 * Two threads bounce a token back and forth over a pair of unbuffered channels,
 * measuring the rendezvous round-trip latency.
 *
 *   main:   send(ping) ──► ponger: recv(ping)
 *   main:   recv(pong) ◄── ponger: send(pong)      ← one round-trip = 2 rendezvous
 *
 * Unbuffered (cap=0) forces every send/recv to synchronously hand off, hitting
 * the park/wake path described in the architecture docs. Each round-trip
 * involves 2 channel rendezvous, each with one park/unpark.
 */
#include <stdio.h>
#include <pthread.h>
#include <time.h>

#include "libchan.h"

#define ROUNDS 200000

static int64_t now_ns(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000000000LL + t.tv_nsec;
}

typedef struct { chan_t *ping, *pong; } arg_t;

/* ponger: recv ping, echo it back as pong. */
static void *ponger(void *arg) {
    arg_t *a = arg;
    int v;
    for (int i = 0; i < ROUNDS; i++) {
        if (chan_recv(a->ping, &v) != CHAN_OK) break;
        chan_send(a->pong, &v);
    }
    return NULL;
}

int main(void) {
    chan_t *ping = chan_create(sizeof(int), 0);   /* unbuffered → force rendezvous */
    chan_t *pong = chan_create(sizeof(int), 0);

    pthread_t pt;
    arg_t a = { ping, pong };
    pthread_create(&pt, NULL, ponger, &a);

    printf("Ping-pong latency benchmark — bouncing over unbuffered channels %d times...\n", ROUNDS);

    int64_t t0 = now_ns();
    int v = 0, r;
    for (int i = 0; i < ROUNDS; i++) {
        chan_send(ping, &v);     /* me → peer */
        chan_recv(pong, &r);     /* peer → me  = one round-trip */
        v++;
    }
    int64_t dt = now_ns() - t0;

    pthread_join(pt, NULL);

    double per = (double)dt / ROUNDS;
    printf("\nTotal time %.1f ms, %d round-trips\n", dt / 1e6, ROUNDS);
    printf("Per round-trip %.0f ns  (= 2 channel rendezvous)\n", per);
    printf("Per channel op \xE2\x89\x88 %.0f ns, \xE2\x89\x88 %.2f Mops/s\n",
           per / 2, 2.0 * ROUNDS / (dt / 1e3));

    chan_destroy(ping);
    chan_destroy(pong);
    return 0;
}
