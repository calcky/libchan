/*
 * sieve_primes.c — Concurrent Prime Sieve
 *
 * A 1:1 port to libchan of Go's most classic channel demo.
 *
 * Principle: for each prime p discovered, spawn a filter thread that reads
 * numbers from its upstream channel, drops multiples of p, and forwards the
 * survivors to its downstream channel. Channels chain these threads into a
 * pipeline:
 *
 *     generate ──► [filter 2] ──► [filter 3] ──► [filter 5] ──► ... ──► main
 *                   drop x2's      drop x3's      drop x5's
 *
 * The first number out of each stage is necessarily prime. Clean shutdown:
 * generate sends up to LIMIT then chan_close; close cascades down the pipeline
 * (each filter, on seeing CLOSED, closes its own downstream and exits), with no
 * leaks.
 */
#include <stdio.h>
#include <pthread.h>

#include "libchan.h"

#define LIMIT    541    /* the 100th prime is 541 */
#define MAXSTAGE 256

typedef struct { chan_t *in, *out; int prime; } filt_t;

/* Source: send 2,3,4,...,LIMIT in order, then close. */
static void *generate(void *arg) {
    chan_t *out = arg;
    for (int i = 2; i <= LIMIT; i++)
        if (chan_send(out, &i) != CHAN_OK) break;   /* downstream closed → stop */
    chan_close(out);
    return NULL;
}

/* One sieve stage: forward all numbers that are not multiples of prime;
 * when upstream closes, cascade the close to downstream. */
static void *filter(void *arg) {
    filt_t *f = arg;
    int i;
    while (chan_recv(f->in, &i) == CHAN_OK) {
        if (i % f->prime != 0)
            if (chan_send(f->out, &i) != CHAN_OK) break;
    }
    chan_close(f->out);   /* cascade close */
    return NULL;
}

int main(void) {
    pthread_t th[MAXSTAGE];
    chan_t   *ch[MAXSTAGE];
    filt_t    fa[MAXSTAGE];
    int       nth = 0, nch = 0;

    chan_t *head = chan_create(sizeof(int), 1);
    ch[nch++] = head;
    pthread_create(&th[nth++], NULL, generate, head);

    printf("Concurrent prime sieve — one filter thread per prime, channels chained into a pipeline\n\n");

    int count = 0, prime;
    chan_t *cur = head;
    while (chan_recv(cur, &prime) == CHAN_OK) {        /* first number of each stage is prime */
        printf("%4d%s", prime, (++count % 10 == 0) ? "\n" : " ");
        chan_t *next = chan_create(sizeof(int), 1);
        ch[nch++] = next;
        fa[nth] = (filt_t){ cur, next, prime };
        pthread_create(&th[nth], NULL, filter, &fa[nth]);
        nth++;
        cur = next;
    }

    printf("\n\nSieved %d primes (≤ %d), using %d threads.\n", count, LIMIT, nth);

    for (int i = 0; i < nth; i++) pthread_join(th[i], NULL);
    for (int i = 0; i < nch; i++) chan_destroy(ch[i]);
    return 0;
}
