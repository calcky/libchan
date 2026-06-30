/*
 * request_reply.c — Request / reply RPC over channels
 *
 * The classic Go-style pattern: each request carries its own reply channel so
 * many clients can share one server without mixing up responses.
 *
 *   client ──► [requests] ──► server ──► chan_send(req.reply, &answer) ──► client recv
 *
 * reply channels are unbuffered (cap=0) for synchronous handoff; the shared
 * request queue is buffered for modest fan-in.
 */
#include <stdio.h>
#include <pthread.h>

#include "libchan.h"

#define NCLIENTS 6
#define NREQ     5

typedef struct {
    int     client_id;
    int     seq;
    chan_t *reply;   /* per-request reply mailbox; server writes, client reads */
} request_t;

typedef struct { chan_t *requests; int cid; } client_arg_t;

static int square(int x) { return x * x; }

static void *server(void *arg) {
    chan_t *requests = arg;
    request_t req;
    while (chan_recv(requests, &req) == CHAN_OK) {
        int answer = square(req.seq);
        chan_send(req.reply, &answer);
        chan_destroy(req.reply);
    }
    return NULL;
}

static void *client(void *arg) {
    client_arg_t *ca = arg;
    for (int seq = 1; seq <= NREQ; seq++) {
        chan_t *reply = chan_create(sizeof(int), 0);
        request_t req = { .client_id = ca->cid, .seq = seq, .reply = reply };
        chan_send(ca->requests, &req);

        int answer;
        chan_recv(reply, &answer);
        printf("  client %d  req seq=%d  →  %d² = %d\n",
               ca->cid, seq, seq, answer);
        /* reply destroyed by server after send */
    }
    return NULL;
}

int main(void) {
    chan_t *requests = chan_create(sizeof(request_t), 8);

    printf("request_reply — %d clients × %d RPCs on one server\n\n",
           NCLIENTS, NREQ);

    pthread_t st;
    pthread_create(&st, NULL, server, requests);

    pthread_t cth[NCLIENTS];
    client_arg_t ca[NCLIENTS];
    for (int i = 0; i < NCLIENTS; i++) {
        ca[i] = (client_arg_t){ requests, i };
        pthread_create(&cth[i], NULL, client, &ca[i]);
    }
    for (int i = 0; i < NCLIENTS; i++)
        pthread_join(cth[i], NULL);

    chan_close(requests);
    pthread_join(st, NULL);
    chan_destroy(requests);

    printf("\nAll %d RPCs completed — each reply routed via its own channel.\n",
           NCLIENTS * NREQ);
    return 0;
}
