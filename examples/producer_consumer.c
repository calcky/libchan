#include <stdio.h>
#include <pthread.h>
#include "libchan.h"

#define N_ITEMS 5

static void *producer(void *arg) {
    chan_t *ch = arg;
    for (int i = 0; i < N_ITEMS; i++) {
        printf("send %d\n", i);
        chan_send(ch, &i);
    }
    chan_close(ch);
    return NULL;
}

int main(void) {
    chan_t *ch = chan_create(sizeof(int), 2);

    pthread_t t;
    pthread_create(&t, NULL, producer, ch);

    int v;
    while (chan_recv(ch, &v) == CHAN_OK)
        printf("recv %d\n", v);

    pthread_join(t, NULL);
    chan_destroy(ch);
    return 0;
}
