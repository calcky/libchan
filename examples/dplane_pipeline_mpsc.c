/*
 * dplane_pipeline_mpsc.c — Simulated data-plane pipeline (per-node MPSC inboxes)
 *
 * Each node thread owns one chan_create inbox (MPMC ring, single consumer).
 * Other threads send packets to that inbox.  Same 6 paths and CLI as
 * dplane_pipeline.c (SPSC per-link version).
 *
 * Inboxes (consumer → producers):
 *   in_tun      tun      ← main, forward
 *   in_forward  forward  ← tun, tunnel, oam, enc
 *   in_enc      enc      ← forward, tunnel
 *   in_tunnel   tunnel   ← main, forward, enc
 *   in_oam      oam      ← main, forward
 *   in_done     main     ← tun, tunnel, oam (sink completions)
 *
 * Build & run:
 *   cmake --build build --target dplane_pipeline_mpsc
 *   ./build/examples/dplane_pipeline_mpsc -l 1 -c 10000 -m spin
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sched.h>

#include "libchan.h"

#define CHAN_CAP              4096
#define CHAN_CAP_HOT          (CHAN_CAP * 2)
#define DRAIN_SEC             30
#define BLOCK_WAIT_USEC_DEFAULT 100

typedef enum {
    PATH_TUN_FORWARD_ENC_TUNNEL,
    PATH_TUNNEL_ENC_FORWARD_TUN,
    PATH_TUNNEL_FORWARD_TUN,
    PATH_TUN_FORWARD_TUNNEL,
    PATH_TUNNEL_FORWARD_OAM,
    PATH_OAM_FORWARD_TUNNEL,
    PATH_COUNT
} path_id_t;

typedef struct {
    uint32_t  seq;
    path_id_t path;
} pkt_t;

typedef struct {
    path_id_t path;
    uint32_t  seq;
    int64_t   done_ns;
} done_msg_t;

typedef struct {
    chan_t *in_tun;
    chan_t *in_forward;
    chan_t *in_enc;
    chan_t *in_tunnel;
    chan_t *in_oam;
    chan_t *in_done;
} inboxes_t;

typedef enum {
    POLL_SPIN,
    POLL_YIELD,
    POLL_BACKOFF,
    POLL_BLOCK,
} poll_mode_t;

typedef struct {
    inboxes_t   box;
    poll_mode_t mode;
    uint32_t    block_wait_usec;
    volatile bool stop;
} plane_t;

typedef struct {
    bool     enabled;
    uint64_t sent;
    uint64_t recv;
    uint64_t dup;
    uint64_t lost;
    uint32_t next_expected;
} path_stat_t;

typedef struct {
    bool        path_enabled[PATH_COUNT];
    uint64_t    count;
    double      duration_sec;
    uint32_t    interval_usec;
    bool        parallel;
    poll_mode_t poll_mode;
    uint32_t    block_wait_usec;
} run_config_t;

typedef struct {
    int64_t  *send_ts;
    uint64_t  send_cap;
    uint64_t *samples;
    uint64_t  n;
    uint64_t  cap;
    uint64_t  sum;
} path_latency_t;

typedef struct {
    path_latency_t path[PATH_COUNT];
} latency_ctx_t;

static int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static void poll_idle(const plane_t *pl, int *idle_spins);

static void sleep_usec(uint32_t usec) {
    if (usec == 0) return;
    struct timespec ts = {
        .tv_sec  = (time_t)(usec / 1000000u),
        .tv_nsec = (long)(usec % 1000000u) * 1000L,
    };
    nanosleep(&ts, NULL);
}

static inline void spin_pause(void) {
#if defined(__x86_64__) || defined(__i386__)
    __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__)
    __asm__ volatile("yield" ::: "memory");
#endif
}

static const char *path_name(path_id_t path) {
    static const char *names[] = {
        "tun->forward->enc->tunnel",
        "tunnel->enc->forward->tun",
        "tunnel->forward->tun",
        "tun->forward->tunnel",
        "tunnel->forward->oam",
        "oam->forward->tunnel",
    };
    return names[path];
}

static bool mpsc_send(chan_t *ch, const void *pkt) {
    return chan_send(ch, pkt) == CHAN_OK;
}

static void node_idle_one(const plane_t *pl, chan_t *inbox, int *idle) {
    if (pl->mode == POLL_BLOCK && inbox && chan_len(inbox) > 0) {
        spin_pause();
        return;
    }
    poll_idle(pl, idle);
}

static bool inbox_done(const plane_t *pl, chan_t *inbox) {
    return chan_is_closed(inbox);
}

/* ---- latency & stats (same as SPSC version) ------------------------- */

static int lat_cmp(const void *a, const void *b) {
    uint64_t va = *(const uint64_t *)a;
    uint64_t vb = *(const uint64_t *)b;
    return (va > vb) - (va < vb);
}

static bool latency_grow_send(path_latency_t *lat, uint32_t seq) {
    uint64_t need = (uint64_t)seq + 1;
    if (need <= lat->send_cap) return true;
    uint64_t new_cap = lat->send_cap ? lat->send_cap : 4096;
    while (new_cap < need) new_cap *= 2;
    int64_t *p = realloc(lat->send_ts, new_cap * sizeof(int64_t));
    if (!p) return false;
    memset(p + lat->send_cap, 0, (new_cap - lat->send_cap) * sizeof(int64_t));
    lat->send_ts = p;
    lat->send_cap = new_cap;
    return true;
}

static bool latency_push(path_latency_t *lat, uint64_t ns) {
    if (lat->n >= lat->cap) {
        uint64_t new_cap = lat->cap ? lat->cap * 2 : 4096;
        uint64_t *p = realloc(lat->samples, new_cap * sizeof(uint64_t));
        if (!p) return false;
        lat->samples = p;
        lat->cap = new_cap;
    }
    lat->samples[lat->n++] = ns;
    lat->sum += ns;
    return true;
}

static bool latency_ctx_init(latency_ctx_t *ctx, const path_stat_t *stats,
                             const run_config_t *cfg) {
    memset(ctx, 0, sizeof(*ctx));
    uint64_t hint = cfg->duration_sec > 0.0 ? 4096 : (cfg->count ? cfg->count : 1);
    for (int i = 0; i < PATH_COUNT; i++) {
        if (!stats[i].enabled) continue;
        path_latency_t *lat = &ctx->path[i];
        lat->send_cap = hint;
        lat->send_ts = calloc(hint, sizeof(int64_t));
        if (!lat->send_ts) return false;
        lat->cap = hint;
        lat->samples = malloc(hint * sizeof(uint64_t));
        if (!lat->samples) return false;
    }
    return true;
}

static void latency_ctx_free(latency_ctx_t *ctx) {
    for (int i = 0; i < PATH_COUNT; i++) {
        free(ctx->path[i].send_ts);
        free(ctx->path[i].samples);
    }
}

static void latency_on_send(latency_ctx_t *ctx, path_id_t path, uint32_t seq) {
    path_latency_t *lat = &ctx->path[path];
    if (!latency_grow_send(lat, seq)) return;
    lat->send_ts[seq] = now_ns();
}

static void latency_on_recv(latency_ctx_t *ctx, path_id_t path, uint32_t seq,
                            int64_t done_ns) {
    path_latency_t *lat = &ctx->path[path];
    if (seq >= lat->send_cap || lat->send_ts[seq] == 0) return;
    int64_t t0 = lat->send_ts[seq];
    lat->send_ts[seq] = 0;
    uint64_t delta = (done_ns > t0) ? (uint64_t)(done_ns - t0) : 1;
    latency_push(lat, delta);
}

static uint64_t latency_percentile(uint64_t *sorted, uint64_t n, double p) {
    if (n == 0) return 0;
    if (n == 1) return sorted[0];
    double rank = p * (double)(n - 1);
    uint64_t lo = (uint64_t)rank;
    uint64_t hi = lo + 1;
    if (hi >= n) return sorted[n - 1];
    double frac = rank - (double)lo;
    return (uint64_t)((double)sorted[lo] + frac * (double)(sorted[hi] - sorted[lo]));
}

typedef struct {
    uint64_t avg, p50, p90, p99;
    uint64_t n;
} latency_summary_t;

static latency_summary_t latency_summarize(const path_latency_t *lat) {
    latency_summary_t out = {0};
    if (lat->n == 0) return out;
    uint64_t *sorted = malloc(lat->n * sizeof(uint64_t));
    if (!sorted) return out;
    memcpy(sorted, lat->samples, lat->n * sizeof(uint64_t));
    qsort(sorted, lat->n, sizeof(uint64_t), lat_cmp);
    out.n = lat->n;
    out.avg = lat->sum / lat->n;
    out.p50 = latency_percentile(sorted, lat->n, 0.50);
    out.p90 = latency_percentile(sorted, lat->n, 0.90);
    out.p99 = latency_percentile(sorted, lat->n, 0.99);
    free(sorted);
    return out;
}

static latency_summary_t latency_summarize_all(const latency_ctx_t *ctx,
                                               const path_stat_t *stats) {
    path_latency_t merged = {0};
    for (int i = 0; i < PATH_COUNT; i++) {
        if (!stats[i].enabled) continue;
        const path_latency_t *lat = &ctx->path[i];
        for (uint64_t j = 0; j < lat->n; j++)
            latency_push(&merged, lat->samples[j]);
    }
    latency_summary_t out = latency_summarize(&merged);
    free(merged.samples);
    return out;
}

static void print_latency_us(const char *label, const latency_summary_t *s) {
    if (s->n == 0) {
        printf("%s: (no samples)\n", label);
        return;
    }
    printf("%s (%llu samples): avg=%.2f us  p50=%.2f us  p90=%.2f us  p99=%.2f us\n",
           label, (unsigned long long)s->n,
           (double)s->avg / 1000.0, (double)s->p50 / 1000.0,
           (double)s->p90 / 1000.0, (double)s->p99 / 1000.0);
}

static void stat_on_recv(path_stat_t *stat, path_id_t path, uint32_t seq,
                         int64_t done_ns, latency_ctx_t *lat) {
    if (!stat->enabled) return;
    if (seq < stat->next_expected) {
        stat->dup++;
        return;
    }
    if (seq > stat->next_expected) {
        stat->lost += (uint64_t)(seq - stat->next_expected);
        stat->next_expected = seq + 1;
        stat->recv++;
        if (lat) latency_on_recv(lat, path, seq, done_ns);
        return;
    }
    stat->next_expected++;
    stat->recv++;
    if (lat) latency_on_recv(lat, path, seq, done_ns);
}

static bool poll_done_try(plane_t *pl, path_stat_t *stats, latency_ctx_t *lat) {
    done_msg_t done;
    if (chan_try_recv(pl->box.in_done, &done) != CHAN_OK)
        return false;
    stat_on_recv(&stats[done.path], done.path, done.seq, done.done_ns, lat);
    return true;
}

static bool poll_done_wait(plane_t *pl, path_stat_t *stats, latency_ctx_t *lat,
                           int *idle) {
    if (poll_done_try(pl, stats, lat)) {
        *idle = 0;
        return true;
    }
    poll_idle(pl, idle);
    return false;
}

static uint64_t total_sent(const path_stat_t *stats) {
    uint64_t n = 0;
    for (int i = 0; i < PATH_COUNT; i++)
        if (stats[i].enabled) n += stats[i].sent;
    return n;
}

static uint64_t total_recv(const path_stat_t *stats) {
    uint64_t n = 0;
    for (int i = 0; i < PATH_COUNT; i++)
        if (stats[i].enabled) n += stats[i].recv;
    return n;
}

static bool all_drained(const path_stat_t *stats) {
    for (int i = 0; i < PATH_COUNT; i++) {
        if (!stats[i].enabled) continue;
        if (stats[i].recv + stats[i].dup < stats[i].sent) return false;
    }
    return true;
}

static bool path_drained(const path_stat_t *stat) {
    return stat->recv + stat->dup >= stat->sent;
}

static int enabled_path_count(const path_stat_t *stats) {
    int n = 0;
    for (int i = 0; i < PATH_COUNT; i++)
        if (stats[i].enabled) n++;
    return n;
}

/* ---- poll modes ----------------------------------------------------- */

static const char *poll_mode_name(poll_mode_t mode) {
    switch (mode) {
    case POLL_SPIN:    return "spin";
    case POLL_YIELD:   return "yield";
    case POLL_BACKOFF: return "backoff";
    case POLL_BLOCK:   return "block";
    }
    return "?";
}

static poll_mode_t parse_poll_mode(const char *s) {
    if (strcmp(s, "spin") == 0)    return POLL_SPIN;
    if (strcmp(s, "yield") == 0)   return POLL_YIELD;
    if (strcmp(s, "backoff") == 0) return POLL_BACKOFF;
    if (strcmp(s, "block") == 0)   return POLL_BLOCK;
    return (poll_mode_t)-1;
}

static int parse_mode_opt(const char *s, poll_mode_t *mode, uint32_t *block_wait_usec) {
    const char *colon = strchr(s, ':');
    if (colon) {
        size_t nlen = (size_t)(colon - s);
        if (nlen == 0 || nlen >= 16) return -1;
        char name[16];
        memcpy(name, s, nlen);
        name[nlen] = '\0';
        poll_mode_t m = parse_poll_mode(name);
        if ((int)m < 0) return -1;
        if (m != POLL_BLOCK) {
            fprintf(stderr, "-m: only block supports :USEC suffix (got '%s')\n", s);
            return -1;
        }
        char *end;
        unsigned long w = strtoul(colon + 1, &end, 10);
        if (end == colon + 1 || *end != '\0' || w == 0) {
            fprintf(stderr, "-m: invalid block wait '%s'\n", colon + 1);
            return -1;
        }
        *mode = m;
        *block_wait_usec = (uint32_t)w;
        return 0;
    }
    poll_mode_t m = parse_poll_mode(s);
    if ((int)m < 0) return -1;
    *mode = m;
    return 0;
}

static void poll_idle(const plane_t *pl, int *idle_spins) {
    switch (pl->mode) {
    case POLL_SPIN:
        break;
    case POLL_YIELD:
        sched_yield();
        break;
    case POLL_BACKOFF:
        if (*idle_spins < 40)
            spin_pause();
        else if (*idle_spins < 200)
            sched_yield();
        else
            sleep_usec(100);
        (*idle_spins)++;
        break;
    case POLL_BLOCK:
        if (*idle_spins < 40)
            spin_pause();
        else if (*idle_spins < 200)
            sched_yield();
        else
            sleep_usec(pl->block_wait_usec > 0 ? pl->block_wait_usec : 1);
        (*idle_spins)++;
        break;
    }
}

/* ---- routing -------------------------------------------------------- */

static void send_done(plane_t *pl, const pkt_t *pkt) {
    done_msg_t done = { pkt->path, pkt->seq, now_ns() };
    mpsc_send(pl->box.in_done, &done);
}

static void forward_route(plane_t *pl, const pkt_t *pkt) {
    switch (pkt->path) {
    case PATH_TUN_FORWARD_ENC_TUNNEL:
        mpsc_send(pl->box.in_enc, pkt);
        break;
    case PATH_TUN_FORWARD_TUNNEL:
    case PATH_OAM_FORWARD_TUNNEL:
        mpsc_send(pl->box.in_tunnel, pkt);
        break;
    case PATH_TUNNEL_ENC_FORWARD_TUN:
    case PATH_TUNNEL_FORWARD_TUN:
        mpsc_send(pl->box.in_tun, pkt);
        break;
    case PATH_TUNNEL_FORWARD_OAM:
        mpsc_send(pl->box.in_oam, pkt);
        break;
    default:
        fprintf(stderr, "forward: unknown path %d\n", (int)pkt->path);
        break;
    }
}

static void handle_enc_pkt(plane_t *pl, const pkt_t *pkt) {
    if (pkt->path == PATH_TUN_FORWARD_ENC_TUNNEL)
        mpsc_send(pl->box.in_tunnel, pkt);
    else if (pkt->path == PATH_TUNNEL_ENC_FORWARD_TUN)
        mpsc_send(pl->box.in_forward, pkt);
}

static void tunnel_outbound(plane_t *pl, const pkt_t *pkt) {
    if (pkt->path == PATH_TUNNEL_ENC_FORWARD_TUN)
        mpsc_send(pl->box.in_enc, pkt);
    else
        mpsc_send(pl->box.in_forward, pkt);
}

static bool tun_pkt_forward(const pkt_t *pkt) {
    return pkt->path == PATH_TUN_FORWARD_ENC_TUNNEL ||
           pkt->path == PATH_TUN_FORWARD_TUNNEL;
}

static bool tun_pkt_sink(const pkt_t *pkt) {
    return pkt->path == PATH_TUNNEL_ENC_FORWARD_TUN ||
           pkt->path == PATH_TUNNEL_FORWARD_TUN;
}

static bool tunnel_pkt_outbound(const pkt_t *pkt) {
    return pkt->path == PATH_TUNNEL_ENC_FORWARD_TUN ||
           pkt->path == PATH_TUNNEL_FORWARD_TUN ||
           pkt->path == PATH_TUNNEL_FORWARD_OAM;
}

static bool tunnel_pkt_sink(const pkt_t *pkt) {
    return pkt->path == PATH_TUN_FORWARD_ENC_TUNNEL ||
           pkt->path == PATH_TUN_FORWARD_TUNNEL ||
           pkt->path == PATH_OAM_FORWARD_TUNNEL;
}

/* ---- node threads --------------------------------------------------- */

static void *tun_node(void *arg) {
    plane_t *pl = arg;
    pkt_t pkt;
    int idle = 0;

    while (!pl->stop) {
        if (chan_try_recv(pl->box.in_tun, &pkt) == CHAN_OK) {
            if (tun_pkt_forward(&pkt))
                mpsc_send(pl->box.in_forward, &pkt);
            else if (tun_pkt_sink(&pkt))
                send_done(pl, &pkt);
            idle = 0;
            continue;
        }
        if (inbox_done(pl, pl->box.in_tun)) break;
        node_idle_one(pl, pl->box.in_tun, &idle);
    }
    return NULL;
}

static void *forward_node(void *arg) {
    plane_t *pl = arg;
    pkt_t pkt;
    int idle = 0;

    while (!pl->stop) {
        if (chan_try_recv(pl->box.in_forward, &pkt) == CHAN_OK) {
            forward_route(pl, &pkt);
            idle = 0;
            continue;
        }
        if (inbox_done(pl, pl->box.in_forward)) break;
        node_idle_one(pl, pl->box.in_forward, &idle);
    }
    return NULL;
}

static void *enc_node(void *arg) {
    plane_t *pl = arg;
    pkt_t pkt;
    int idle = 0;

    while (!pl->stop) {
        if (chan_try_recv(pl->box.in_enc, &pkt) == CHAN_OK) {
            handle_enc_pkt(pl, &pkt);
            idle = 0;
            continue;
        }
        if (inbox_done(pl, pl->box.in_enc)) break;
        node_idle_one(pl, pl->box.in_enc, &idle);
    }
    return NULL;
}

static void *tunnel_node(void *arg) {
    plane_t *pl = arg;
    pkt_t pkt;
    int idle = 0;

    while (!pl->stop) {
        if (chan_try_recv(pl->box.in_tunnel, &pkt) == CHAN_OK) {
            if (tunnel_pkt_outbound(&pkt))
                tunnel_outbound(pl, &pkt);
            else if (tunnel_pkt_sink(&pkt))
                send_done(pl, &pkt);
            idle = 0;
            continue;
        }
        if (inbox_done(pl, pl->box.in_tunnel)) break;
        node_idle_one(pl, pl->box.in_tunnel, &idle);
    }
    return NULL;
}

static void *oam_node(void *arg) {
    plane_t *pl = arg;
    pkt_t pkt;
    int idle = 0;

    while (!pl->stop) {
        if (chan_try_recv(pl->box.in_oam, &pkt) == CHAN_OK) {
            if (pkt.path == PATH_OAM_FORWARD_TUNNEL)
                mpsc_send(pl->box.in_forward, &pkt);
            else if (pkt.path == PATH_TUNNEL_FORWARD_OAM)
                send_done(pl, &pkt);
            idle = 0;
            continue;
        }
        if (inbox_done(pl, pl->box.in_oam)) break;
        node_idle_one(pl, pl->box.in_oam, &idle);
    }
    return NULL;
}

/* ---- inboxes setup -------------------------------------------------- */

static bool inboxes_init(inboxes_t *box) {
    box->in_tun     = chan_create(sizeof(pkt_t), CHAN_CAP);
    box->in_forward = chan_create(sizeof(pkt_t), CHAN_CAP_HOT);
    box->in_enc     = chan_create(sizeof(pkt_t), CHAN_CAP);
    box->in_tunnel  = chan_create(sizeof(pkt_t), CHAN_CAP_HOT);
    box->in_oam     = chan_create(sizeof(pkt_t), CHAN_CAP);
    box->in_done    = chan_create(sizeof(done_msg_t), CHAN_CAP_HOT);
    return box->in_tun && box->in_forward && box->in_enc &&
           box->in_tunnel && box->in_oam && box->in_done;
}

static void chan_destroy_null(chan_t *ch) { if (ch) chan_destroy(ch); }

static void inboxes_destroy(inboxes_t *box) {
    chan_destroy_null(box->in_tun);
    chan_destroy_null(box->in_forward);
    chan_destroy_null(box->in_enc);
    chan_destroy_null(box->in_tunnel);
    chan_destroy_null(box->in_oam);
    chan_destroy_null(box->in_done);
}

static void close_all(plane_t *pl) {
    chan_t *all[] = {
        pl->box.in_tun, pl->box.in_forward, pl->box.in_enc,
        pl->box.in_tunnel, pl->box.in_oam, pl->box.in_done,
    };
    for (size_t i = 0; i < sizeof(all) / sizeof(all[0]); i++)
        chan_close(all[i]);
}

static chan_t *inject_inbox(inboxes_t *box, path_id_t path) {
    switch (path) {
    case PATH_TUN_FORWARD_ENC_TUNNEL:
    case PATH_TUN_FORWARD_TUNNEL:    return box->in_tun;
    case PATH_TUNNEL_ENC_FORWARD_TUN:
    case PATH_TUNNEL_FORWARD_TUN:
    case PATH_TUNNEL_FORWARD_OAM:     return box->in_tunnel;
    case PATH_OAM_FORWARD_TUNNEL:    return box->in_oam;
    default:                         return NULL;
    }
}

static bool inject_packet(plane_t *pl, path_stat_t *stats, path_id_t path,
                          latency_ctx_t *lat) {
    uint32_t seq = (uint32_t)stats[path].sent;
    pkt_t pkt = { .seq = seq, .path = path };
    if (lat) latency_on_send(lat, path, seq);
    chan_t *inj = inject_inbox(&pl->box, path);
    if (!mpsc_send(inj, &pkt)) return false;
    stats[path].sent++;
    return true;
}

static int drain_remaining(plane_t *pl, path_stat_t *stats, latency_ctx_t *lat,
                           int timeout_sec) {
    int idle = 0;
    int64_t deadline = now_ns() + (int64_t)timeout_sec * 1000000000LL;
    while (now_ns() < deadline) {
        if (all_drained(stats)) return 0;
        poll_done_wait(pl, stats, lat, &idle);
    }
    return -1;
}

static int drain_path(plane_t *pl, path_stat_t *stats, path_id_t path,
                      latency_ctx_t *lat, int timeout_sec) {
    int idle = 0;
    int64_t deadline = now_ns() + (int64_t)timeout_sec * 1000000000LL;
    while (now_ns() < deadline) {
        if (path_drained(&stats[path])) return 0;
        poll_done_wait(pl, stats, lat, &idle);
    }
    return -1;
}

static int inject_round(plane_t *pl, path_stat_t *stats, const run_config_t *cfg,
                        latency_ctx_t *lat) {
    for (path_id_t path = 0; path < PATH_COUNT; path++) {
        if (!stats[path].enabled) continue;
        if (!inject_packet(pl, stats, path, lat)) {
            fprintf(stderr, "inject blocked on path [%d]\n", (int)path + 1);
            return -1;
        }
        poll_done_try(pl, stats, lat);
        sleep_usec(cfg->interval_usec);
    }
    return 0;
}

static int run_traffic_parallel(plane_t *pl, path_stat_t *stats,
                                const run_config_t *cfg, latency_ctx_t *lat) {
    const bool timed = cfg->duration_sec > 0.0;
    int64_t deadline = timed
        ? now_ns() + (int64_t)(cfg->duration_sec * 1e9)
        : 0;

    if (timed) {
        printf("Timed run %.1f s, parallel, interval %u us\n",
               cfg->duration_sec, cfg->interval_usec);
        while (now_ns() < deadline) {
            if (inject_round(pl, stats, cfg, lat) != 0) return -1;
        }
    } else {
        printf("Count run %llu pkt/path, parallel, interval %u us\n",
               (unsigned long long)cfg->count, cfg->interval_usec);
        for (uint64_t n = 0; n < cfg->count; n++) {
            if (inject_round(pl, stats, cfg, lat) != 0) {
                fprintf(stderr, "inject blocked at round %llu\n",
                        (unsigned long long)n);
                return -1;
            }
        }
    }
    return 0;
}

static int run_traffic_sequential(plane_t *pl, path_stat_t *stats,
                                  const run_config_t *cfg, latency_ctx_t *lat) {
    const bool timed = cfg->duration_sec > 0.0;

    if (timed) {
        printf("Timed run %.1f s/path, sequential, interval %u us\n",
               cfg->duration_sec, cfg->interval_usec);
        for (path_id_t path = 0; path < PATH_COUNT; path++) {
            if (!stats[path].enabled) continue;
            int64_t deadline = now_ns() + (int64_t)(cfg->duration_sec * 1e9);
            printf("  path [%d] %s\n", (int)path + 1, path_name(path));
            while (now_ns() < deadline) {
                if (!inject_packet(pl, stats, path, lat)) {
                    fprintf(stderr, "inject blocked on path [%d]\n", (int)path + 1);
                    return -1;
                }
                poll_done_try(pl, stats, lat);
                sleep_usec(cfg->interval_usec);
            }
            if (drain_path(pl, stats, path, lat, DRAIN_SEC) != 0) {
                fprintf(stderr, "drain timeout on path [%d]\n", (int)path + 1);
                return -1;
            }
        }
    } else {
        printf("Count run %llu pkt/path, sequential, interval %u us\n",
               (unsigned long long)cfg->count, cfg->interval_usec);
        for (path_id_t path = 0; path < PATH_COUNT; path++) {
            if (!stats[path].enabled) continue;
            printf("  path [%d] %s\n", (int)path + 1, path_name(path));
            for (uint64_t n = 0; n < cfg->count; n++) {
                if (!inject_packet(pl, stats, path, lat)) {
                    fprintf(stderr, "inject blocked on path [%d] seq %llu\n",
                            (int)path + 1, (unsigned long long)n);
                    return -1;
                }
                poll_done_try(pl, stats, lat);
                sleep_usec(cfg->interval_usec);
            }
            if (drain_path(pl, stats, path, lat, DRAIN_SEC) != 0) {
                fprintf(stderr, "drain timeout on path [%d]\n", (int)path + 1);
                return -1;
            }
        }
    }
    return 0;
}

static int run_traffic(plane_t *pl, path_stat_t *stats, const run_config_t *cfg,
                       latency_ctx_t *lat) {
    const bool multi = enabled_path_count(stats) > 1;
    int rc;

    if (multi && !cfg->parallel)
        rc = run_traffic_sequential(pl, stats, cfg, lat);
    else
        rc = run_traffic_parallel(pl, stats, cfg, lat);
    if (rc != 0) return rc;

    printf("Inject done — sent %llu, draining up to %d s...\n",
           (unsigned long long)total_sent(stats), DRAIN_SEC);
    return drain_remaining(pl, stats, lat, DRAIN_SEC);
}

static bool path_ok(const path_stat_t *stat) {
    if (!stat->enabled) return true;
    return stat->lost == 0 && stat->dup == 0 && stat->recv == stat->sent;
}

static void print_report(const path_stat_t *stats, const latency_ctx_t *lat,
                         int64_t elapsed_ns) {
    printf("\n%-4s  %-32s  %8s  %8s  %6s  %6s  %4s\n",
           "path", "pipeline", "sent", "recv", "lost", "dup", "ok");
    printf("%-4s  %-32s  %8s  %8s  %6s  %6s  %4s\n",
           "----", "--------------------------------", "--------",
           "--------", "------", "------", "----");
    int failures = 0;
    for (path_id_t path = 0; path < PATH_COUNT; path++) {
        if (!stats[path].enabled) continue;
        bool ok = path_ok(&stats[path]);
        if (!ok) failures++;
        printf("[%d]  %-32s  %8llu  %8llu  %6llu  %6llu  %4s\n",
               (int)path + 1, path_name(path),
               (unsigned long long)stats[path].sent,
               (unsigned long long)stats[path].recv,
               (unsigned long long)stats[path].lost,
               (unsigned long long)stats[path].dup,
               ok ? "yes" : "NO");
    }
    double sec = (double)elapsed_ns / 1e9;
    uint64_t recv = total_recv(stats);
    printf("\nTotal recv %llu in %.3f s (%.2f pkt/s)\n",
           (unsigned long long)recv, sec, sec > 0 ? recv / sec : 0.0);

    if (lat) {
        printf("\nLatency (inject → done, us):\n");
        for (path_id_t path = 0; path < PATH_COUNT; path++) {
            if (!stats[path].enabled) continue;
            latency_summary_t s = latency_summarize(&lat->path[path]);
            char label[64];
            snprintf(label, sizeof(label), "  [%d] %s", (int)path + 1, path_name(path));
            print_latency_us(label, &s);
        }
        latency_summary_t all = latency_summarize_all(lat, stats);
        print_latency_us("  ALL", &all);
    }

    if (failures)
        printf("\nFAILED: %d path(s) — loss/dup/mismatch (sent != recv)\n", failures);
    else
        printf("\nPASSED: all enabled paths — in-order seq, zero loss, zero dup\n");
}

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s [options]\n"
        "\n"
        "  Same options as dplane_pipeline (SPSC).  Uses per-node MPSC inboxes\n"
        "  (chan_create) instead of per-link chan_create_spsc.\n"
        "\n"
        "  -l LIST   Comma-separated path ids 1..6 (default: all)\n"
        "  -c N      Inject N packets per enabled path (default: 1)\n"
        "  -t SEC    Run for SEC seconds instead of -c\n"
        "  -i USEC   Microseconds between injections (default: 0)\n"
        "  -P        Parallel inject when -l selects multiple paths\n"
        "  -m MODE   spin | yield | backoff | block[:USEC]\n"
        "  -h        Show this help\n",
        prog);
}

static int parse_path_list(const char *list, bool *enabled) {
    memset(enabled, 0, PATH_COUNT * sizeof(bool));
    const char *p = list;
    int n = 0;
    while (*p) {
        char *end;
        long v = strtol(p, &end, 10);
        if (end == p || v < 1 || v > PATH_COUNT) {
            fprintf(stderr, "invalid path id in -l: '%s'\n", list);
            return -1;
        }
        if (!enabled[v - 1]) n++;
        enabled[v - 1] = true;
        p = end;
        if (*p == ',') p++;
        else if (*p != '\0') {
            fprintf(stderr, "invalid -l list: '%s'\n", list);
            return -1;
        }
    }
    return n;
}

static int parse_args(int argc, char **argv, run_config_t *cfg, path_stat_t *stats) {
    memset(cfg, 0, sizeof(*cfg));
    cfg->count = 1;
    cfg->poll_mode = POLL_SPIN;
    for (int i = 0; i < PATH_COUNT; i++) {
        stats[i].enabled = true;
        cfg->path_enabled[i] = true;
    }

    int opt;
    while ((opt = getopt(argc, argv, "l:c:t:i:m:Ph")) != -1) {
        switch (opt) {
        case 'l': {
            int n = parse_path_list(optarg, cfg->path_enabled);
            if (n < 0) return -1;
            if (n == 0) {
                fprintf(stderr, "-l: no paths selected\n");
                return -1;
            }
            for (int i = 0; i < PATH_COUNT; i++)
                stats[i].enabled = cfg->path_enabled[i];
            break;
        }
        case 'c':
            cfg->count = (uint64_t)strtoull(optarg, NULL, 10);
            if (cfg->count == 0) {
                fprintf(stderr, "-c: count must be > 0\n");
                return -1;
            }
            cfg->duration_sec = 0;
            break;
        case 't':
            cfg->duration_sec = strtod(optarg, NULL);
            if (cfg->duration_sec <= 0) {
                fprintf(stderr, "-t: duration must be > 0\n");
                return -1;
            }
            break;
        case 'i':
            cfg->interval_usec = (uint32_t)strtoul(optarg, NULL, 10);
            break;
        case 'P':
            cfg->parallel = true;
            break;
        case 'm':
            if (parse_mode_opt(optarg, &cfg->poll_mode, &cfg->block_wait_usec) != 0) {
                fprintf(stderr,
                        "-m: unknown mode '%s' (use spin|yield|backoff|block[:USEC])\n",
                        optarg);
                return -1;
            }
            if (cfg->poll_mode == POLL_BLOCK && cfg->block_wait_usec == 0)
                cfg->block_wait_usec = BLOCK_WAIT_USEC_DEFAULT;
            break;
        case 'h':
            usage(argv[0]);
            return 1;
        default:
            usage(argv[0]);
            return -1;
        }
    }
    return 0;
}

int main(int argc, char **argv) {
    run_config_t cfg;
    path_stat_t stats[PATH_COUNT];
    memset(stats, 0, sizeof(stats));

    int pr = parse_args(argc, argv, &cfg, &stats);
    if (pr != 0) return pr > 0 ? 0 : 1;

    plane_t pl;
    memset(&pl, 0, sizeof(pl));
    pl.mode = cfg.poll_mode;
    pl.block_wait_usec = cfg.block_wait_usec;

    if (!inboxes_init(&pl.box)) {
        fprintf(stderr, "channel alloc failed\n");
        inboxes_destroy(&pl.box);
        return 1;
    }

    latency_ctx_t lat;
    if (!latency_ctx_init(&lat, stats, &cfg)) {
        fprintf(stderr, "latency alloc failed\n");
        inboxes_destroy(&pl.box);
        return 1;
    }

    pthread_t tun_thread, forward_thread, enc_thread, tunnel_thread, oam_thread;
    pthread_create(&tun_thread,     NULL, tun_node,     &pl);
    pthread_create(&forward_thread, NULL, forward_node, &pl);
    pthread_create(&enc_thread,     NULL, enc_node,     &pl);
    pthread_create(&tunnel_thread,  NULL, tunnel_node,  &pl);
    pthread_create(&oam_thread,     NULL, oam_node,     &pl);

    if (pl.mode == POLL_BLOCK)
        printf("dplane_pipeline_mpsc — MPSC inbox data-plane, poll mode: block (wait %u us)\n",
               pl.block_wait_usec);
    else
        printf("dplane_pipeline_mpsc — MPSC inbox data-plane, poll mode: %s\n",
               poll_mode_name(pl.mode));

    int64_t t0 = now_ns();
    int rc = run_traffic(&pl, stats, &cfg, &lat);
    int64_t elapsed = now_ns() - t0;

    pl.stop = true;
    close_all(&pl);

    pthread_join(tun_thread,     NULL);
    pthread_join(forward_thread, NULL);
    pthread_join(enc_thread,     NULL);
    pthread_join(tunnel_thread,  NULL);
    pthread_join(oam_thread,     NULL);

    print_report(stats, &lat, elapsed);

    inboxes_destroy(&pl.box);
    latency_ctx_free(&lat);

    if (rc != 0) return 1;
    for (path_id_t path = 0; path < PATH_COUNT; path++)
        if (!path_ok(&stats[path])) return 1;
    return 0;
}
