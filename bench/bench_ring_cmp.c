/*
 * bench_ring_cmp.c
 *
 * 量化 DPDK 4-游标 rte_ring 与 Vyukov per-slot MPMC 的吞吐差距。
 *
 * DPDK rte_ring：Phase 3 需自旋等待 prod.tail == 本轮预约位置后才能提交，
 * 多个并发生产者的提交串行化（convoy 效应）。
 *
 * Vyukov per-slot：每个槽独立持有 _Atomic uint64_t seq，生产者写完后直接
 * store_release 该槽的 seq，无跨生产者的 Phase 3 等待。
 *
 * 关键变量：
 *   - 线程数：1P1C / 2P2C / 4P4C / 8P8C
 *   - 元素大小：8 B（int64）和 64 B（一条 cache line）
 *     元素越大 Phase 2 memcpy 越慢 → rte_ring Phase 3 convoy 越严重
 */

#define _GNU_SOURCE
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

/* ------------------------------------------------------------------ */
/* 计时                                                                 */
/* ------------------------------------------------------------------ */

static inline int64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* ------------------------------------------------------------------ */
/* 自旋提示                                                             */
/* ------------------------------------------------------------------ */

static inline void spin_hint(void) {
#if defined(__x86_64__) || defined(__i386__)
    __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__)
    __asm__ volatile("yield" ::: "memory");
#endif
}

/* ------------------------------------------------------------------ */
/* DPDK 4-游标 rte_ring（与 src/ring_lf.c 相同逻辑，inline 实现）      */
/* ------------------------------------------------------------------ */

#define RING_ALIGNED __attribute__((aligned(64)))

typedef struct {
    struct { _Atomic uint32_t head; _Atomic uint32_t tail; } prod RING_ALIGNED;
    struct { _Atomic uint32_t head; _Atomic uint32_t tail; } cons RING_ALIGNED;
    uint32_t mask;
    uint32_t capacity;
    size_t   elem_size;
    char    *slots;
} dpdk_ring_t;

static bool dpdk_init(dpdk_ring_t *r, size_t cap, size_t elem_size) {
    /* 向上取整为 2 的幂 */
    uint32_t n = (uint32_t)cap - 1;
    n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8; n |= n >> 16;
    n++;
    r->slots = malloc((size_t)n * elem_size);
    if (!r->slots) return false;
    atomic_init(&r->prod.head, 0); atomic_init(&r->prod.tail, 0);
    atomic_init(&r->cons.head, 0); atomic_init(&r->cons.tail, 0);
    r->mask = n - 1; r->capacity = n; r->elem_size = elem_size;
    return true;
}
static void dpdk_destroy(dpdk_ring_t *r) { free(r->slots); }

/* 阻塞式 push（满时自旋等待）*/
static void dpdk_push_wait(dpdk_ring_t *r, const void *data) {
    uint32_t ph, pnext;
    for (;;) {
        ph = atomic_load_explicit(&r->prod.head, memory_order_relaxed);
        uint32_t ct = atomic_load_explicit(&r->cons.tail, memory_order_acquire);
        if ((uint32_t)(ph - ct) >= r->capacity) { spin_hint(); continue; }
        pnext = ph + 1;
        if (atomic_compare_exchange_weak_explicit(&r->prod.head, &ph, pnext,
                memory_order_relaxed, memory_order_relaxed))
            break;
    }
    /* Phase 2: 写数据 */
    memcpy(r->slots + (ph & r->mask) * r->elem_size, data, r->elem_size);
    /* Phase 3: 等待前驱提交，再推进 prod.tail */
    while (atomic_load_explicit(&r->prod.tail, memory_order_relaxed) != ph)
        spin_hint();
    atomic_store_explicit(&r->prod.tail, pnext, memory_order_release);
}

/* 阻塞式 pop（空时自旋等待）*/
static void dpdk_pop_wait(dpdk_ring_t *r, void *out) {
    uint32_t ch, cnext;
    for (;;) {
        ch = atomic_load_explicit(&r->cons.head, memory_order_relaxed);
        uint32_t pt = atomic_load_explicit(&r->prod.tail, memory_order_acquire);
        if (pt == ch) { spin_hint(); continue; }
        cnext = ch + 1;
        if (atomic_compare_exchange_weak_explicit(&r->cons.head, &ch, cnext,
                memory_order_relaxed, memory_order_relaxed))
            break;
    }
    memcpy(out, r->slots + (ch & r->mask) * r->elem_size, r->elem_size);
    while (atomic_load_explicit(&r->cons.tail, memory_order_relaxed) != ch)
        spin_hint();
    atomic_store_explicit(&r->cons.tail, cnext, memory_order_release);
}

/* ------------------------------------------------------------------ */
/* Vyukov per-slot MPMC                                                 */
/* ------------------------------------------------------------------ */

typedef struct {
    uint32_t          mask;
    uint32_t          capacity;
    size_t            elem_size;
    _Atomic uint64_t  prod_head;
    _Atomic uint64_t  cons_head;
    _Atomic uint64_t *seqs;    /* per-slot 序列号，初始化为 i */
    char             *slots;
} vy_ring_t;

static bool vy_init(vy_ring_t *r, size_t cap, size_t elem_size) {
    uint32_t n = (uint32_t)cap - 1;
    n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8; n |= n >> 16;
    n++;
    r->seqs  = malloc((size_t)n * sizeof(_Atomic uint64_t));
    r->slots = malloc((size_t)n * elem_size);
    if (!r->seqs || !r->slots) { free(r->seqs); free(r->slots); return false; }
    for (uint32_t i = 0; i < n; i++)
        atomic_init(&r->seqs[i], (uint64_t)i);
    atomic_init(&r->prod_head, 0);
    atomic_init(&r->cons_head, 0);
    r->mask = n - 1; r->capacity = n; r->elem_size = elem_size;
    return true;
}
static void vy_destroy(vy_ring_t *r) { free(r->seqs); free(r->slots); }

/*
 * Vyukov push 逻辑：
 *   pos 是本线程预约的写入位置。
 *   seq == pos → 槽空闲可写；seq < pos → 槽被占用（满）；seq > pos → CAS 失败需重载
 *
 * 写完后 store_release(seq, pos+1)，消费者看到 seq == pos+1 时读取数据。
 * 关键：不存在跨生产者的 Phase 3 自旋，每个生产者独立提交自己的槽。
 */
static void vy_push_wait(vy_ring_t *r, const void *data) {
    uint64_t pos = atomic_load_explicit(&r->prod_head, memory_order_relaxed);
    for (;;) {
        uint32_t idx = (uint32_t)(pos & r->mask);
        uint64_t seq = atomic_load_explicit(&r->seqs[idx], memory_order_acquire);
        int64_t  diff = (int64_t)(seq - pos);
        if (diff == 0) {
            if (atomic_compare_exchange_weak_explicit(&r->prod_head, &pos, pos + 1,
                    memory_order_relaxed, memory_order_relaxed))
                break;          /* 成功预约 */
            /* CAS 失败：另一生产者抢先，pos 已被更新为 expected 的新值，继续 */
        } else if (diff < 0) {
            /* 满：等待消费者释放 */
            spin_hint();
            pos = atomic_load_explicit(&r->prod_head, memory_order_relaxed);
        } else {
            /* diff > 0：另一生产者已拿走此槽，重载 pos */
            pos = atomic_load_explicit(&r->prod_head, memory_order_relaxed);
        }
    }
    memcpy(r->slots + (pos & r->mask) * r->elem_size, data, r->elem_size);
    /* 提交：让消费者看到 seq == pos + 1 */
    atomic_store_explicit(&r->seqs[pos & r->mask], pos + 1, memory_order_release);
}

/*
 * Vyukov pop 逻辑：
 *   seq == pos+1 → 数据就绪；seq < pos+1 → 空；seq > pos+1 → CAS 失败需重载
 *
 * 读完后 store_release(seq, pos + capacity)，让下一轮生产者看到槽空闲。
 */
static void vy_pop_wait(vy_ring_t *r, void *out) {
    uint64_t pos = atomic_load_explicit(&r->cons_head, memory_order_relaxed);
    for (;;) {
        uint32_t idx = (uint32_t)(pos & r->mask);
        uint64_t seq = atomic_load_explicit(&r->seqs[idx], memory_order_acquire);
        int64_t  diff = (int64_t)(seq - (pos + 1));
        if (diff == 0) {
            if (atomic_compare_exchange_weak_explicit(&r->cons_head, &pos, pos + 1,
                    memory_order_relaxed, memory_order_relaxed))
                break;
        } else if (diff < 0) {
            spin_hint();
            pos = atomic_load_explicit(&r->cons_head, memory_order_relaxed);
        } else {
            pos = atomic_load_explicit(&r->cons_head, memory_order_relaxed);
        }
    }
    memcpy(out, r->slots + (pos & r->mask) * r->elem_size, r->elem_size);
    /* 释放槽：下一轮生产者在 pos + capacity 时预约此槽 */
    atomic_store_explicit(&r->seqs[pos & r->mask],
                          pos + r->capacity, memory_order_release);
}

/* ------------------------------------------------------------------ */
/* 基准框架                                                             */
/* ------------------------------------------------------------------ */

#define RING_CAP   1024
#define TOTAL_OPS  4000000L   /* 每次测试总 send+recv 次数 */
#define WARMUP_OPS  200000L

typedef enum { IMPL_DPDK, IMPL_VYUKOV } impl_t;

typedef struct {
    impl_t        impl;
    void         *ring;      /* dpdk_ring_t* 或 vy_ring_t* */
    size_t        elem_size;
    _Atomic long  sent;      /* 全局已发送计数 */
    _Atomic long  recvd;     /* 全局已接收计数 */
    long          target;
} bench_state_t;

static void *producer_fn(void *arg) {
    bench_state_t *s = arg;
    char buf[64] = {0};
    for (;;) {
        long idx = atomic_fetch_add_explicit(&s->sent, 1, memory_order_relaxed);
        if (idx >= s->target) break;
        /* 写入序号到 buf 头部（用于检验正确性时可扩展） */
        memcpy(buf, &idx, sizeof(idx));
        if (s->impl == IMPL_DPDK)
            dpdk_push_wait((dpdk_ring_t *)s->ring, buf);
        else
            vy_push_wait((vy_ring_t *)s->ring, buf);
    }
    return NULL;
}

static void *consumer_fn(void *arg) {
    bench_state_t *s = arg;
    char buf[64];
    for (;;) {
        long idx = atomic_fetch_add_explicit(&s->recvd, 1, memory_order_relaxed);
        if (idx >= s->target) break;
        if (s->impl == IMPL_DPDK)
            dpdk_pop_wait((dpdk_ring_t *)s->ring, buf);
        else
            vy_pop_wait((vy_ring_t *)s->ring, buf);
    }
    return NULL;
}

/* 返回吞吐量 Mops/s */
static double run_bench(impl_t impl, void *ring, size_t elem_size,
                        int nprод, int ncons, long total) {
    bench_state_t s = {
        .impl      = impl,
        .ring      = ring,
        .elem_size = elem_size,
        .target    = total,
    };
    atomic_init(&s.sent,  0);
    atomic_init(&s.recvd, 0);

    pthread_t *pthr = malloc(sizeof(pthread_t) * (size_t)(nprод + ncons));

    int64_t t0 = now_ns();
    for (int i = 0; i < nprод; i++)
        pthread_create(&pthr[i], NULL, producer_fn, &s);
    for (int i = 0; i < ncons; i++)
        pthread_create(&pthr[nprод + i], NULL, consumer_fn, &s);
    for (int i = 0; i < nprод + ncons; i++)
        pthread_join(pthr[i], NULL);
    int64_t elapsed = now_ns() - t0;

    free(pthr);
    return (double)total / ((double)elapsed / 1e9) / 1e6;
}

/* ------------------------------------------------------------------ */
/* main                                                                 */
/* ------------------------------------------------------------------ */

int main(void) {
    static const int  thread_counts[] = { 1, 2, 4, 8 };
    static const size_t elem_sizes[]  = { 8, 64 };
    static const char *ec_names[]     = { "8B ", "64B" };
    const int ntc = (int)(sizeof(thread_counts) / sizeof(thread_counts[0]));
    const int nes = (int)(sizeof(elem_sizes)    / sizeof(elem_sizes[0]));

    printf("CPU: rte_ring (DPDK 4-游标) vs Vyukov per-slot MPMC\n");
    printf("环容量: %d  总操作数: %ldM/测试  每次操作=push+pop\n\n",
           RING_CAP, TOTAL_OPS / 1000000L);

    printf("%-14s  %-6s  %12s  %12s  %8s\n",
           "场景", "elem", "DPDK Mops/s", "Vyukov Mops/s", "Vyukov/DPDK");
    printf("%-14s  %-6s  %12s  %12s  %8s\n",
           "--------------", "------",
           "------------", "-------------", "----------");

    for (int ei = 0; ei < nes; ei++) {
        size_t esz = elem_sizes[ei];
        for (int ti = 0; ti < ntc; ti++) {
            int np = thread_counts[ti];

            /* 初始化两种 ring */
            dpdk_ring_t dr; vy_ring_t vr;
            if (!dpdk_init(&dr, RING_CAP, esz) || !vy_init(&vr, RING_CAP, esz)) {
                fprintf(stderr, "ring init failed\n"); return 1;
            }

            /* warmup（用 DPDK ring，两种都需要预热 CPU 缓存）*/
            bench_state_t ws = { .impl=IMPL_DPDK, .ring=&dr, .elem_size=esz,
                                  .target=WARMUP_OPS };
            atomic_init(&ws.sent, 0); atomic_init(&ws.recvd, 0);
            {
                pthread_t *pt = malloc(sizeof(pthread_t) * (size_t)(np * 2));
                for (int i = 0; i < np; i++) pthread_create(&pt[i], NULL, producer_fn, &ws);
                for (int i = 0; i < np; i++) pthread_create(&pt[np+i], NULL, consumer_fn, &ws);
                for (int i = 0; i < np*2; i++) pthread_join(pt[i], NULL);
                free(pt);
            }

            double dpdk_mops = run_bench(IMPL_DPDK,   &dr, esz, np, np, TOTAL_OPS);
            double vy_mops   = run_bench(IMPL_VYUKOV, &vr, esz, np, np, TOTAL_OPS);
            double ratio     = vy_mops / dpdk_mops;

            char label[16];
            snprintf(label, sizeof(label), "%dP+%dC", np, np);
            printf("%-14s  %-6s  %12.2f  %13.2f  %9.2fx\n",
                   label, ec_names[ei], dpdk_mops, vy_mops, ratio);

            dpdk_destroy(&dr);
            vy_destroy(&vr);
        }
        if (ei < nes - 1) printf("\n");
    }

    printf("\n说明:\n");
    printf("  DPDK Phase 3 自旋：当 N 个生产者并发预约后，第 k 个生产者必须等前 k-1 个\n");
    printf("  提交完才能推进 prod.tail（串行 convoy）。\n");
    printf("  Vyukov per-slot：每个生产者独立写槽后直接 store_release 本槽 seq，无等待。\n");
    printf("  元素越大（Phase 2 memcpy 越长），convoy 窗口越宽，DPDK 劣势越明显。\n");
    return 0;
}
