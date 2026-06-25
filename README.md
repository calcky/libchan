# libchan

> 纯 C11 实现的高性能 channel 库 —— 把 Go / Rust 的 channel 语义带到 C。

`libchan` 提供无缓冲同步握手、有缓冲异步队列、多生产者多消费者（MPMC）、
close 通知，以及类 Go `select` 的多路复用。核心是一条**无锁快路径**
（DPDK 风格无锁 ring + Linux futex park）。其 `select` 路径在大缓冲下领先 Go 与 crossbeam；
默认 MPMC 直连 send/recv 慢于二者，但选择性加入的 **`chan_create_spsc`（单生产单消费）**
直连吞吐**反超 crossbeam**（见[性能](#性能)中的诚实对比）。

---

## 特性

- **三种 channel 语义** —— `cap == 0` 无缓冲同步 rendezvous；`cap > 0` 有界 FIFO 缓冲。
- **MPMC** —— 任意数量生产者/消费者并发安全。
- **无锁快路径** —— 无竞争时 `send`/`recv`/`select` 完全绕过 mutex，直接走原子 ring。
- **类 Go select** —— `chan_select` 多路复用，多个 case 就绪时均匀随机选择。
- **完整阻塞语义** —— 阻塞 / 非阻塞（`try_`）/ 超时（`_timeout`）三套接口。
- **close 广播** —— 唤醒所有阻塞方，有缓冲通道 close 后仍可排空（Go 语义）。
- **引用计数** —— `chan_retain` / `chan_destroy` 安全管理生命周期。
- **可移植 park 后端** —— Linux 用 futex，其它平台回退 pthread mutex+cond。
- **纯 C11 + pthreads** —— 无第三方依赖。

---

## 快速上手

```c
#include "libchan.h"
#include <pthread.h>
#include <stdio.h>

static void *producer(void *arg) {
    chan_t *ch = arg;
    for (int i = 0; i < 5; i++)
        chan_send(ch, &i);
    chan_close(ch);          // 通知消费者数据发完
    return NULL;
}

int main(void) {
    chan_t *ch = chan_create(sizeof(int), 2);   // 有缓冲，cap=2

    pthread_t t;
    pthread_create(&t, NULL, producer, ch);

    int v;
    while (chan_recv(ch, &v) == CHAN_OK)         // close+排空后返回 CHAN_ERR_CLOSED
        printf("recv %d\n", v);

    pthread_join(t, NULL);
    chan_destroy(ch);
    return 0;
}
```

`select` 多路复用：

```c
int a, b;
chan_select_case_t cases[2] = {
    { .ch = ch_a, .op = CHAN_OP_RECV, .data = &a },
    { .ch = ch_b, .op = CHAN_OP_RECV, .data = &b },
};
int idx = chan_select(cases, 2);                 // 阻塞直到某个 case 就绪
if (cases[idx].result == CHAN_OK)
    printf("case %d ready\n", idx);
```

更多示例见 [`examples/`](examples/)。

---

## 构建

依赖：CMake ≥ 3.16、C11 编译器（GCC/Clang）、pthreads。

```bash
cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
```

或用顶层 `Makefile`：

```bash
make          # 构建库（静态 + 动态）
make test     # 构建并运行所有单元测试
make asan     # AddressSanitizer + UBSan
make tsan     # ThreadSanitizer（需原生 Linux）
make bench    # 构建吞吐基准
```

构建产物：`build/libchan.so`、`build/libchan.a`，头文件 `include/libchan.h`。

### 构建选项

| 选项 | 默认 | 说明 |
|------|------|------|
| `LIBCHAN_BUILD_SHARED` | `ON` | 动态库 `libchan.so` |
| `LIBCHAN_BUILD_STATIC` | `ON` | 静态库 `libchan.a` |
| `LIBCHAN_BUILD_TESTS` | `ON` | 单元测试 |
| `LIBCHAN_BUILD_BENCH` | `OFF` | 吞吐基准 |
| `LIBCHAN_BUILD_EXAMPLES` | `ON` | 示例程序 |
| `LIBCHAN_FORCE_PTHREAD_PARK` | `OFF` | 强制 pthread park 后端（禁用 futex） |
| `LIBCHAN_SPIN_LIMIT` | `40` | 进入内核 park 前的自旋次数 |

---

## API 概览

| 类别 | 函数 |
|------|------|
| **生命周期** | `chan_create` · `chan_destroy` · `chan_retain` |
| **发送** | `chan_send` · `chan_try_send` · `chan_send_timeout` |
| **接收** | `chan_recv` · `chan_try_recv` · `chan_recv_timeout` |
| **关闭** | `chan_close` · `chan_is_closed` |
| **多路复用** | `chan_select` · `chan_select_try` · `chan_select_timeout` |
| **内省** | `chan_len` · `chan_cap` |
| **诊断** | `chan_strerror` |

返回码 `chan_err_t`：`CHAN_OK` / `CHAN_ERR_CLOSED` / `CHAN_ERR_TIMEOUT` /
`CHAN_ERR_WOULDBLOCK` / `CHAN_ERR_INVALID` / `CHAN_ERR_NOMEM`。

完整签名与语义见 [`doc/api_reference.md`](doc/api_reference.md)。

---

## 设计要点

- **无锁 ring（`src/ring_lf.c`）** —— DPDK rte_ring 风格的无锁 MPMC 队列：
  CAS 预约 `head` → 写槽位 → 提交 `tail`，容量向上取整到 2 的幂。
- **快路径** —— `send`/`recv` 在 `send_waiter_cnt == 0 && recv_waiter_cnt == 0`
  时直接 `ring_lf_push/pop`，零锁、零系统调用。
- **慢路径** —— 仅在需要阻塞时取 mutex、注册 waiter、park。
- **park 后端（`src/park.c`）** —— Linux 走 futex（`FUTEX_WAIT/WAKE`），
  先自旋 `LIBCHAN_SPIN_LIMIT` 次再进内核；其它平台回退 pthread cond。
- **select（`src/select.c`）** —— 先尝试无锁快路径执行就绪 case；
  全不就绪时按地址序锁定所有通道、注册共享 stub、park，唤醒后清理。

架构细节见 [`doc/design.md`](doc/design.md)。

---

## 性能

### 性能阶梯（libchan 自身,13th Gen i7-13700H / WSL2,中位数）

从硬件极限到完整阻塞路径,逐层看清开销来源（`bench/run_showcase.sh`）：

| 层次 | ns/op | Mops/s | 说明 |
|------|------:|-------:|------|
| 裸 memcpy | 0.11 | 9487 | 硬件极限 |
| atomic_fetch_add | 3.82 | 261 | 无锁原子下界 |
| 无锁 ring 纯队列 | 14.6 | 68 | 队列数据结构本身 |
| chan try_send/recv | 25.0 | 40 | + channel 语义,单核无等待 |
| chan **MPMC** 跨核稳态 | 121 | 8.2 | + 跨核 cache 一致性:**每条消息弹跳游标** ← 墙 |
| chan **SPSC** 跨核稳态 | 23.9 | 42 | + 游标缓存:**破除一致性墙**(同测法,5×) |

**要点**:channel 语义只比纯队列贵 ~10 ns(快路径几乎无软件税)；24→121 的跳变是
**跨核 cache 一致性**——MPMC 每条消息都要读对端被对端反复改写的热游标,撞上物理墙(8 Mops)；
`chan_create_spsc` 用**游标缓存**消除这次弹跳,同一 busy-poll 测法下 8→42（5×）。
SPSC 阻塞流式更高(下表 73 Mops:生产者跑在前面摊薄游标读取,优于锁步 busy-poll)。
完整 A/B 分档表与方法论见 [`doc/benchmarks.md`](doc/benchmarks.md)。

### 跨语言对比（单位 Mops/s,越高越好,固定消息数 + 精确计数,i7-13700H/WSL2）

**直连 chan_send / chan_recv（核心路径）：**

| 场景 | libchan (MPMC) | **libchan SPSC** | Go chan | crossbeam (Rust) |
|------|--------:|--------:|--------:|-----------------:|
| 1P+1C cap=64   |  7.70 | **64.28** | 32.50 | 50.91 |
| 1P+1C cap=1024 |  8.81 | **69.48** | 34.97 | 66.73 |
| 2P+2C cap=1024 |  6.26 | — | 27.66 | **48.55** |
| 4P+4C cap=1024 |  3.97 | — | 14.00 | **25.90** |
| 8P+8C cap=1024 |  3.18 | — |  4.06 | **11.77** |

**select 多路复用：**

| 场景 | libchan | Go chan | crossbeam (Rust) |
|------|--------:|--------:|-----------------:|
| 1P+1C cap=0 (unbuf) |  0.108 | **4.596** |  0.126 |
| 1P+1C cap=1024      | **19.043** | 12.870 | 16.301 |
| 4P+4C cap=1024      |  5.403 |  5.254 | **7.480** |
| 8P+8C cap=1024      |  3.736 |  2.441 | **5.979** |

**结论（诚实）**:按使用形态选路径——① **单生产单消费 + 直连为主 → `chan_create_spsc`,
直连吞吐反超 crossbeam**(64–69 vs 51–67,约为自身 MPMC 直连的 8×);② 多生产/多消费 +
直连为主 → crossbeam/Go 更快(libchan 默认 MPMC 直连为修复有缓冲丢唤醒死锁付出 fence +
即时唤醒代价);③ 以 select 多路复用为主 → libchan 有优势。SPSC 列仅 1P1C 有缓冲有值
(契约:至多一个生产者 + 一个消费者)。完整表、方法论与分析见
[`doc/comparison.md`](doc/comparison.md),基准代码见 [`bench/crosslang/`](bench/crosslang/)。

---

## 测试

```bash
make test          # 全部单元测试
make asan          # ASan + UBSan
make tsan          # ThreadSanitizer（原生 Linux）
```

覆盖：基础收发、阻塞/超时、close 语义、MPMC、`select`、高竞争压力测试
（8P+8C × 50k 消息，验证零丢失）。

---

## 项目结构

```
libchan/
├── include/libchan.h     # 公开 API
├── src/                  # 实现
│   ├── chan_core.c       #   生命周期 / close
│   ├── chan_send.c       #   发送（快路径 + 慢路径）
│   ├── chan_recv.c       #   接收
│   ├── select.c          #   chan_select 多路复用
│   ├── ring_lf.c         #   DPDK 风格无锁 MPMC ring
│   ├── waitq.c           #   waiter 等待队列
│   └── park.c            #   futex / pthread park 后端
├── tests/                # 单元 + 压力测试
├── examples/             # 用法示例
├── bench/                # 性能基准（含跨语言对比 crosslang/）
└── doc/                  # 文档
```

---

## 文档

| 文档 | 内容 |
|------|------|
| [getting_started.md](doc/getting_started.md) | 构建与上手 |
| [api_reference.md](doc/api_reference.md) | 完整 API 参考 |
| [architecture.md](doc/architecture.md) | 工作原理图解（分层 / 快慢路径 / rendezvous 时序） |
| [design.md](doc/design.md) | 内部架构与并发设计 |
| [benchmarks.md](doc/benchmarks.md) | 内部基准方法与数据 |
| [comparison.md](doc/comparison.md) | 与 Go / Rust 的跨语言对比 |
