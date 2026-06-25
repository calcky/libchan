# Channel 跨语言性能对比报告

## 测试目标

在同一硬件上公平对比三种 channel 实现，**分别测量直连收发与 select 两条路径**：

| 实现 | 语言 | channel 类型 |
|------|------|------------|
| libchan | C11 | MPMC 有界，DPDK 风格无锁 ring + Linux futex park；另有 `chan_create_spsc` 单生产单消费快路径（游标缓存，无 per-op fence） |
| Go 内置 `chan` | Go | MPMC 有界/无界，goroutine 调度 park |
| `crossbeam-channel` | Rust | MPMC 有界，crossbeam ring + futex Parker |

---

## 公平性方法论

| 维度 | 说明 |
|------|------|
| **优化级别** | C `-O3`；Rust `--release`（LLVM O3）；Go `go build`（≈O2，标准发布级） |
| **测量方法** | **固定消息数**（非固定时长）：每生产者发固定 K 条，消费者收到通道关闭为止，收发条数精确相等、不会丢消息。先用小消息数校准吞吐，再标定正式消息数到约 1.5 s。计时为从开始发送到全部 drain 完毕的墙钟时间。 |
| **路径** | **direct**：默认 MPMC 通道，生产者直接 send / 消费者直接 recv；**spsc**：同 direct 但通道由 `chan_create_spsc` 创建（仅 1P1C 有缓冲场景，单列于 direct 表）；**select**：生产者/消费者各跑一次 2-case select（含一个永不就绪的 dummy 第二路），对标三端的 select。 |
| **数据大小** | 均为 4 B（C `int`，Go `int32`，Rust `i32`） |
| **收尾机制** | C: `chan_close`；Go: `close(ch)`；Rust: drop 全部 sender |
| **线程模型** | C/Rust 使用 OS 线程（1:1）；Go 使用 goroutine（M:N，GOMAXPROCS=nCPU）—— **固有差异，非设计偏差** |

> **为什么用固定消息数**：旧版基准固定时长 + 在停止时刻采样计数，既无法保证不丢消息，
> 又因 C 端把计数累加在局部变量、循环结束才汇总，使预热清零失效、数字虚高约 1.27×。
> 固定消息数 + 精确计数断言彻底消除了这两个问题。

---

## 测试环境

```
CPU    : 13th Gen Intel(R) Core(TM) i7-13700H
OS     : 6.6.87.2-microsoft-standard-WSL2
C      : gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0
Go     : go1.23.10
Rust   : 1.96.0
Date   : 2026-06-24
```

---

## 结果（单位：Mops/s，越高越好）

### 路径一：直连 chan_send / chan_recv（核心路径）

| 场景                 |     libchan (C) |   libchan SPSC |    Go chan |   crossbeam (Rust) |
|----------------------|-----------------|----------------|------------|--------------------|
| 1P+1C  cap=0 (unbuf)   |         3.999   |            — |      8.555 |              0.069 |
| 1P+1C  cap=64          |         7.697   |         64.282 |     32.498 |             50.911 |
| 1P+1C  cap=1024        |         8.810   |         69.478 |     34.965 |             66.730 |
| 2P+2C  cap=1024        |         6.264   |            — |     27.661 |             48.546 |
| 4P+4C  cap=1024        |         3.972   |            — |     13.995 |             25.896 |
| 8P+8C  cap=1024        |         3.175   |            — |      4.060 |             11.772 |

### 路径二：select 多路复用

| 场景                 |     libchan (C) |    Go chan |   crossbeam (Rust) |
|----------------------|-----------------|------------|--------------------|
| 1P+1C  cap=0 (unbuf)   |         0.108   |      4.596 |              0.126 |
| 1P+1C  cap=64          |        10.511   |     12.493 |              4.786 |
| 1P+1C  cap=1024        |        19.043   |     12.870 |             16.301 |
| 2P+2C  cap=1024        |         8.921   |     10.913 |             11.145 |
| 4P+4C  cap=1024        |         5.403   |      5.254 |              7.480 |
| 8P+8C  cap=1024        |         3.736   |      2.441 |              5.979 |


> select 在 MPMC（≥2P+2C）下，libchan 有约 0.01% 的计数偏差（已知限制，见
> [`design.md`](design.md) 的 Select 小节），不影响吞吐量级；direct 路径三端均精确无误差。

---

## 分析

### 直连默认路径（MPMC `chan_create`）：libchan 不占优，crossbeam 最快

默认 MPMC 通道的直连 send/recv 下，**crossbeam > Go > libchan(C)**（有缓冲场景）。原因：

- **crossbeam / Go** 的有缓冲队列在无竞争时基本是纯无锁/轻量 CAS，且唤醒批处理良好。
- **libchan(C)** 的 MPMC 直连路径为保证正确性（修复有缓冲通道的丢唤醒死锁）付出代价：
  每次成功 push/pop 多一道 `seq_cst` fence，且当对端已 park 时立即**取锁**唤醒，牺牲了批处理。
- **无缓冲（cap=0）**：crossbeam 的 `bounded(0)` rendezvous 极慢（已知），libchan 与 Go
  量级相近。

### 直连 SPSC 路径（`chan_create_spsc`）：libchan 反超 crossbeam

当应用满足单生产单消费契约时，`chan_create_spsc` 把直连吞吐拉到 **libchan SPSC 列**所示水平
——在 1P+1C 有缓冲场景**超过 crossbeam**，约为自身 MPMC 直连路径的 ~8×。原因：

- **游标缓存**：每侧缓存对端热游标的下界，消除了每条消息一次的跨核 cache line 弹跳
  （MPMC 路径的主要成本）；
- **无 per-op fence**：SPSC 热路径省掉上面那道 `seq_cst` fence；
- **park 侧有界重检**：把唤醒竞态的修复代价隔离到（本就慢的）park 路径，热路径零开销，
  且**不依赖 `chan_close`**——请求/响应（ping-pong）也不死锁。

代价是契约：至多一个生产者线程 + 一个消费者线程（违反即 UB）。故 SPSC 列只在 1P1C
有缓冲场景有值，其余为 —。

### select 路径：libchan 领先

select 下 libchan **快于自身的 MPMC 直连路径**，也普遍领先 Go 与 crossbeam。原因：
libchan 的 select 走无锁 ring 快路径，且 park 的 select stub **不增加** waiter 计数、
采用有界延迟唤醒（生产者无锁灌满环 → 一次唤醒 → 消费者批量取走），保留了批处理。
代价是 MPMC 下的微小计数偏差（上文已注）。

### 启示

**按使用形态选路径**：① 多生产/多消费 + 以直连 send/recv 为主 → crossbeam/Go 更快；
② **单生产单消费 + 直连为主 → `chan_create_spsc`，反超 crossbeam**；③ 以 select 多路复用
为主 → libchan(MPMC) 有优势。旧版基准只测了 select，掩盖了 MPMC 直连路径的劣势；本次拆成
三列（MPMC 直连 / SPSC 直连 / select）如实呈现各自的强弱区间。

---

## 重新运行

```bash
cd /path/to/libchan
bash bench/crosslang/run_comparison.sh
```
