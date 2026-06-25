# libchan Benchmark Results

**环境**
- CPU：13th Gen Intel Core i7-13700H（20 逻辑核）
- OS：WSL2 / Linux 6.6.87.2-microsoft-standard-WSL2
- 编译器：GCC 13.3.0，`-O3`，`LIBCHAN_SPIN_LIMIT=40`
- 测试日期：2026-06-24
- 基准程序：`bench/bench_showcase`（性能阶梯）、`bench/bench_lock_overhead`、`bench/bench_mpmc`

**测量方法**
- 每个数据点跑 7 次，报**中位数**与 **min**（min ≈ 无干扰下界）。
- `taskset` 钉核运行（`bench/run_showcase.sh`），减少线程迁移抖动。
- 同时报 **ns/op** 与 **Mops/s**。
- ⚠️ **WSL2 caveat**：跑在 Windows 调度器之上，含 park 的场景（B 档）抖动明显，数字仅供
  量级参考；A 档（无锁快路径，不 park）较稳定。严肃测量请在原生 Linux + isolcpus 上跑。

---

## 0. 性能阶梯（展示主角）

`bench/bench_showcase` 把"从硬件极限到完整阻塞路径"串成一条阶梯，让每一层的开销来源
一目了然。**关键洞察：libchan 的无锁快路径贴近硬件极限；慢的地方是 OS 的 park/调度，
不是库本身。**

### A 档 · 几乎不 park（测 "channel 本身有多快"）

| # | 场景 | 中位 ns | min ns | Mops/s | 这一层加了什么 |
|---|------|--------:|-------:|-------:|---------------|
| 1 | 裸 memcpy | 0.11 | 0.10 | 9487 | 硬件极限 |
| 2 | atomic_fetch_add | 3.82 | 3.77 | 261 | 无锁原子原语下界 |
| 3 | 无锁 ring 纯队列 | 14.6 | 14.5 | 68 | 队列数据结构(CAS+内存序+memcpy) |
| 4 | chan try_send/recv | 25.0 | 24.7 | 40 | + channel 语义(closed 检查/waiter 门槛)，无等待 |
| 5 | chan **MPMC** 跨核稳态 | 121.4 | 119.4 | 8.2 | + **跨核 cache 一致性**:每条消息读对端反复改写的热游标 ← 物理墙 |
| 6 | chan **SPSC** 跨核稳态 | 23.9 | 22.1 | 42 | + **游标缓存**:消除弹跳,破墙(同 busy-poll 测法,5×) |

> **怎么读这张表**：
> - 1→4 全在**单核**完成，无跨核流量：channel 语义只比纯队列贵 ~10 ns，比裸原子贵 ~20 ns，
>   说明 libchan 的快路径几乎没有"软件税"。
> - 4→5 的跳变（24→121 ns）**不是锁、不是 park**，而是**跨核 cache 一致性**：MPMC 下两个线程在
>   不同核上交替读写同一组 ring 游标，每条消息都触发一次缓存行跨核迁移（~100 ns）。这是
>   **朴素**跨核队列的物理墙(~8 Mops)。
> - 5→6：`chan_create_spsc` 用**游标缓存**(每侧缓存对端游标的下界,仅在缓存显示满/空时才回读
>   真实热游标)消除这次弹跳——同一 busy-poll 测法下 8→42 Mops（5×），几乎回到单核 try 的量级。
>   单属主缓存只在 SPSC 契约(1 生产者 + 1 消费者)下安全，故为选择性加入。
> - SPSC **阻塞流式**(B 档 #7)比这里的 busy-poll 稳态还高(73 vs 42)：流式让生产者跑在前面、
>   把游标读取摊薄到一批，而 busy-poll 锁步加剧缓存争用。

### B 档 · 阻塞延迟（含 park + OS 调度，仅量级参考）

| # | 场景 | 中位 ns | min ns | Mops/s | 主导开销 |
|---|------|--------:|-------:|-------:|---------|
| 7 | chan SPSC 阻塞 cap=1024 | 13.8 | 13.2 | 72.8 | 流式快路径 + 偶发 park（游标缓存生效）|
| 8 | chan 无缓冲 rendezvous | 285.3 | 259.1 | 3.5 | 每 op 必 park(futex 往返) |
| 9 | chan MPMC 4P+4C cap=1024 | 208.1 | 162.0 | 4.8 | 竞争 + park + 调度 |

> B 档每次操作可能进入内核 park/wake，测的是 **channel 作为同步原语的端到端延迟**，
> 不是队列本身。这一档主要反映 OS 的 futex + 调度效率（见 §2 futex vs pthread），
> 跨语言/跨实现比较时尤其要注意它**不可与 A 档同表论高下**。

### 参考：GitHub Actions 共享 runner（CI 实测）

CI 每次 push 在 `ubuntu-latest`（~4 逻辑核、虚拟化）上跑同一套 `bench_showcase`。
绝对数字明显低于上面的开发机（核更少 + 虚拟化调度），**仅供趋势参考**；但阶梯形状与
"SPSC 破墙、阻塞流式优于 busy-poll" 的结论一致。

| # | 场景 | 中位 ns | min ns | Mops/s |
|---|------|--------:|-------:|-------:|
| 1 | 裸 memcpy | 0.16 | 0.15 | 6315.91 |
| 2 | atomic_fetch_add | 2.21 | 2.10 | 453.08 |
| 3 | 无锁 ring 纯队列 | 11.53 | 10.89 | 86.71 |
| 4 | chan try_send/recv | 19.68 | 19.61 | 50.80 |
| 5 | chan **MPMC** 跨核稳态（缓存一致性墙） | 176.88 | 171.40 | 5.65 |
| 6 | chan **SPSC** 跨核稳态（游标缓存破墙） | 63.43 | 63.04 | 15.77 |
| 7 | chan SPSC 阻塞 cap=1024 | 27.09 | 21.34 | 36.92 |
| 8 | chan 无缓冲 rendezvous | 701.29 | 696.18 | 1.43 |
| 9 | chan MPMC 4P+4C cap=1024 | 1002.05 | 680.79 | 1.00 |

> SPSC 跨核(15.77)≈ MPMC 跨核(5.65)的 **2.8×**（开发机 5×；4 核争用把"墙"压扁但仍在）；
> SPSC 阻塞流式(36.9 Mops)> busy-poll 稳态(15.8)，与开发机同理（生产者跑在前摊薄游标读取）。
> 实时趋势见 github-action-benchmark 维护的 `gh-pages` 仪表盘。

---

## 1. Park 后端对比：futex vs pthread

`bench_lock_overhead` 以 futex（Linux 默认）和 pthread condvar（`-DLIBCHAN_FORCE_PTHREAD_PARK=ON`）两种配置各运行一次，每次操作 = send 1 个 int + recv 1 个 int。

```
场景                                  futex                  pthread condvar
                                 ns/op   Mops/s           ns/op   Mops/s
-------------------------------------------------------------------
1. 裸 memcpy（基线）              0.10    9579          0.11    9443
2. atomic_fetch_add relaxed      3.86     259          3.86     259
3. mutex lock+nop+unlock（无竞）  2.64     379          2.57     389
4. mutex lock+nop+unlock（2线程） 83.2    12.0         92.2    10.9
5. try_send+try_recv cap=1024    25.0    40.0         26.1    38.3
   （单线程，纯快路径 + fence）
6a. send+recv cap=1024（1+1线程） 136     7.34          134     7.48
6b. send+recv 无缓冲（1+1线程）  215     4.65          774     1.29  ← 关键差距
6c. send+recv cap=1024（2+2线程） 205     4.88          133     7.54
6d. send+recv cap=1024（4+4线程）2102     0.48         2483     0.40
```

**关键结论**

| 场景 | futex | pthread | 比值 |
|------|-------|---------|------|
| 快路径（try_send+try_recv，无 park） | 25.0 ns | 26.1 ns | ≈1.0× |
| 有缓冲阻塞（1+1，cap=1024） | 136 ns | 134 ns | ≈1.0× |
| **无缓冲阻塞（1+1，必须 park）** | **215 ns** | **774 ns** | **futex 快 3.6×** |
| 有缓冲高竞争（4+4，cap=1024） | 2102 ns | 2483 ns | 量级相当（噪声大） |

- **快路径不涉及 park**，两种后端几乎无差异；fence 开销对两者一致。
- **无缓冲通道强制每次 rendezvous 都走 park/unpark**，futex 系统调用开销（~100 ns）远低于 pthread_cond_wait + 互斥锁序列（~500 ns），差距 **3.6×**。
- 高竞争场景（2+2、4+4）测量噪声较大（标准差 ±20% 以上），futex/pthread 互有胜负，不宜过度解读。

---

## 2. N×M 生产者-消费者吞吐（Mops/s）

`bench_mpmc` 固定测量 1500 ms，预热 400 ms，单位 = 完成的 send+recv 对 / 秒。

### capacity = 0（无缓冲，强制 rendezvous）

```
            1C      2C      4C      8C
  1P |    3.84    2.64    0.35    0.17
  2P |    2.76    2.34    1.97    0.76
  4P |    0.50    1.59    1.41    1.15
  8P |    0.15    0.83    1.14    1.15
```

### capacity = 64

```
            1C      2C      4C      8C
  1P |    4.87    4.38    0.44    0.12
  2P |    6.78    6.86    3.71    0.78
  4P |    0.42    4.63    3.71    1.80
  8P |    0.14    0.73    2.03    2.32
```

### capacity = 1024

```
            1C      2C      4C      8C
  1P |    7.23    6.44    1.23    0.15
  2P |   10.00    7.70    4.92    1.61
  4P |    1.66    5.60    4.43    3.47
  8P |    0.14    2.80    3.62    3.19
```

> 相比 2026-06-22 的数据，有缓冲场景（cap=64/1024）的**直接** send/recv 吞吐有所下降
> （如 1P+1C cap=1024 从 ~13.6→7.2 Mops/s）。原因是丢唤醒修复后，当对端已 park 时，
> 快路径会取锁把数据交付给它并唤醒（此前会错过唤醒，是个 bug）。正确唤醒 park 的等待者
> 本身就有锁开销。需要极限直连吞吐时可改用 `chan_select`（不经此路径）。

**规律总结**

1. **对角线（nP+nC）吞吐最高**：生产消费平衡，慢路径（park）触发最少。
2. **严重不对称时吞吐崩溃**（左下角 / 右上角接近 0）：少数一侧成为瓶颈，多余的线程全部 park 等待，park/unpark 开销主导。
   - 8P+1C：0.15 Mops/s — 8 个生产者争一个消费者，几乎全部时间在 park/wake 循环
   - 1P+8C：同理，0.15–0.19 Mops/s
3. **大缓冲（cap=1024）缓解不对称**：吸收突发使不对称时的 park 触发减少，2P+1C 从 10.2→11.5 Mops/s，4P+2C 从 4.87→5.89。
4. **无缓冲（cap=0）整体较低**：每次 send/recv 均需等待对端，无法并发流水。
5. **WSL2 高线程数衰减明显**：4P+4C cap=1024（4.56）远低于理论线性扩展（1P+1C×4 = 54.4），主因是单 channel mutex 序列化 + WSL2 线程调度开销。

---

## 3. 重新运行

```bash
# 性能阶梯（A/B 分档，钉核 + 中位数）
bash bench/run_showcase.sh

# Park 对比（futex vs pthread）
bash bench/run_park_cmp.sh

# N×M 吞吐
cmake -B build -DLIBCHAN_BUILD_BENCH=ON && cmake --build build --parallel
build/bench/bench_mpmc
```

---

## 4. 跨语言对照

与 Go 内置 `chan`、Rust `crossbeam-channel` 的对比（direct / spsc / select 三条路径，
固定消息数 + 精确计数）见 [`comparison.md`](comparison.md)。一句话结论：**按使用形态选路径**——
单生产单消费直连用 `chan_create_spsc`，吞吐**反超 crossbeam**（与 §0 阶梯 #6 一致）；多生产/
多消费直连下 crossbeam/Go 更快（libchan 默认 MPMC 为正确性付出 fence + 即时唤醒代价）；
select 多路复用 libchan 有优势。
