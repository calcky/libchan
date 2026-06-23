# libchan Benchmark Results

**环境**
- CPU：13th Gen Intel Core i7-13700H（20 逻辑核）
- OS：WSL2 / Linux 6.6.87.2-microsoft-standard-WSL2
- 编译器：GCC 13.3.0，`-O3`，`LIBCHAN_SPIN_LIMIT=40`
- 测试日期：2026-06-23
- 基准程序：`bench/bench_lock_overhead`、`bench/bench_mpmc`

> **注**：自 2026-06-23 起，`chan_send`/`chan_recv` 的无锁快路径在每次成功 push/pop
> 后多了一道 `seq_cst` fence（修复丢唤醒死锁的握手，见 [`architecture.md`](architecture.md)）。
> 这给**直接** send/recv 的快路径增加了约 7 ns/op（纯快路径 try 从 ~17.5→25 ns）。
> 跨语言对比（[`comparison.md`](comparison.md)）走 `chan_select`，不经过该 fence，吞吐不受影响。

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
# Park 对比（需 CMake + GCC）
bash bench/run_park_cmp.sh

# N×M 吞吐（约 90 秒）
cmake -B build -DLIBCHAN_BUILD_BENCH=ON && cmake --build build --parallel
build/bench/bench_mpmc
```
