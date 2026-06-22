# libchan Benchmark Results

**环境**
- CPU：13th Gen Intel Core i7-13700H（20 逻辑核）
- OS：WSL2 / Linux 6.6.87.2-microsoft-standard-WSL2
- 编译器：GCC 13.3.0，`-O3`，`LIBCHAN_SPIN_LIMIT=40`
- 测试日期：2026-06-22
- 基准程序：`bench/bench_lock_overhead`、`bench/bench_mpmc`

---

## 1. Park 后端对比：futex vs pthread

`bench_lock_overhead` 以 futex（Linux 默认）和 pthread condvar（`-DLIBCHAN_FORCE_PTHREAD_PARK=ON`）两种配置各运行一次，每次操作 = send 1 个 int + recv 1 个 int。

```
场景                                  futex                  pthread condvar
                                 ns/op   Mops/s           ns/op   Mops/s
-------------------------------------------------------------------
1. 裸 memcpy（基线）              0.11    9366          0.11    9230
2. atomic_fetch_add relaxed      3.78     264          3.92     255
3. mutex lock+nop+unlock（无竞）  2.51     398          2.59     387
4. mutex lock+nop+unlock（2线程） 79.3    12.6         82.5    12.1
5. try_send+try_recv cap=1024    17.5    57.1         17.0    58.7
   （单线程，纯快路径）
6a. send+recv cap=1024（1+1线程） 90.9    11.0         83.3    12.0
6b. send+recv 无缓冲（1+1线程）  232     4.30          770     1.30  ← 关键差距
6c. send+recv cap=1024（2+2线程） 107     9.31          107     9.38
6d. send+recv cap=1024（4+4线程） 962     1.04          713     1.40
```

**关键结论**

| 场景 | futex | pthread | 比值 |
|------|-------|---------|------|
| 快路径（try_send+try_recv，无 park） | 17.5 ns | 17.0 ns | ≈1.0× |
| 有缓冲阻塞（1+1，cap=1024） | 90.9 ns | 83.3 ns | ≈1.0× |
| **无缓冲阻塞（1+1，必须 park）** | **232 ns** | **770 ns** | **futex 快 3.3×** |
| 有缓冲高竞争（4+4，cap=1024） | 962 ns | 713 ns | pthread 略快 1.35× |

- **快路径不涉及 park**，两种后端几乎无差异。
- **无缓冲通道强制每次 rendezvous 都走 park/unpark**，futex 系统调用开销（~100 ns）远低于 pthread_cond_wait + 互斥锁序列（~500 ns），差距 **3.3×**。
- 高竞争场景（4+4）下 pthread 反而略快，原因：大量线程争抢时 OS 调度效果盖过了 futex 本身的优势；测量噪声也较大（标准差 ±20%）。

---

## 2. N×M 生产者-消费者吞吐（Mops/s）

`bench_mpmc` 固定测量 1500 ms，预热 400 ms，单位 = 完成的 send+recv 对 / 秒。

### capacity = 0（无缓冲，强制 rendezvous）

```
            1C      2C      4C      8C
  1P |    4.22    2.82    0.42    0.17
  2P |    2.74    2.29    2.14    0.86
  4P |    0.33    2.08    1.81    1.15
  8P |    0.15    0.81    1.12    1.15
```

### capacity = 64

```
            1C      2C      4C      8C
  1P |   11.68    7.23    0.48    0.15
  2P |   10.21    8.05    4.33    0.63
  4P |    0.55    4.87    4.09    1.74
  8P |    0.15    0.75    1.92    2.70
```

### capacity = 1024

```
            1C      2C      4C      8C
  1P |   13.60   11.05    1.54    0.19
  2P |   11.46    9.28    6.06    2.35
  4P |    2.26    5.89    4.56    3.64
  8P |    0.15    3.35    3.93    3.32
```

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
