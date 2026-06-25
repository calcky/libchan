# 跨语言 Channel 性能对比基准

在**同一台机器、同一组场景、等价的停止机制**下，对比三种 channel 实现的吞吐：

| 实现 | 语言 | channel 类型 |
|------|------|-------------|
| [libchan](libchan/bench.c) | C11 | MPMC 有界，DPDK 风格无锁 ring + Linux futex park |
| Go 内置 `chan` | Go | MPMC 有界/无缓冲，goroutine 调度 park |
| [`crossbeam-channel`](rust/src/main.rs) | Rust | MPMC 有界，crossbeam ring + futex Parker |

> 完整结果与分析见 [`doc/comparison.md`](../../doc/comparison.md)。

---

## 一键运行

```bash
# 从仓库根目录执行
bash bench/crosslang/run_comparison.sh
```

脚本会自动：

1. 用 CMake 构建 libchan 静态库（Release，`-O3`）
2. 编译 C 基准（静态链接 libchan）、Go 基准（`go build`）、Rust 基准（`cargo build --release`）
3. 依次运行三端，每场景 400 ms 预热 + 1500 ms 测量
4. 合并 CSV，打印对比表并写入 `doc/comparison.md`

总耗时约 6 场景 × 3 语言 × 1.9 s ≈ **35 s**。

### 依赖

| 工具 | 用途 |
|------|------|
| `cmake` + `gcc`（C11，支持 `-pthread`） | 构建 libchan 与 C 基准 |
| `go`（≥ 1.18） | Go 基准 |
| `cargo` / `rustc` | Rust 基准（首次会联网下载 `crossbeam-channel`） |

---

## 目录结构

```
bench/crosslang/
├── run_comparison.sh     # 编译 + 运行 + 生成报告
├── libchan/
│   └── bench.c           # C 基准（chan_select + stop 通道）
├── go/
│   ├── go.mod
│   └── main.go           # Go 基准（select + close(done)）
└── rust/
    ├── Cargo.toml
    └── src/main.rs       # Rust 基准（select! + drop(stop_tx)）
```

---

## 测试场景

元素统一为 4 字节整数（C `int` / Go `int32` / Rust `i32`）。

| 编号 | cap | P | C | 测试重点 |
|------|-----|---|---|---------|
| S1 | 0 (unbuffered) | 1 | 1 | rendezvous 开销，park/unpark 延迟 |
| S2 | 64 | 1 | 1 | 小缓冲 SPSC，偶发阻塞 |
| S3 | 1024 | 1 | 1 | 大缓冲 SPSC，无锁快路径极限 |
| S4 | 1024 | 2 | 2 | 对称低竞争 MPMC |
| S5 | 1024 | 4 | 4 | 对称中等竞争 MPMC |
| S6 | 1024 | 8 | 8 | 对称高竞争 MPMC（16 线程） |

---

## 公平性方法论

三端在以下维度严格对齐，消除 benchmark 偏向：

| 维度 | 三端一致做法 |
|------|------------|
| **热路径** | 每次迭代都执行一次 **2-case select**，同时监听 data 通道与 stop 通道 |
| **计数口径** | 只统计 **consumer 实际收到的消息数**（C 的 `min(sent,recv)` 恒等于 `recv`） |
| **时间窗口** | 400 ms 预热（清零计数）+ 1500 ms 测量 |
| **数据大小** | 均为 4 B |
| **停止机制** | 广播关闭 stop 通道：C `chan_close` / Go `close(done)` / Rust `drop(stop_tx)` |
| **优化级别** | C `-O3` / Rust `--release`(LLVM O3) / Go 标准发布构建 |

各端热路径形态：

```c
// C / libchan
chan_select({ send(data_ch, v), recv(stop_ch) });
```
```go
// Go
select { case ch <- v: ...; case <-done: return }
```
```rust
// Rust
select! { send(data_tx, v) -> _ => {...}, recv(stop_rx) -> _ => break }
```

### 两个固有差异（如实声明，非基准偏向）

1. **线程模型 M:N vs 1:1** —— Go 用 goroutine（用户态调度 park，~200 ns），
   C/Rust 用 OS 线程 + Linux futex（每次 rendezvous 一次内核往返 ~1–2 µs）。
   这是 S1 无缓冲场景 Go 领先的根本原因；crossbeam 同用 futex，S1 与 libchan
   同量级，佐证这是 OS 线程 park 的共同上限。

2. **libchan select 的语义松弛** —— 为避免 MPMC 锁串行化，libchan 的 select
   stub 在有缓冲通道上不增加 waiter 计数，代价是一个 park 的 select-sender 最多
   延迟"一个 ring 容量"的消息才被唤醒。此松弛在本基准中**反而略微拖慢 libchan**，
   不构成对其有利的不公平。

---

## 结果速览

> 13th Gen Intel i7-13700H / WSL2 / gcc 13.3 / go 1.23 / rustc 1.96，单位 **Mops/s（越高越好）**。
> 实际数字每次运行略有波动，以 `doc/comparison.md` 最新一次为准。

| 场景 | libchan (C) | Go chan | crossbeam (Rust) | 胜者 |
|------|------------:|--------:|-----------------:|------|
| S1  1P+1C cap=0    |  0.235 | **4.748** |  0.150 | Go |
| S2  1P+1C cap=64   | **14.436** | 12.548 |  5.227 | libchan |
| S3  1P+1C cap=1024 | **24.155** | 12.799 |  9.410 | libchan |
| S4  2P+2C cap=1024 | **12.885** | 10.734 |  9.873 | libchan |
| S5  4P+4C cap=1024 | **11.020** |  6.150 |  9.314 | libchan |
| S6  8P+8C cap=1024 |  **9.620** |  3.238 |  6.053 | libchan |

**结论**：6 个场景 libchan 赢 5 个。全部有缓冲场景（S2–S6）超过 Go 与 crossbeam，
得益于无锁 ring 快路径（S3 ≈ 40 ns/op）。唯一落后的 S1 是 OS 线程对
Go 协程在同步 rendezvous 上的固有劣势，非实现缺陷。
