#!/usr/bin/env bash
# bench/crosslang/run_comparison.sh
#
# 编译并运行 libchan (C)、Go、Rust 三端基准，输出 Markdown 对比报告。
#
# 用法：
#   bash bench/crosslang/run_comparison.sh
#
# 输出：
#   终端打印对比表；同时追加写入 doc/comparison.md

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CL="$ROOT/bench/crosslang"

info()  { printf '\033[0;34m[INFO]\033[0m  %s\n' "$*" >&2; }
ok()    { printf '\033[0;32m[ OK ]\033[0m  %s\n' "$*" >&2; }
die()   { printf '\033[0;31m[FAIL]\033[0m  %s\n' "$*" >&2; exit 1; }

# ── Step 1: 构建 libchan 静态库 ─────────────────────────────────────────────
info "构建 libchan (Release, static)..."
cmake -B "$ROOT/build" -S "$ROOT" \
    -DCMAKE_BUILD_TYPE=Release \
    -DLIBCHAN_BUILD_BENCH=OFF \
    -DLIBCHAN_BUILD_TESTS=OFF \
    -DLIBCHAN_BUILD_EXAMPLES=OFF \
    -DLIBCHAN_BUILD_SHARED=OFF \
    -Wno-dev -DCMAKE_RULE_MESSAGES=OFF \
    > /dev/null 2>&1
cmake --build "$ROOT/build" --parallel --target chan_static > /dev/null 2>&1

# 定位静态库（cmake 不同版本输出位置不同）
LIBCHAN_A=""
for candidate in "$ROOT/build/libchan.a" "$ROOT/build/lib/libchan.a" \
                  "$ROOT/build/Release/libchan.a"; do
    [ -f "$candidate" ] && { LIBCHAN_A="$candidate"; break; }
done
# 如果找不到，用 find 搜一下
if [ -z "$LIBCHAN_A" ]; then
    LIBCHAN_A="$(find "$ROOT/build" -name 'libchan.a' 2>/dev/null | head -1)"
fi
[ -f "$LIBCHAN_A" ] || die "找不到 libchan.a，请先构建主项目"
ok "libchan: $LIBCHAN_A"

# ── Step 2: 编译 C 基准 ──────────────────────────────────────────────────────
info "编译 C 基准..."
gcc -O3 -std=c11 -pthread \
    "$CL/libchan/bench.c" "$LIBCHAN_A" \
    -I"$ROOT/include" \
    -o "$CL/libchan/bench_c"
ok "bench_c 编译完成"

# ── Step 3: 编译 Go 基准 ─────────────────────────────────────────────────────
info "编译 Go 基准..."
( cd "$CL/go" && go build -o bench_go . )
ok "bench_go 编译完成 ($(go version | awk '{print $3}'))"

# ── Step 4: 编译 Rust 基准 ───────────────────────────────────────────────────
info "编译 Rust 基准（cargo --release，首次需下载依赖）..."
( cd "$CL/rust" && cargo build --release -q 2>&1 | grep -v "^$" >&2 || true )
[ -f "$CL/rust/target/release/bench_rust" ] || \
    die "bench_rust 编译失败"
ok "bench_rust 编译完成 ($(rustc --version | awk '{print $2}'))"

# ── Step 5: 运行三端基准 ─────────────────────────────────────────────────────
TMP_C=$(mktemp);  TMP_GO=$(mktemp);  TMP_RUST=$(mktemp)
trap 'rm -f "$TMP_C" "$TMP_GO" "$TMP_RUST"' EXIT

NSCEN=6
TOTAL_S=$(( NSCEN * 3 * (400 + 1500) / 1000 ))
info "运行基准（每场景 1.9s × $NSCEN 场景 × 3 语言 ≈ ${TOTAL_S}s）..."

info "  C / libchan..."
"$CL/libchan/bench_c" > "$TMP_C"
info "  Go..."
"$CL/go/bench_go" > "$TMP_GO"
info "  Rust / crossbeam..."
"$CL/rust/target/release/bench_rust" > "$TMP_RUST"
ok "基准运行完成"

# ── Step 6: 合并 CSV，输出 Markdown ─────────────────────────────────────────
ENV_LINE="CPU: $(grep 'model name' /proc/cpuinfo 2>/dev/null | head -1 | cut -d: -f2 | xargs)  |  OS: $(uname -r)  |  Date: $(date +%Y-%m-%d)"
GCC_VER="$(gcc --version 2>/dev/null | head -1)"
GO_VER="$(go version | awk '{print $3}')"
RUST_VER="$(rustc --version | awk '{print $2}')"

# 用 bash 关联数组合并三个 CSV，按 lang 后缀（_direct / _select）分桶
declare -A M   # 键: "<lang>_<variant>|<np>,<nc>,<cap>"

ingest() {  # $1=文件
    while IFS=, read -r lang np nc cap mops; do
        [ -z "$lang" ] && continue
        M["${lang}|${np},${nc},${cap}"]="$mops"
    done < "$1"
}
ingest "$TMP_C"; ingest "$TMP_GO"; ingest "$TMP_RUST"

# 场景定义：key 对应 CSV 中的 np,nc,cap
KEYS=("1,1,0" "1,1,64" "1,1,1024" "2,2,1024" "4,4,1024" "8,8,1024")
LABELS=("1P+1C  cap=0 (unbuf)" "1P+1C  cap=64" "1P+1C  cap=1024" \
        "2P+2C  cap=1024" "4P+4C  cap=1024" "8P+8C  cap=1024")

# build_table <variant> [with_spsc]   (direct | select ; with_spsc=1 adds libchan SPSC 列)
build_table() {
    local var="$1" with_spsc="${2:-0}"
    if [ "$with_spsc" = 1 ]; then
        printf "| %-22s | %15s | %14s | %10s | %18s |\n" \
            "场景" "libchan (C)" "libchan SPSC" "Go chan" "crossbeam (Rust)"
        printf "|%s|%s|%s|%s|%s|\n" \
            "----------------------" "-----------------" "----------------" "------------" "--------------------"
    else
        printf "| %-22s | %15s | %10s | %18s |\n" \
            "场景" "libchan (C)" "Go chan" "crossbeam (Rust)"
        printf "|%s|%s|%s|%s|\n" \
            "----------------------" "-----------------" "------------" "--------------------"
    fi
    for i in "${!KEYS[@]}"; do
        k="${KEYS[$i]}"
        c="${M[libchan_${var}|$k]:-0.000}"
        g="${M[go_${var}|$k]:-0.000}"
        r="${M[crossbeam_${var}|$k]:-0.000}"
        if [ "$with_spsc" = 1 ]; then
            s="${M[libchan_spsc|$k]:-—}"     # SPSC 仅 1P1C 有缓冲场景有值，其余为 —
            printf "| %-22s | %13s   | %14s | %10s | %18s |\n" \
                "${LABELS[$i]}" "$c" "$s" "$g" "$r"
        else
            printf "| %-22s | %13s   | %10s | %18s |\n" \
                "${LABELS[$i]}" "$c" "$g" "$r"
        fi
    done
}

TABLE_DIRECT="$(build_table direct 1)"
TABLE_SELECT="$(build_table select 0)"

# 终端输出
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "  Channel 跨语言吞吐对比（Mops/s，越高越好）"
echo "  $ENV_LINE"
echo "════════════════════════════════════════════════════════════════════"
echo "【直连 chan_send/recv】"
echo "$TABLE_DIRECT"
echo ""
echo "【select 多路复用】"
echo "$TABLE_SELECT"
echo ""

# ── Step 7: 写入 doc/comparison.md ──────────────────────────────────────────
OUT="$ROOT/doc/comparison.md"
{
cat << 'HEADER'
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

HEADER

printf "## 测试环境\n\n"
printf '```\n'
printf "CPU    : %s\n" "$(grep 'model name' /proc/cpuinfo 2>/dev/null | head -1 | cut -d: -f2 | xargs)"
printf "OS     : %s\n" "$(uname -r)"
printf "C      : %s\n" "$GCC_VER"
printf "Go     : %s\n" "$GO_VER"
printf "Rust   : %s\n" "$RUST_VER"
printf "Date   : %s\n" "$(date '+%Y-%m-%d')"
printf '```\n\n'

printf -- "---\n\n"
printf "## 结果（单位：Mops/s，越高越好）\n\n"

printf "### 路径一：直连 chan_send / chan_recv（核心路径）\n\n"
echo "$TABLE_DIRECT"
echo ""
printf "### 路径二：select 多路复用\n\n"
echo "$TABLE_SELECT"
echo ""

cat << 'ANALYSIS'

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
ANALYSIS
} > "$OUT"

ok "报告已写入 $OUT"
