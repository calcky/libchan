#!/usr/bin/env bash
# bench/run_showcase.sh
#
# 钉核运行 bench_showcase（性能阶梯 + A/B 分档），并打印测量环境。
#
# 用法:
#   bash bench/run_showcase.sh
#
# 钉核(taskset)把基准线程限制在固定物理核上,减少线程迁移带来的抖动。
# 注意:WSL2 跑在 Windows 调度器之上,taskset 只能减少 Linux 侧迁移,无法消除
# Windows 层的抖动 —— 严肃测量请在【原生 Linux】上跑,并考虑 isolcpus 隔离核。
# 因此本基准的 B 档(含 park/调度)数字仅供量级参考;A 档(无锁快路径)较稳定。

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD="$ROOT/build-bench"

# ── 构建 ──────────────────────────────────────────────────────
cmake -B "$BUILD" -S "$ROOT" \
    -DCMAKE_BUILD_TYPE=Release \
    -DLIBCHAN_BUILD_BENCH=ON \
    -DLIBCHAN_BUILD_TESTS=OFF \
    -DLIBCHAN_BUILD_EXAMPLES=OFF \
    > /dev/null 2>&1
cmake --build "$BUILD" --parallel --target bench_showcase > /dev/null 2>&1

BIN="$BUILD/bench/bench_showcase"
[ -x "$BIN" ] || { echo "构建失败: 找不到 $BIN" >&2; exit 1; }

# ── 环境信息 ──────────────────────────────────────────────────
echo "════════════════════════════════════════════════════════════════════"
echo "  CPU    : $(grep 'model name' /proc/cpuinfo 2>/dev/null | head -1 | cut -d: -f2 | xargs)"
echo "  核数   : $(nproc) 逻辑核"
echo "  OS     : $(uname -r)"
echo "  编译器 : $(gcc --version 2>/dev/null | head -1)"
echo "  日期   : $(date '+%Y-%m-%d %H:%M')"
echo "════════════════════════════════════════════════════════════════════"
echo ""

# ── 钉核运行 ──────────────────────────────────────────────────
# 选 8 个核(若逻辑核 >= 8),让最大 4P+4C 场景每线程独占一核。
NCPU=$(nproc)
if [ "$NCPU" -ge 8 ]; then
    CPUSET="2-9"
    echo "[钉核] taskset -c $CPUSET"
    taskset -c "$CPUSET" "$BIN"
else
    echo "[未钉核] 逻辑核 < 8,直接运行"
    "$BIN"
fi
