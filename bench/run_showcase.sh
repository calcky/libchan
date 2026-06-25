#!/usr/bin/env bash
# bench/run_showcase.sh
#
# Run bench_showcase with core pinning (performance ladder + A/B tiers), and
# print the measurement environment.
#
# Usage:
#   bash bench/run_showcase.sh
#
# Core pinning (taskset) restricts the benchmark threads to fixed physical
# cores, reducing jitter from thread migration.
# Note: WSL2 runs on top of the Windows scheduler, so taskset can only reduce
# Linux-side migration and cannot eliminate jitter from the Windows layer --
# for serious measurement, run on [native Linux] and consider isolating cores
# with isolcpus.
# Therefore this benchmark's B-tier numbers (which include park/scheduling) are
# only an order-of-magnitude reference; the A-tier (lock-free fast path) is more
# stable.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD="$ROOT/build-bench"

# ── build ─────────────────────────────────────────────────────
cmake -B "$BUILD" -S "$ROOT" \
    -DCMAKE_BUILD_TYPE=Release \
    -DLIBCHAN_BUILD_BENCH=ON \
    -DLIBCHAN_BUILD_TESTS=OFF \
    -DLIBCHAN_BUILD_EXAMPLES=OFF \
    > /dev/null 2>&1
cmake --build "$BUILD" --parallel --target bench_showcase > /dev/null 2>&1

BIN="$BUILD/bench/bench_showcase"
[ -x "$BIN" ] || { echo "build failed: $BIN not found" >&2; exit 1; }

# ── environment info ──────────────────────────────────────────
echo "════════════════════════════════════════════════════════════════════"
echo "  CPU    : $(grep 'model name' /proc/cpuinfo 2>/dev/null | head -1 | cut -d: -f2 | xargs)"
echo "  cores  : $(nproc) logical cores"
echo "  OS     : $(uname -r)"
echo "  compiler : $(gcc --version 2>/dev/null | head -1)"
echo "  date   : $(date '+%Y-%m-%d %H:%M')"
echo "════════════════════════════════════════════════════════════════════"
echo ""

# ── run with core pinning ─────────────────────────────────────
# pick 8 cores (if logical cores >= 8) so the largest 4P+4C scenario gives each
# thread its own core.
NCPU=$(nproc)
if [ "$NCPU" -ge 8 ]; then
    CPUSET="2-9"
    echo "[core pinning] taskset -c $CPUSET"
    taskset -c "$CPUSET" "$BIN"
else
    echo "[no core pinning] logical cores < 8, running directly"
    "$BIN"
fi
