#!/usr/bin/env bash
# run_park_cmp.sh
#
# 分别以 futex 和 pthread 两种 park 后端构建 libchan，
# 运行 bench_lock_overhead，并排对比结果。
#
# 用法：
#   bash bench/run_park_cmp.sh              # 仅打印
#   bash bench/run_park_cmp.sh --save       # 同时写入 doc/benchmarks.md
#
# 要求：cmake、make、gcc 已在 PATH 中。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
BUILD_FUTEX="$ROOT/build-park-futex"
BUILD_PTHREAD="$ROOT/build-park-pthread"
SAVE=0
[ "${1:-}" = "--save" ] && SAVE=1

# ---- 构建两种配置 ----
build_variant() {
    local dir="$1" force_pthread="$2"
    cmake -B "$dir" -S "$ROOT" \
        -DCMAKE_BUILD_TYPE=Release \
        -DLIBCHAN_BUILD_BENCH=ON \
        -DLIBCHAN_BUILD_TESTS=OFF \
        -DLIBCHAN_BUILD_EXAMPLES=OFF \
        -DLIBCHAN_FORCE_PTHREAD_PARK="$force_pthread" \
        -DLIBCHAN_SPIN_LIMIT=40 \
        -DCMAKE_C_FLAGS="-O3" \
        -Wno-dev -DCMAKE_RULE_MESSAGES=OFF \
        > /dev/null 2>&1
    cmake --build "$dir" --parallel --target bench_lock_overhead \
        > /dev/null 2>&1
}

echo "构建 futex 版本 ..." >&2
build_variant "$BUILD_FUTEX"  "OFF"
echo "构建 pthread 版本 ..." >&2
build_variant "$BUILD_PTHREAD" "ON"

# ---- 运行并捕获输出（只保留数据行）----
# 过滤掉空行和纯分隔行，保留标题行和数据行
run_and_filter() {
    "$1/bench/bench_lock_overhead" 2>/dev/null \
        | grep -v '^[[:space:]]*$' \
        | grep -v '^-\+$'
}

TMP_F="$(mktemp)"
TMP_P="$(mktemp)"
trap 'rm -f "$TMP_F" "$TMP_P"' EXIT

echo "运行 futex ..." >&2
run_and_filter "$BUILD_FUTEX"  > "$TMP_F"
echo "运行 pthread ..." >&2
run_and_filter "$BUILD_PTHREAD" > "$TMP_P"

# ---- 并排输出 ----
COL=52   # 左列宽度

format_row() {
    local left="$1" right="$2"
    printf "%-${COL}s  %s\n" "$left" "$right"
}

HEADER_LEFT="futex (Linux syscall)"
HEADER_RIGHT="pthread (mutex+condvar)"

OUTPUT=""
OUTPUT+="$(format_row "场景" "futex ns/op    Mops/s    pthread ns/op    Mops/s")"$'\n'
OUTPUT+="$(printf '%0.s-' {1..90})"$'\n'

# 逐行对比（行数可能不一致，用 paste 对齐）
while IFS= read -r line; do
    OUTPUT+="$line"$'\n'
done < <(paste -d'|' "$TMP_F" "$TMP_P" \
    | awk -F'|' '{
        printf "%-52s  %s\n", $1, $2
    }')

echo ""
echo "======================================================================"
echo "  libchan Park 后端对比：futex vs pthread"
echo "======================================================================"
printf "  %-48s  %s\n" "[ futex ]" "[ pthread ]"
echo "----------------------------------------------------------------------"
paste "$TMP_F" "$TMP_P" | awk -F'\t' '{printf "  %-50s  %s\n", $1, $2}'
echo ""

# ---- 可选：写入 doc/benchmarks.md ----
if [ "$SAVE" -eq 1 ]; then
    DOC="$ROOT/doc/benchmarks.md"
    {
        echo ""
        echo "## 1. Park 后端对比：futex vs pthread"
        echo ""
        echo '```'
        echo "构建配置：LIBCHAN_SPIN_LIMIT=40，-O3，$(uname -r)"
        printf "  %-50s  %s\n" "[ futex ]" "[ pthread ]"
        echo "----------------------------------------------------------------------"
        paste "$TMP_F" "$TMP_P" | awk -F'\t' '{printf "  %-50s  %s\n", $1, $2}'
        echo '```'
    } >> "$DOC"
    echo "已追加到 $DOC" >&2
fi
