#!/usr/bin/env bash
# run_park_cmp.sh
#
# Build libchan with each of the two park backends (futex and pthread),
# run bench_lock_overhead, and compare the results side by side.
#
# Usage:
#   bash bench/run_park_cmp.sh              # print only
#   bash bench/run_park_cmp.sh --save       # also write to doc/benchmarks.md
#
# Requires: cmake, make, gcc must be on PATH.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
BUILD_FUTEX="$ROOT/build-park-futex"
BUILD_PTHREAD="$ROOT/build-park-pthread"
SAVE=0
[ "${1:-}" = "--save" ] && SAVE=1

# ---- build both configurations ----
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

echo "Building futex version ..." >&2
build_variant "$BUILD_FUTEX"  "OFF"
echo "Building pthread version ..." >&2
build_variant "$BUILD_PTHREAD" "ON"

# ---- run and capture output (keep only data rows) ----
# filter out blank lines and pure separator rows, keep the header and data rows
run_and_filter() {
    "$1/bench/bench_lock_overhead" 2>/dev/null \
        | grep -v '^[[:space:]]*$' \
        | grep -v '^-\+$'
}

TMP_F="$(mktemp)"
TMP_P="$(mktemp)"
trap 'rm -f "$TMP_F" "$TMP_P"' EXIT

echo "Running futex ..." >&2
run_and_filter "$BUILD_FUTEX"  > "$TMP_F"
echo "Running pthread ..." >&2
run_and_filter "$BUILD_PTHREAD" > "$TMP_P"

# ---- side-by-side output ----
COL=52   # left column width

format_row() {
    local left="$1" right="$2"
    printf "%-${COL}s  %s\n" "$left" "$right"
}

HEADER_LEFT="futex (Linux syscall)"
HEADER_RIGHT="pthread (mutex+condvar)"

OUTPUT=""
OUTPUT+="$(format_row "scenario" "futex ns/op    Mops/s    pthread ns/op    Mops/s")"$'\n'
OUTPUT+="$(printf '%0.s-' {1..90})"$'\n'

# compare line by line (line counts may differ, align with paste)
while IFS= read -r line; do
    OUTPUT+="$line"$'\n'
done < <(paste -d'|' "$TMP_F" "$TMP_P" \
    | awk -F'|' '{
        printf "%-52s  %s\n", $1, $2
    }')

echo ""
echo "======================================================================"
echo "  libchan park backend comparison: futex vs pthread"
echo "======================================================================"
printf "  %-48s  %s\n" "[ futex ]" "[ pthread ]"
echo "----------------------------------------------------------------------"
paste "$TMP_F" "$TMP_P" | awk -F'\t' '{printf "  %-50s  %s\n", $1, $2}'
echo ""

# ---- optional: write to doc/benchmarks.md ----
if [ "$SAVE" -eq 1 ]; then
    DOC="$ROOT/doc/benchmarks.md"
    {
        echo ""
        echo "## 1. Park backend comparison: futex vs pthread"
        echo ""
        echo '```'
        echo "build config: LIBCHAN_SPIN_LIMIT=40, -O3, $(uname -r)"
        printf "  %-50s  %s\n" "[ futex ]" "[ pthread ]"
        echo "----------------------------------------------------------------------"
        paste "$TMP_F" "$TMP_P" | awk -F'\t' '{printf "  %-50s  %s\n", $1, $2}'
        echo '```'
    } >> "$DOC"
    echo "Appended to $DOC" >&2
fi
