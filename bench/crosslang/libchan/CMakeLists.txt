cmake_minimum_required(VERSION 3.16)
project(bench_crosslang_c C)

set(CMAKE_C_STANDARD 11)

# libchan 根目录由调用方传入：-DLIBCHAN_ROOT=...
if(NOT DEFINED LIBCHAN_ROOT)
    set(LIBCHAN_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../..")
endif()

add_executable(bench_c bench.c)
target_include_directories(bench_c PRIVATE ${LIBCHAN_ROOT}/include)
target_compile_options(bench_c PRIVATE -O3)
target_compile_definitions(bench_c PRIVATE _GNU_SOURCE)

# 优先链接静态库
find_library(CHAN_LIB
    NAMES chan
    PATHS ${LIBCHAN_ROOT}/build/lib
          ${LIBCHAN_ROOT}/build
    NO_DEFAULT_PATH)
if(CHAN_LIB)
    target_link_libraries(bench_c PRIVATE ${CHAN_LIB} pthread)
else()
    message(FATAL_ERROR "libchan not found under ${LIBCHAN_ROOT}/build — run cmake build first")
endif()
