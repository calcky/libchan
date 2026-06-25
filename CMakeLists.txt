cmake_minimum_required(VERSION 3.16)
project(libchan VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

option(LIBCHAN_BUILD_SHARED  "Build shared library"          ON)
option(LIBCHAN_BUILD_STATIC  "Build static library"          ON)
option(LIBCHAN_BUILD_TESTS   "Build unit tests"              ON)
option(LIBCHAN_BUILD_BENCH   "Build benchmarks"              OFF)
option(LIBCHAN_FORCE_PTHREAD_PARK "Force POSIX pthread fallback (disables futex)" OFF)
set(LIBCHAN_SPIN_LIMIT 40 CACHE STRING "Spin iterations before sleeping (0 = disable)")

include(cmake/CompilerWarnings.cmake)
include(cmake/Sanitizers.cmake)
include(cmake/Coverage.cmake)
find_package(Threads REQUIRED)

# Platform detection
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT LIBCHAN_FORCE_PTHREAD_PARK)
    set(LIBCHAN_USE_FUTEX ON)
    message(STATUS "libchan: using Linux futex park backend")
else()
    set(LIBCHAN_USE_FUTEX OFF)
    message(STATUS "libchan: using POSIX pthread park backend")
endif()

set(LIBCHAN_SOURCES
    src/chan_core.c
    src/chan_send.c
    src/chan_recv.c
    src/ring_buffer.c
    src/ring_lf.c
    src/waitq.c
    src/park.c
    src/select.c
    src/util.c
)

add_library(chan_objs OBJECT ${LIBCHAN_SOURCES})
set_property(TARGET chan_objs PROPERTY POSITION_INDEPENDENT_CODE ON)
target_include_directories(chan_objs PUBLIC include PRIVATE src)
target_link_libraries(chan_objs PUBLIC Threads::Threads)
target_compile_definitions(chan_objs PRIVATE
    LIBCHAN_BUILDING=1
    LIBCHAN_SPIN_LIMIT=${LIBCHAN_SPIN_LIMIT}
    $<$<BOOL:${LIBCHAN_USE_FUTEX}>:LIBCHAN_USE_FUTEX=1>
)

chan_set_compiler_warnings(chan_objs)
chan_enable_sanitizers(chan_objs)
chan_enable_coverage(chan_objs)
target_compile_definitions(chan_objs PRIVATE _GNU_SOURCE)

if(LIBCHAN_BUILD_SHARED)
    add_library(chan SHARED $<TARGET_OBJECTS:chan_objs>)
    target_include_directories(chan PUBLIC include)
    target_link_libraries(chan PUBLIC Threads::Threads)
    set_target_properties(chan PROPERTIES
        OUTPUT_NAME chan
        SOVERSION 0
        VERSION 0.1.0
        C_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN ON
    )
    install(TARGETS chan
        EXPORT libchanTargets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
    )
endif()

if(LIBCHAN_BUILD_STATIC)
    add_library(chan_static STATIC $<TARGET_OBJECTS:chan_objs>)
    target_include_directories(chan_static PUBLIC include)
    target_link_libraries(chan_static PUBLIC Threads::Threads)
    set_target_properties(chan_static PROPERTIES OUTPUT_NAME chan)
    chan_enable_sanitizers(chan_static)
    install(TARGETS chan_static
        EXPORT libchanTargets
        ARCHIVE DESTINATION lib
    )
endif()

install(FILES include/libchan.h DESTINATION include)

if(LIBCHAN_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(LIBCHAN_BUILD_BENCH)
    add_subdirectory(bench)
endif()

option(LIBCHAN_BUILD_EXAMPLES "Build examples" ON)
if(LIBCHAN_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
