# Coverage instrumentation (gcov/lcov/gcovr).
#
# Enable with -DLIBCHAN_COVERAGE=ON. Instruments only the library objects
# (src/*.c); test code is excluded from the report by gcovr's --filter.
# -fprofile-update=atomic is required: the stress tests are heavily threaded
# and non-atomic counter updates would race and corrupt the .gcda counts.
option(LIBCHAN_COVERAGE "Instrument the library for gcov coverage" OFF)

function(chan_enable_coverage target)
    if(NOT LIBCHAN_COVERAGE)
        return()
    endif()
    target_compile_options(${target} PRIVATE
        --coverage -fprofile-update=atomic -O0 -g -fno-inline)
    target_link_options(${target} PUBLIC --coverage)
endfunction()
