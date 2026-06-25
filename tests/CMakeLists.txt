add_library(chan_test_helper INTERFACE)
target_include_directories(chan_test_helper INTERFACE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(chan_test_helper INTERFACE chan_static Threads::Threads)
target_compile_definitions(chan_test_helper INTERFACE _GNU_SOURCE)
if(LIBCHAN_SANITIZE)
    target_compile_options(chan_test_helper INTERFACE -fsanitize=${LIBCHAN_SANITIZE} -fno-omit-frame-pointer)
    target_link_options(chan_test_helper INTERFACE -fsanitize=${LIBCHAN_SANITIZE})
endif()

function(add_chan_test name)
    add_executable(${name} ${name}.c)
    target_link_libraries(${name} PRIVATE chan_test_helper)
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES TIMEOUT 30)
endfunction()

add_chan_test(test_basic)
add_chan_test(test_blocking)
add_chan_test(test_close)
add_chan_test(test_mpmc)
add_chan_test(test_select)
add_chan_test(test_stress)

# test_bulk exercises the internal ring bulk API directly, so it also needs the
# src/ include path for ring_lf.h.
add_chan_test(test_bulk)
target_include_directories(test_bulk PRIVATE ${CMAKE_SOURCE_DIR}/src)
