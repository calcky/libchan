foreach(ex producer_consumer multi_recv_select multi_recv_fanin multi_recv_spin
           sieve_primes worker_pool select_cancel ping_pong ping_pong_buffered
           burst_pipeline select_fairness request_reply snake_chan
           dplane_pipeline dplane_pipeline_mpsc)
    add_executable(${ex} ${ex}.c)
    target_include_directories(${ex} PRIVATE ${CMAKE_SOURCE_DIR}/include)
    target_link_libraries(${ex} PRIVATE chan_static Threads::Threads)
    target_compile_definitions(${ex} PRIVATE _GNU_SOURCE)
    target_compile_options(${ex} PRIVATE -O2)
endforeach()
