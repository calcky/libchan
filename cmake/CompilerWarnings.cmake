function(chan_set_compiler_warnings target)
    target_compile_options(${target} PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wshadow
        -Wcast-align
        -Wstrict-prototypes
        -Wmissing-prototypes
        -Wno-unused-parameter
        $<$<C_COMPILER_ID:Clang>:-Wno-gnu-zero-variadic-macro-arguments>
    )
endfunction()
