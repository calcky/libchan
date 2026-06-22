set(LIBCHAN_SANITIZE "" CACHE STRING
    "Sanitizer build: address | thread | undefined | address,undefined")

function(chan_enable_sanitizers target)
    if(NOT LIBCHAN_SANITIZE)
        return()
    endif()
    if(LIBCHAN_SANITIZE MATCHES "thread" AND LIBCHAN_SANITIZE MATCHES "address")
        message(FATAL_ERROR "TSan and ASan cannot be enabled simultaneously.")
    endif()
    target_compile_options(${target} PRIVATE -fsanitize=${LIBCHAN_SANITIZE} -fno-omit-frame-pointer)
    target_link_options(${target} PUBLIC -fsanitize=${LIBCHAN_SANITIZE})
endfunction()
