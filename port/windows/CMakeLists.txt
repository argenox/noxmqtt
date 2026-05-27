if(NOT WIN32)
    return()
endif()

set(NOXMQTT_WINDOWS_TCP_TARGET noxmqtt_transport_windows_tcp)
set(NOXMQTT_WINDOWS_NOXTLS_TARGET noxmqtt_transport_noxtls)
set(NOXMQTT_NOXTLS_LIBS)

add_library(${NOXMQTT_WINDOWS_TCP_TARGET} STATIC
    noxmqtt_transport_windows.c
)

target_include_directories(${NOXMQTT_WINDOWS_TCP_TARGET}
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/noxmqtt-lib
)

target_link_libraries(${NOXMQTT_WINDOWS_TCP_TARGET}
    PUBLIC
        noxmqtt::noxmqtt
        ws2_32
)

target_compile_features(${NOXMQTT_WINDOWS_TCP_TARGET} PRIVATE c_std_11)
target_compile_definitions(${NOXMQTT_WINDOWS_TCP_TARGET} PRIVATE NOXMQTT_ENABLE_NOXTLS=0)

if(MSVC)
    target_compile_definitions(${NOXMQTT_WINDOWS_TCP_TARGET} PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

add_library(noxmqtt::transport_windows_tcp ALIAS ${NOXMQTT_WINDOWS_TCP_TARGET})
add_library(noxmqtt::transport_tcp ALIAS ${NOXMQTT_WINDOWS_TCP_TARGET})

if(NOXMQTT_BUILD_NOXTLS_TRANSPORT OR NOXMQTT_CLI_ENABLE_NOXTLS)
    if(NOT EXISTS "${NOXMQTT_NOXTLS_ROOT}/CMakeLists.txt")
        message(FATAL_ERROR
            "NOXNoxMQTTTLS transport requested but NOXMQTT_NOXTLS_ROOT is invalid: ${NOXMQTT_NOXTLS_ROOT}")
    endif()

    set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(BUILD_APPLICATIONS OFF CACHE BOOL "" FORCE)
    set(NOXTLS_PROFILE "${NOXMQTT_NOXTLS_PROFILE}" CACHE STRING "" FORCE)

    if(NOT TARGET noxtls_tls)
        add_subdirectory("${NOXMQTT_NOXTLS_ROOT}" noxtls-build EXCLUDE_FROM_ALL)
    endif()

    set(NOXMQTT_NOXTLS_LIBS
        noxtls_tls
        noxtls_cert
        noxtls_pkc
        noxtls_encryption
        noxtls_hash
        noxtls_common
        noxtls_drbg
        noxtls_utility
        noxtls_certificates
    )

    add_library(${NOXMQTT_WINDOWS_NOXTLS_TARGET} STATIC
        noxmqtt_transport_windows.c
    )

    target_include_directories(${NOXMQTT_WINDOWS_NOXTLS_TARGET}
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/../../src/noxmqtt-lib
    )

    target_link_libraries(${NOXMQTT_WINDOWS_NOXTLS_TARGET}
        PUBLIC
            noxmqtt::noxmqtt
            ws2_32
            ${NOXMQTT_NOXTLS_LIBS}
    )

    target_compile_features(${NOXMQTT_WINDOWS_NOXTLS_TARGET} PRIVATE c_std_11)
    target_compile_definitions(${NOXMQTT_WINDOWS_NOXTLS_TARGET} PRIVATE NOXMQTT_ENABLE_NOXTLS=1)

    if(MSVC)
        target_compile_definitions(${NOXMQTT_WINDOWS_NOXTLS_TARGET} PRIVATE _CRT_SECURE_NO_WARNINGS)
    endif()

    add_library(noxmqtt::transport_noxtls ALIAS ${NOXMQTT_WINDOWS_NOXTLS_TARGET})
endif()
