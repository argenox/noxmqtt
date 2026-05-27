set(NOXMQTT_CLI_TARGET noxmqtt_cli)

if(NOXMQTT_CLI_ENABLE_NOXTLS)
    if(NOT TARGET noxmqtt_transport_noxtls)
        message(FATAL_ERROR
            "NOXMQTT_CLI_ENABLE_NOXTLS=ON requires the reusable noxmqtt_transport_noxtls target.")
    endif()
    set(NOXMQTT_CLI_TRANSPORT_TARGET noxmqtt_transport_noxtls)
    set(NOXMQTT_CLI_HAS_NOXTLS 1)
else()
    if(NOT TARGET noxmqtt::transport_tcp)
        message(FATAL_ERROR
            "No reusable TCP transport target is available for this platform.")
    endif()
    set(NOXMQTT_CLI_TRANSPORT_TARGET noxmqtt::transport_tcp)
    set(NOXMQTT_CLI_HAS_NOXTLS 0)
endif()

add_executable(${NOXMQTT_CLI_TARGET}
    main.c
)

set_target_properties(${NOXMQTT_CLI_TARGET} PROPERTIES OUTPUT_NAME noxmqtt)

target_include_directories(${NOXMQTT_CLI_TARGET}
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/../../src/noxmqtt-lib
)

if(NOXMQTT_CLI_ENABLE_NOXTLS)
    target_include_directories(${NOXMQTT_CLI_TARGET}
        PRIVATE
            ${NOXMQTT_NOXTLS_ROOT}
    )
endif()

target_link_libraries(${NOXMQTT_CLI_TARGET}
    PRIVATE
        noxmqtt::noxmqtt
        ${NOXMQTT_CLI_TRANSPORT_TARGET}
)

target_compile_features(${NOXMQTT_CLI_TARGET} PRIVATE c_std_11)
target_compile_definitions(${NOXMQTT_CLI_TARGET} PRIVATE NOXMQTT_APP_HAS_NOXTLS=${NOXMQTT_CLI_HAS_NOXTLS})

if(MSVC)
    target_compile_definitions(${NOXMQTT_CLI_TARGET} PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()
