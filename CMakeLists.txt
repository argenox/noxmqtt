cmake_minimum_required(VERSION 3.16)

if(ESP_PLATFORM)
    idf_component_register(
        SRCS
            "src/noxmqtt-lib/noxmqtt.c"
            "src/noxmqtt-lib/noxmqtt_mqtt5.c"
            "src/noxmqtt-lib/noxmqttlib.c"
            "src/noxmqtt-lib/noxmqtt_debug.c"
        INCLUDE_DIRS
            "src/noxmqtt-lib"
    )

    target_compile_features(${COMPONENT_LIB} PRIVATE c_std_11)
else()
    project(noxmqtt LANGUAGES C)

    option(NOXMQTT_BUILD_CLI_APP "Build the noxmqtt command-line application" ON)
    option(NOXMQTT_BUILD_NOXTLS_TRANSPORT "Build the reusable Windows NoxTLS transport module" OFF)
    option(NOXMQTT_CLI_ENABLE_NOXTLS "Enable NoxTLS in the noxmqtt CLI transport" OFF)
    set(NOXMQTT_NOXTLS_ROOT
        "${CMAKE_CURRENT_SOURCE_DIR}/../noxtls_stm32_cube/third_party/noxtls"
        CACHE PATH
        "Path to the NoxTLS source tree used by the noxmqtt CLI transport")
    set(NOXMQTT_NOXTLS_PROFILE
        "default"
        CACHE STRING
        "NoxTLS feature profile used by the noxmqtt CLI transport")

    if(DEFINED NOXMQTT_BUILD_CLIENT_APP)
        set(NOXMQTT_BUILD_CLI_APP "${NOXMQTT_BUILD_CLIENT_APP}" CACHE BOOL "" FORCE)
    endif()

    if(DEFINED NOXMQTT_CLIENT_APP_ENABLE_NOXTLS)
        set(NOXMQTT_CLI_ENABLE_NOXTLS "${NOXMQTT_CLIENT_APP_ENABLE_NOXTLS}" CACHE BOOL "" FORCE)
    endif()

    add_subdirectory(src/noxmqtt-lib)
    if(WIN32)
        add_subdirectory(port/windows)
    elseif(UNIX)
        add_subdirectory(port/posix)
    endif()

    if(NOXMQTT_BUILD_CLI_APP)
        add_subdirectory(apps/noxmqtt)
    endif()
endif()
