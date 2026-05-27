cmake_minimum_required(VERSION 3.16)

project(noxmqtt_tests C)

enable_testing()

option(NOXMQTT_BUILD_INTEGRATION_TESTS "Build broker-backed integration tests" ${UNIX})

add_executable(noxmqtt_tests
    test_noxmqtt_auth.c
    ../src/noxmqtt-lib/noxmqtt.c
    ../src/noxmqtt-lib/noxmqtt_mqtt5.c
    ../src/noxmqtt-lib/noxmqttlib.c
    ../src/noxmqtt-lib/noxmqtt_debug.c
)

target_include_directories(noxmqtt_tests PRIVATE
    ../src/noxmqtt-lib
)

target_compile_features(noxmqtt_tests PRIVATE c_std_11)

if (MSVC)
    target_compile_definitions(noxmqtt_tests PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

add_test(NAME noxmqtt_unit_tests COMMAND noxmqtt_tests)
set_tests_properties(noxmqtt_unit_tests PROPERTIES LABELS "unit")

if (NOXMQTT_BUILD_INTEGRATION_TESTS)
    add_executable(noxmqtt_integration_tests
        test_noxmqtt_integration.c
        ../src/noxmqtt-lib/noxmqtt.c
        ../src/noxmqtt-lib/noxmqtt_mqtt5.c
        ../src/noxmqtt-lib/noxmqttlib.c
        ../src/noxmqtt-lib/noxmqtt_debug.c
    )

    target_include_directories(noxmqtt_integration_tests PRIVATE
        ../src/noxmqtt-lib
    )

    target_compile_features(noxmqtt_integration_tests PRIVATE c_std_11)

    if (MSVC)
        target_compile_definitions(noxmqtt_integration_tests PRIVATE _CRT_SECURE_NO_WARNINGS)
    else()
        target_compile_definitions(noxmqtt_integration_tests PRIVATE _POSIX_C_SOURCE=200809L)
    endif()

    add_test(NAME noxmqtt_integration_tests COMMAND noxmqtt_integration_tests)
    set_tests_properties(noxmqtt_integration_tests PROPERTIES LABELS "integration")
endif()
