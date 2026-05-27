add_library(noxmqtt STATIC
    noxmqtt.c
    noxmqtt_mqtt5.c
    noxmqttlib.c
    noxmqtt_debug.c
)

add_library(noxmqtt::noxmqtt ALIAS noxmqtt)

target_include_directories(noxmqtt
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_features(noxmqtt PUBLIC c_std_11)

if(MSVC)
    target_compile_definitions(noxmqtt PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()
