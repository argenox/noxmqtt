idf_component_register(
    SRCS
        "noxmqtt_transport_espidf_tcp.c"
    INCLUDE_DIRS
        "."
    REQUIRES
        noxmqtt
    PRIV_REQUIRES
        esp_timer
        freertos
        lwip
)

target_compile_features(${COMPONENT_LIB} PRIVATE c_std_11)
