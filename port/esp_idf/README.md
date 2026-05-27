# NoxMQTT ESP-IDF Transports

The root `argenox/noxmqtt` component is now MQTT-core only. It does not bundle any socket or TLS transport.

To use NoxMQTT in an ESP-IDF application, add:

- the core NoxMQTT component
- exactly one ESP-IDF transport component from this directory

Available local transport components:

- `noxmqtt_transport_espidf_tcp`
- `noxmqtt_transport_espidf_noxtls`

## Local application layout

Add this repository's transport components to your application before `project()`:

```cmake
list(APPEND EXTRA_COMPONENT_DIRS
    "/path/to/noxmqtt/port/esp_idf/components"
)
```

If you use the NoxTLS transport, also add the NoxTLS ESP-IDF component wrapper before `project()`:

```cmake
list(APPEND EXTRA_COMPONENT_DIRS
    "/path/to/noxtls/ports/esp-idf"
)
```

Then make your application depend on the NoxMQTT core plus one transport:

```cmake
idf_component_register(
    SRCS "main.c"
    REQUIRES
        noxmqtt
        noxmqtt_transport_espidf_noxtls
)
```

Or for plain TCP:

```cmake
idf_component_register(
    SRCS "main.c"
    REQUIRES
        noxmqtt
        noxmqtt_transport_espidf_tcp
)
```

## NoxTLS transport notes

`noxmqtt_transport_espidf_noxtls` keeps NoxTLS out of the NoxMQTT core library target. The transport component owns:

- socket connect and receive task
- TLS handshake
- trust-store loading
- encrypted send/receive

Current limitation:

- client certificate / private key authentication is not wired into the ESP-IDF NoxTLS transport yet

The transport expects `conf.server.tls.ca_cert` to be either:

- a PEM certificate string embedded in firmware, or
- a readable filesystem path on the target
