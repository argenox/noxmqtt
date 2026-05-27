# NoxMQTT - MQTT Client for Embedded Devices

NoxMQTT is a complete MQTT client that's cross platform and enables optimized simple access
to MQTT brokers.

## License

NoxMQTT is dual-licensed under:

- `GPL-2.0-only`
- `LicenseRef-Argenox-Commercial`

You may use NoxMQTT under GPL v2.0 only, or under a separate commercial license from Argenox
Technologies LLC. See [LICENSE](./LICENSE) and
[LICENSE-ARGENOX-COMMERCIAL.md](./LICENSE-ARGENOX-COMMERCIAL.md).

## Why another MQTT Client?

While there's are several mqtt clients out there, these clients tend to be relatively large 
and complicated to integrate in a real embedded system.

NoxMQTT is small, efficient, and most importantly easy to use.

Our experience with other MQTT clients is that they hide a lot of things behind the scenes which
makes them difficult to use in real products where errors can occur for a variety of reasons.

Many of these clients are also vendor/platform specific, making reusing of code difficult.
Some of these have terrible code, wierd behaviors and side effects that make creating a reliable 


## Integrating NoxMQTT

* Add files in src/lib to your project
* Import header files
    #include "noxmqtt.h"
    #include "noxmqtt_tal.h"

* Use an existing TCP Abstraction Layer (TAL) file or create your own. This file provides
  standard TCP socket connectivity functions. See `noxmqtt_tal.h` for files needed.

* Call library functions

## CMake Builds

The core `noxmqtt` library target is transport-agnostic and does not depend on NoxTLS.

Build the cross-platform `noxmqtt` CLI with plain TCP transport:

```sh
cmake -S . -B build -DNOXMQTT_BUILD_CLI_APP=ON
cmake --build build
```

Build the `noxmqtt` CLI with a NoxTLS-backed secure transport without adding NoxTLS to the
`noxmqtt` library target:

```sh
cmake -S . -B build-noxtls \
  -DNOXMQTT_BUILD_CLI_APP=ON \
  -DNOXMQTT_CLI_ENABLE_NOXTLS=ON \
  -DNOXMQTT_NOXTLS_ROOT=/path/to/noxtls \
  -DNOXMQTT_NOXTLS_PROFILE=default
cmake --build build-noxtls
```

Relevant CMake options:

- `NOXMQTT_BUILD_CLI_APP`: builds the `noxmqtt` command-line application.
- `NOXMQTT_CLI_ENABLE_NOXTLS`: enables the CLI application's NoxTLS transport.
- `NOXMQTT_BUILD_NOXTLS_TRANSPORT`: builds the reusable `noxmqtt_transport_noxtls` module even when the CLI app is not using it.
- `NOXMQTT_NOXTLS_ROOT`: points at the NoxTLS source tree used by the reusable NoxTLS transport module.
- `NOXMQTT_NOXTLS_PROFILE`: selects the NoxTLS feature profile for secure CLI builds.

Reusable transport targets:

- `noxmqtt_transport_windows_tcp`: reusable Windows TCP transport module.
- `noxmqtt_transport_posix_tcp`: reusable POSIX TCP transport module for Linux and macOS.
- `noxmqtt_transport_noxtls`: reusable secure transport module backed by NoxTLS on the active host platform.

## CLI Application

The repository now ships a full client application named `noxmqtt`.

Examples:

```sh
noxmqtt pub -h broker.example.com -p 1883 -t demo/topic -m hello -q 1
noxmqtt sub --tls --cafile ./ca.pem -h broker.example.com -p 8883 -t demo/topic --count 1
noxmqtt interactive --host test.mosquitto.org --port 1883
```

`noxmqtt interactive` opens a REPL with connect, disconnect, publish, subscribe, unsubscribe, and
live configuration commands. The binary also supports one-shot `pub`, `sub`, and `unsub` modes so
it can be used similarly to the Mosquitto CLI tools.

## ESP-IDF Component

This repository includes an ESP-IDF component definition at the repository root for publishing
`argenox/noxmqtt` to the ESP Component Registry.

Example dependency:

```yaml
dependencies:
  argenox/noxmqtt: "^1.0.0"
```

The published root component is now MQTT-core only. It packages the protocol sources from
`src/noxmqtt-lib` and does not bundle any ESP-IDF socket or TLS transport implementation.

For ESP-IDF applications, pick exactly one local transport component from
[`port/esp_idf/README.md`](./port/esp_idf/README.md):

- `noxmqtt_transport_espidf_tcp`
- `noxmqtt_transport_espidf_noxtls`

Example app configuration with the local NoxTLS transport:

```cmake
list(APPEND EXTRA_COMPONENT_DIRS
    "/path/to/noxmqtt/port/esp_idf/components"
    "/path/to/noxtls/ports/esp-idf"
)

idf_component_register(
    SRCS "main.c"
    REQUIRES
        noxmqtt
        noxmqtt_transport_espidf_noxtls
)
```

That keeps `noxmqtt` transport-agnostic while allowing the application to opt into MQTTS by
adding the NoxTLS-backed transport component.

### Publishing on tag release

GitHub Actions publishes the component on tag pushes via
`.github/workflows/publish-esp-idf-component.yml`.

Before the workflow can upload with OIDC, configure `argenox/noxmqtt` in the ESP Component
Registry and add this repository/workflow as a trusted uploader.
