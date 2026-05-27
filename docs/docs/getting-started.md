---
sidebar_position: 2
description: Build and integration overview for NoxMQTT.
---

# Getting Started

## Prerequisites

- CMake 3.16 or newer for the desktop build
- A C11-capable compiler
- A platform transport implementation

Optional:

- NoxTLS if your application wants MQTTS through a reusable transport module
- ESP-IDF 5.x or newer for ESP32 component integration

## Build the core library

From the repository root:

```bash
cmake -S . -B build
cmake --build build --config Debug
```

That builds the transport-agnostic MQTT core.

## Build the noxmqtt CLI

```bash
cmake -S . -B build -DNOXMQTT_BUILD_CLI_APP=ON
cmake --build build --config Debug
```

## Build the noxmqtt CLI with NoxTLS

```bash
cmake -S . -B build-noxtls \
  -DNOXMQTT_BUILD_CLI_APP=ON \
  -DNOXMQTT_CLI_ENABLE_NOXTLS=ON \
  -DNOXMQTT_NOXTLS_ROOT=/path/to/noxtls \
  -DNOXMQTT_NOXTLS_PROFILE=default

cmake --build build-noxtls --config Debug
```

This keeps NoxTLS outside the MQTT core target and links it only into the transport/app side.

## Recommended next steps

- [Quickstart](./start-here/quickstart)
- [Connect to a Broker](./start-here/connect-to-broker)
- [Secure Connections](./secure-connections)
- [ESP-IDF Integration](./esp-idf)
