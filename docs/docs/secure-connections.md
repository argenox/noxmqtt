---
description: How NoxMQTT handles secure MQTTS connections through transport modules.
---

# Secure Connections

NoxMQTT does not hard-wire TLS into the MQTT core.

## Model

The application selects a transport module that provides one of:

- plain TCP
- TLS over TCP using NoxTLS

The MQTT client then runs unchanged over that transport.

## Why this matters

This avoids:

- making `noxmqtt` depend directly on NoxTLS
- coupling every application to one TLS implementation
- mixing certificate logic into MQTT packet code

## NoxTLS integration model

The NoxTLS transport module is responsible for:

1. opening the socket
2. binding NoxTLS send/receive callbacks
3. performing the TLS handshake
4. exposing secure send/receive to the MQTT core

## Current status

- Windows NoxTLS transport: available
- ESP-IDF NoxTLS transport component: available
- ESP-IDF client cert/key support in that transport: still pending
