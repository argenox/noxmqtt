---
description: NoxMQTT architecture and layering.
---

# Architecture

NoxMQTT is organized around one boundary that matters most: the MQTT client core must not own the transport implementation.

## Layers

### MQTT core

The core library owns:

- packet encoding and decoding
- MQTT 3.1.1 and MQTT 5 state handling
- session state
- QoS handshake tracking
- reconnect and replay behavior
- broker capability tracking

Relevant source area:

- `src/noxmqtt-lib`

### Transport abstraction

The transport interface is defined through the platform transport hooks in `noxmqtt_tal.h`.

The core only expects a transport to provide:

- connect
- send
- disconnect
- receive dispatch
- time/debug support

That makes the same client core usable over:

- plain TCP
- NoxTLS-backed TLS
- future transports such as WebSocket or platform SDK sockets

### Reusable transport modules

Transport implementations should live outside the core library.

Current examples:

- Windows TCP transport
- Windows NoxTLS transport
- ESP-IDF TCP transport component
- ESP-IDF NoxTLS transport component

## Why TLS is not in the core

TLS is treated as a transport concern, not an MQTT concern.

That keeps:

- `noxmqtt` reusable without a hard dependency on any one TLS library
- application builds free to choose TCP-only or MQTTS
- NoxTLS integration localized to transport modules

This is the same architectural direction used by mature MQTT stacks that keep their security layer under the socket abstraction instead of inside MQTT packet code.
