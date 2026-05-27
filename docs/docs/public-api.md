---
description: Public API surface for NoxMQTT.
---

# Public API

The main public headers are:

- `noxmqtt.h`
- `noxmqtt_tal.h`

## `noxmqtt.h`

This header exposes:

- client configuration
- protocol version selection
- publish/subscribe APIs
- reconnect and disconnect APIs
- MQTT 5 property structures
- callback/event structures

## `noxmqtt_tal.h`

This header defines the transport boundary the platform or transport module must implement.

That boundary is what allows:

- reusable TCP transports
- reusable NoxTLS transports
- future platform-specific backends

## Documentation scope

This site currently documents the public API at the architectural level. For exact field and function details, read the header comments in the repository source while the dedicated MQTT-focused API reference is expanded.
