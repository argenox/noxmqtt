---
description: MQTT 3.1.1 and MQTT 5 coverage in NoxMQTT.
---

# MQTT Versions

NoxMQTT targets both MQTT 3.1.1 and MQTT 5.

## MQTT 3.1.1

Primary expectations:

- stable baseline interoperability
- QoS 0, 1, and 2
- session handling
- reconnect support

## MQTT 5

Primary expectations:

- properties and reason codes
- broker capability negotiation
- topic aliases
- session expiry
- enhanced auth support over time

## Practical guidance

Use MQTT 3.1.1 when you need the simplest interoperability target. Use MQTT 5 when you need richer broker signaling and are prepared to validate the full feature set against your broker matrix.
