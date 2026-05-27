---
description: Current NoxMQTT feature matrix and implementation focus.
---

# Feature Matrix

This page summarizes the direction of the library rather than claiming final parity everywhere.

## Protocol support

| Area | Status |
|------|--------|
| MQTT 3.1.1 connect/publish/subscribe flows | Implemented |
| MQTT 3.1.1 QoS 0/1/2 handling | Implemented |
| MQTT 5 connect/publish/subscribe core flows | Implemented |
| MQTT 5 property parsing and validation | In progress, substantial coverage added |
| MQTT 5 enhanced auth | Partial |
| Broker interoperability coverage | In progress |

## Transport model

| Area | Status |
|------|--------|
| Core library independent from TCP/TLS backend | Implemented |
| Reusable Windows TCP transport | Implemented |
| Reusable Windows NoxTLS transport | Implemented |
| ESP-IDF TCP transport component | Implemented |
| ESP-IDF NoxTLS transport component | Implemented |
| ESP-IDF client certificate auth through NoxTLS | Pending |

## Packaging

| Area | Status |
|------|--------|
| Root ESP-IDF core component | Implemented |
| Tag-release registry publishing | Implemented |
| Transport components outside the core package | Implemented |

## Main open work

- complete remaining MQTT 5 edge semantics and interop tests
- finish client certificate support in ESP-IDF NoxTLS transport
- continue tightening reason/property validation and broker-backed tests
