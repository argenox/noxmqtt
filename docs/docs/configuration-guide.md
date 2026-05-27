---
description: Configuration guide for NoxMQTT build options and runtime configuration.
---

# Configuration Guide

NoxMQTT configuration is split between build-time sizing and runtime client configuration.

## Build-time sizing

Common sizing controls include:

- transmit buffer size
- receive buffer size
- max cached subscriptions
- max queued/outbox messages
- max MQTT 5 topic aliases
- max stored MQTT 5 user properties

On ESP-IDF, transport-task sizing belongs to the transport component, not to the core component.

## Runtime client configuration

The main runtime configuration is carried by `noxmqtt_client_conf_t`.

Important groups:

- broker address and port
- transport mode
- reconnect timing
- username/password
- client identifier
- will topic and payload
- protocol version
- MQTT 5 connect properties

## TLS-related runtime configuration

TLS fields are exposed in the client config so applications can pass transport-level settings without forcing the core to depend on a TLS implementation.

Typical fields include:

- CA certificate
- client certificate
- client private key
- server name / SNI
- ALPN list
- peer verification
- hostname verification

Whether those fields are supported depends on the selected transport module.

## Recommended approach

Keep the configuration split clean:

- MQTT semantics in the MQTT client config
- TLS mechanics in the transport implementation
- platform task/socket settings in the platform transport component
