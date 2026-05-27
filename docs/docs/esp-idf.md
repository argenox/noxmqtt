---
description: ESP-IDF integration model for NoxMQTT.
---

# ESP-IDF

The ESP-IDF integration is intentionally split into a core component and separate transport components.

## Core component

Published component:

- `argenox/noxmqtt`

What it contains:

- MQTT client core only
- no socket code
- no NoxTLS dependency

## Local transport components

Transport components shipped in this repository:

- `noxmqtt_transport_espidf_tcp`
- `noxmqtt_transport_espidf_noxtls`

Your application should link:

- the core `noxmqtt` component
- exactly one transport component

## Why this split exists

It keeps the published MQTT component reusable for:

- plain TCP apps
- apps that want NoxTLS
- future apps that may want a different transport backend

See `port/esp_idf/README.md` in the repository for the local component layout used in the codebase.
