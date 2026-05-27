---
sidebar_position: 1
description: What NoxMQTT is and how it differs from a TLS library.
---

# What Is NoxMQTT?

NoxMQTT is an embedded MQTT client library in C.

It is not a TLS library. Instead, it is designed to sit on top of a transport selected by the application.

## In one sentence

NoxMQTT handles MQTT. A transport module handles TCP or TLS.

## Why that split matters

It keeps the MQTT engine:

- portable
- easier to test
- reusable across platforms
- independent from any one TLS implementation

If your application needs secure broker connectivity, use a NoxTLS-backed transport module with NoxMQTT rather than pulling NoxTLS into the MQTT core.
