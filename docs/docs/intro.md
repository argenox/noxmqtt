---
sidebar_position: 1
description: Introduction to NoxMQTT, an embedded MQTT 3.1.1 and MQTT 5 client library in C.
keywords:
  - noxmqtt
  - mqtt client
  - mqtt 5
  - mqtt 3.1.1
  - embedded c
---

# Introduction

:::tip New to NoxMQTT?
Start with [What is NoxMQTT?](./start-here/what-is-noxmqtt), then follow the [Quickstart](./start-here/quickstart) to build the library and connect to a broker.
:::

NoxMQTT is the documentation identity for the `noxmqtt` codebase: a transport-agnostic MQTT client library written in C for embedded and systems software.

The library is being extended toward full MQTT 3.1.1 and MQTT 5 support, with architecture aimed at feature parity and better integration flexibility than tightly platform-coupled clients.

## What NoxMQTT focuses on

- MQTT 3.1.1 and MQTT 5 protocol support
- A reusable client core that stays independent from TCP or TLS implementations
- Embedded-friendly integration with explicit platform transport hooks
- Secure transport through application-selected TLS backends such as NoxTLS
- ESP-IDF component packaging for the core library and transport modules

## Core design principles

| Area | NoxMQTT approach |
|------|------------------|
| Protocol engine | MQTT packet encode/decode, state machine, QoS flows, reconnect, MQTT 5 features |
| Transport | Separate transport abstraction, implemented per platform or per TLS backend |
| TLS | Kept out of the MQTT core; provided by reusable transport modules |
| Packaging | Core library can be used alone, or paired with TCP/TLS transports per application |

## Current documentation scope

This site now documents:

- how to build and integrate the NoxMQTT library
- how to select transport modules
- how secure MQTTS is added without making the MQTT core depend on NoxTLS
- how the ESP-IDF component story is split between core and transports
- the current feature matrix and roadmap focus

For the repository itself, see [Project](./project). For feature status, see [Feature Matrix](./documentation-parity-matrix).
