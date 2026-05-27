---
sidebar_position: 2
description: Quickstart for building NoxMQTT and connecting to a broker.
---

# Quickstart

## 1. Build the library

```bash
cmake -S . -B build
cmake --build build --config Debug
```

## 2. Decide on transport

Pick one:

- plain TCP
- TLS through a NoxTLS-backed transport module

## 3. Configure the client

Set at minimum:

- broker host
- broker port
- protocol version
- client identifier

For secure MQTTS, also configure the TLS fields in the client config and link a TLS transport module.

## 4. Connect and process

Typical flow:

1. initialize the client
2. connect transport
3. send MQTT `CONNECT`
4. call process logic regularly or via the receive loop
5. publish/subscribe as needed

## Next

- [Connect to a Broker](./connect-to-broker)
- [Configure Certificates and TLS](./configure-certificates)
