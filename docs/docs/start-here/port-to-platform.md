---
sidebar_position: 6
description: Port NoxMQTT to a new platform.
---

# Port to Your Platform

Porting NoxMQTT is mostly transport work.

## Minimum porting target

Implement a transport that can:

- connect
- send
- receive
- disconnect
- notify the MQTT core on transport loss

## Recommended order

1. plain TCP port
2. reconnect stability
3. TLS port
4. MQTT 5 broker interop testing

Keep the TLS implementation inside the transport module so the core library stays reusable.
