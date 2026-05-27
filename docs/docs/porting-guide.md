---
description: Porting NoxMQTT to new platforms and transports.
---

# Porting Guide

Porting NoxMQTT is primarily about implementing the transport layer correctly.

## What the core expects

The client core needs a transport that can:

- establish a connection
- send bytes reliably
- feed received bytes back into the client
- report disconnects
- provide a millisecond time source

## Porting strategy

### Plain TCP port

Implement a minimal transport first:

- create socket
- connect to broker
- start receive loop or receive task
- pass incoming bytes into `noxmqtt_process()` through the receive callback

### TLS port

Add TLS only after plain TCP is stable.

The recommended approach is:

1. open TCP socket
2. bind TLS library callbacks to that socket
3. perform TLS handshake
4. run MQTT over the encrypted stream

## ESP-IDF note

For ESP-IDF, keep the root `noxmqtt` component core-only and ship transport implementations as separate components. That avoids forcing all users to inherit a specific socket or TLS stack.

## Porting checklist

- use per-client transport context, not global connection state
- handle partial sends
- treat timeout as a normal process tick, not as a hard disconnect
- clear transport state on reconnect/disconnect
- keep TLS library ownership inside the transport module
