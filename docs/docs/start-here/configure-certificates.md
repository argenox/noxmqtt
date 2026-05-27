---
sidebar_position: 4
description: Configure certificates and secure transport inputs for NoxMQTT deployments.
---

# Configure Certificates

Certificates matter only when your application uses a TLS-backed transport.

## Typical inputs

- CA certificate for broker verification
- optional client certificate
- optional client private key
- server name for SNI / hostname verification

## Important boundary

These values are carried through the MQTT client config, but they are consumed by the selected transport module, not by the MQTT protocol core.

## Current status

- CA-based server verification is part of the NoxTLS transport direction
- client certificate support is still transport-specific and not complete on every platform
