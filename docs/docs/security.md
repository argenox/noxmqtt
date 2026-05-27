---
description: Security guidance for NoxMQTT deployments.
---

# Security

NoxMQTT itself is an MQTT protocol engine. Secure deployment depends on how you pair it with transport security and broker policy.

## Recommended baseline

- use TLS for production broker connections
- verify the broker certificate
- verify hostname / server name when supported
- avoid anonymous or unauthenticated deployments
- use least-privilege broker credentials

## Core security boundaries

The MQTT core is responsible for:

- protocol correctness
- session handling
- MQTT 5 property validation
- preventing malformed packet acceptance where possible

The transport layer is responsible for:

- socket security
- TLS handshake
- certificate validation
- client certificate authentication when enabled

## MQTT 5 considerations

MQTT 5 adds more surface area than MQTT 3.1.1:

- reason codes
- properties
- topic aliases
- session expiry
- enhanced auth
- broker capability negotiation

Those features need strict validation to avoid ambiguous or unsafe state transitions.

## Operational guidance

- pin protocol version explicitly during rollout
- test against the real broker versions you ship with
- verify reconnect/session behavior under packet loss
- treat transport and broker auth failures as separate observability events
