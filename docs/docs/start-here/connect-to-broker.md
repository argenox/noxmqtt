---
sidebar_position: 3
description: Connect NoxMQTT to a broker over TCP or MQTTS.
---

# Connect to a Broker

## TCP

Use a transport module that implements the generic transport hooks over plain sockets.

Set:

- broker address
- port `1883` or your broker's TCP listener
- protocol version

## MQTTS

Use a transport module that performs TLS before MQTT starts.

Set:

- broker address
- port `8883` or your broker's TLS listener
- CA certificate
- server name / SNI when required
- peer and hostname verification

## MQTT version choice

- MQTT 3.1.1: simplest interoperability target
- MQTT 5: richer broker capabilities and diagnostics
