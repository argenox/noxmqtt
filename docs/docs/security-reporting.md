---
description: How to report security issues in NoxMQTT.
---

# Security Reporting

If you find a security issue in NoxMQTT:

- do not open a public issue first for a live vulnerability
- contact Argenox privately at `info@argenox.com`
- include the affected version or commit
- include protocol version, transport mode, and broker context
- include a minimal reproduction if possible

Useful reports usually describe:

- whether the issue is in MQTT parsing, session handling, reconnect logic, or transport integration
- whether the issue affects MQTT 3.1.1, MQTT 5, or both
- whether TLS or plain TCP is involved
