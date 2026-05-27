---
description: Recent documentation-level release notes for NoxMQTT.
---

# Release Notes

## Current documentation refresh

This documentation set was repurposed from the earlier NoxTLS Docusaurus structure and rewritten to document NoxMQTT instead.

Key documentation changes:

- renamed the site identity to `NoxMQTT`
- rewrote the navigation around MQTT client concepts instead of TLS/crypto modules
- documented the split between core MQTT library and reusable transport modules
- documented ESP-IDF core-only component publishing with separate TCP and NoxTLS transport components
- documented the secure transport model where NoxTLS is selected by the application, not forced into the MQTT core

## Recent library direction reflected here

- deeper MQTT 5 support
- stronger protocol validation
- reusable Windows transport modules
- ESP-IDF transport component split
- registry-ready core component packaging
