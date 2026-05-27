---
sidebar_position: 5
description: Use NoxMQTT as an ESP-IDF component with separate transports.
---

# ESP-IDF Component

Use the published core component for the MQTT library itself:

```yaml
dependencies:
  argenox/noxmqtt: "^1.0.0"
```

Then add the local transport component directory to your app:

```cmake
list(APPEND EXTRA_COMPONENT_DIRS
    "/path/to/noxmqtt/port/esp_idf/components"
)
```

If you want MQTTS through NoxTLS, also add:

```cmake
list(APPEND EXTRA_COMPONENT_DIRS
    "/path/to/noxtls/ports/esp-idf"
)
```

Link exactly one transport component into the application.
