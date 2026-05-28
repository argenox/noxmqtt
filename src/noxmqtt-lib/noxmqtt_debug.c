/*****************************************************************************
* Copyright (c) [2024] - [2026], Argenox Technologies LLC
* All rights reserved.
* SPDX-License-Identifier: GPL-2.0-only OR LicenseRef-Argenox-Commercial
*
*
* This file is part of the NoxMQTT Library.
*
* Licensed under the GNU General Public License v2.0 or later,
* or alternatively under a commercial license from
* Argenox Technologies LLC.
*
* See the LICENSE file in the project root for full details.
* CONTACT: info@argenox.com
*
*
* File:    noxmqtt_debug.c
* Summary: NoxMQTT Debug
*
*****************************************************************************/

#ifdef __cplusplus
extern "C" {
#endif

/* System Includes */
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>

/* Library Includes */
#include "noxmqtt.h"
#include "noxmqttlib.h"
#include "noxmqtt_err.h"
#include "noxmqtt_common.h"
#include "noxmqtt_tal.h"
#include "noxmqtt_debug.h"


/* MQTT Packet Types */
static const noxmqtt_item_desc_t mqtt_packet_type[] =
{
    {"MQTT_RESERVED", NOXMQTT_CTRL_PKT_TYPE_RESERVED},
    {"MQTT_CONNECT", NOXMQTT_CTRL_PKT_TYPE_CONNECT},
    {"MQTT_CONNACK", NOXMQTT_CTRL_PKT_TYPE_CONNACK},
    {"MQTT_PUBLISH", NOXMQTT_CTRL_PKT_TYPE_PUBLISH},
    {"MQTT_PUBACK", NOXMQTT_CTRL_PKT_TYPE_PUBACK},
    {"MQTT_PUBREC", NOXMQTT_CTRL_PKT_TYPE_PUBREC},
    {"MQTT_PUBREL", NOXMQTT_CTRL_PKT_TYPE_PUBREL},
    {"MQTT_PUBCOMP", NOXMQTT_CTRL_PKT_TYPE_PUBCOMP},
    {"MQTT_SUBSCRIBE", NOXMQTT_CTRL_PKT_TYPE_SUBSCRIBE},
    {"MQTT_SUBACK", NOXMQTT_CTRL_PKT_TYPE_SUBACK},
    {"MQTT_UNSUBSCRIBE", NOXMQTT_CTRL_PKT_TYPE_UNSUBSCRIBE},
    {"MQTT_UNSUBACK", NOXMQTT_CTRL_PKT_TYPE_UNSUBACK},
    {"MQTT_PINGREQ", NOXMQTT_CTRL_PKT_TYPE_PINGREQ},
    {"MQTT_PINGRESP", NOXMQTT_CTRL_PKT_TYPE_PINGRESP},
    {"MQTT_DISCONNECT", NOXMQTT_CTRL_PKT_TYPE_DISCONNECT},
    {"MQTT_AUTH", NOXMQTT_CTRL_PKT_TYPE_AUTH},
};


/**
 * @brief Gets the display string for an MQTT control packet type.
 *
 * @param[in] code MQTT control packet type code.
 *
 * @return Human-readable packet type string.
 */
char * get_mqtt_packet_type_str(int32_t code)
{
    int i = 0;
    for(i = 0; i < ARRAY_LEN(mqtt_packet_type); i++)
    {
        if(mqtt_packet_type[i].code == code){
            return (char *)mqtt_packet_type[i].name;
        }
    }

    return "";
}

/**
 * @brief Prints a buffer in hexadecimal form.
 *
 * @param[in] data Buffer to print.
 * @param[in] len Number of bytes to print.
 */
void print_buffer(const uint8_t* data, uint16_t len)
{    
    size_t i;
    printf("-----Data[%d]: ", len);
    for (i = 0; i < len; i++)
    {
        printf("%02X ", data[i]);
    }
    printf("\n");
}

/**
 * @brief Prints a formatted debug message when the level is enabled.
 *
 * @param[in] c NoxMQTT client instance that holds the debug level.
 * @param[in] lvl Debug level for the message.
 * @param[in] format Printf-style format string.
 */
void noxmqtt_debug_printf(const noxmqtt_client_t* c, noxmqtt_debug_lvl_t lvl, const char* format, ...)
{
    if (c != NULL && lvl <= c->debug_lvl) {
        char buffer[256];
        va_list args;
        va_start(args, format);
        vsnprintf(buffer, sizeof(buffer), format, args);
        va_end(args);

        /* Send to system print */
        noxmqtt_hal_debug_printf(buffer);
    }
}

#ifdef __cplusplus
}
#endif
