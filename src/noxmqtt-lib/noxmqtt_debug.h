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
* File:    noxmqtt_debug.h
* Summary: NoxMQTT External APIs
*
*****************************************************************************/

#ifndef _NOXMQTT_DEBUG_H_
#define _NOXMQTT_DEBUG_H_

#include <stdint.h>


#ifdef __cplusplus
extern "C" {
#endif

#include "noxmqtt_err.h"

#define DESC_MAX_NAME_LEN 64

typedef struct
{
    char name[DESC_MAX_NAME_LEN];
    int32_t code;
} noxmqtt_item_desc_t;

/**
 * @brief Gets the display string for an MQTT control packet type.
 *
 * @param[in] code MQTT control packet type code.
 *
 * @return Human-readable packet type string.
 */
extern char * get_mqtt_packet_type_str(int32_t code);

/**
 * @brief Prints a buffer in hexadecimal form.
 *
 * @param[in] data Buffer to print.
 * @param[in] len Number of bytes to print.
 */
extern void print_buffer(uint8_t* data, uint16_t len);

/**
 * @brief Prints a formatted debug message when the level is enabled.
 *
 * @param[in] c NoxMQTT client instance that holds the debug level.
 * @param[in] lvl Debug level for the message.
 * @param[in] format Printf-style format string.
 */
extern void noxmqtt_debug_printf(noxmqtt_client_t* c, noxmqtt_debug_lvl_t lvl, const char* format, ...);

#ifdef __cplusplus
}
#endif

#endif /* _NOXMQTT_DEBUG_H_ */
