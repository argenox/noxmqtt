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
 * File: noxmqttlib.c
 * Summary: NoxMQTT Internal
*
*****************************************************************************/

#include <string.h>

#include "noxmqttlib.h"

/**
 * @brief Validates an MQTT client identifier string.
 *
 * @param[in] str Null-terminated client identifier string.
 *
 * @return 0 when the identifier is valid, otherwise `-1`.
 */
int noxmqttlib_validate_device_id(const char * str)
{
	size_t len = 0;

	if (str == NULL) {
		return -1;
	}

	len = strlen(str);
	if (len > MQTT_MAX_DEVICE_ID_LEN) {
		return -1;
	}

	return 0;
}
