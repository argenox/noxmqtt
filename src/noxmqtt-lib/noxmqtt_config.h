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
 * File: noxmqtt_config.h
 * Summary: NoxMQTT Library Configuration
*
*****************************************************************************/

#ifndef _NOXMQTT_CONFIG_H_
#define _NOXMQTT_CONFIG_H_

#include <stdint.h>

#ifdef ESP_PLATFORM
#include "sdkconfig.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Default buffer sizes used when the caller does not override them */
#ifdef CONFIG_NOXMQTT_TX_BUF_SIZE
#define NOXMQTT_TX_BUF_SIZE CONFIG_NOXMQTT_TX_BUF_SIZE
#else
#define NOXMQTT_TX_BUF_SIZE 1024
#endif

#ifdef CONFIG_NOXMQTT_RX_BUF_SIZE
#define NOXMQTT_RX_BUF_SIZE CONFIG_NOXMQTT_RX_BUF_SIZE
#else
#define NOXMQTT_RX_BUF_SIZE 4096
#endif

#ifdef CONFIG_NOXMQTT_MAX_SUBSCRIPTIONS
#define NOXMQTT_MAX_SUBSCRIPTIONS CONFIG_NOXMQTT_MAX_SUBSCRIPTIONS
#else
#define NOXMQTT_MAX_SUBSCRIPTIONS 16
#endif

#ifdef CONFIG_NOXMQTT_MAX_OUTBOX_MESSAGES
#define NOXMQTT_MAX_OUTBOX_MESSAGES CONFIG_NOXMQTT_MAX_OUTBOX_MESSAGES
#else
#define NOXMQTT_MAX_OUTBOX_MESSAGES 16
#endif

#ifdef CONFIG_NOXMQTT_MAX_TOPIC_ALIASES
#define NOXMQTT_MAX_TOPIC_ALIASES CONFIG_NOXMQTT_MAX_TOPIC_ALIASES
#else
#define NOXMQTT_MAX_TOPIC_ALIASES 16
#endif

#ifdef CONFIG_NOXMQTT_MAX_USER_PROPERTIES
#define NOXMQTT_MAX_USER_PROPERTIES CONFIG_NOXMQTT_MAX_USER_PROPERTIES
#else
#define NOXMQTT_MAX_USER_PROPERTIES 8
#endif

#ifdef CONFIG_NOXMQTT_MAX_SUBSCRIPTION_IDENTIFIERS
#define NOXMQTT_MAX_SUBSCRIPTION_IDENTIFIERS CONFIG_NOXMQTT_MAX_SUBSCRIPTION_IDENTIFIERS
#else
#define NOXMQTT_MAX_SUBSCRIPTION_IDENTIFIERS 8
#endif

#ifndef NOXMQTT_ENABLE_NOXTLS
#define NOXMQTT_ENABLE_NOXTLS 0
#endif

/* MQTT defaults */
#define NOXMQTT_DEFAULT_RECONNECT_TIMEOUT_MS 10000U
#define NOXMQTT_DEFAULT_NETWORK_TIMEOUT_MS   10000U


#ifdef __cplusplus
}
#endif

#endif /* _NOXMQTT_CONFIG_H_ */
