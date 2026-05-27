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
* File:    noxmqtt_tal.h
* Summary: NoxMQTT Transport Abstration Layer
*
*****************************************************************************/

#include <stdint.h>
#include "noxmqtt.h"

#ifndef _NOXMQTT_TAL_H_
#define _NOXMQTT_TAL_H_

#ifdef __cplusplus
extern "C" {
#endif

/* APIs which must be implemented by the target platform */

typedef void (*noxmqtt_transport_rcv_t)(noxmqtt_client_t* c, uint8_t * data, uint16_t len);


/**
 * @brief Initializes the platform transport layer.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] rcv_cback Receive callback invoked with incoming bytes.
 *
 * @return 0 on success, otherwise `-1`.
 */
extern int noxmqtt_transport_init(noxmqtt_client_t* c, noxmqtt_transport_rcv_t rcv_cback);

/**
 * @brief Connects the platform transport to the configured broker.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] conf Client connection configuration.
 *
 * @return 0 on success, otherwise `-1`.
 */
extern int noxmqtt_transport_connect(noxmqtt_client_t* c, const noxmqtt_client_conf_t* conf);

/**
 * @brief Sends raw bytes through the platform transport.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] data Buffer containing bytes to send.
 * @param[in] len Number of bytes to send.
 *
 * @return 0 on success, otherwise `-1`.
 */
extern int noxmqtt_transport_send(noxmqtt_client_t* c, const uint8_t * data, uint16_t len);

/**
 * @brief Transport receive thread entry point.
 *
 * @param[in] ptr Platform-specific transport context.
 *
 * @return 0 on success, otherwise `-1`.
 */
extern int noxmqtt_transport_receive_thread(void* ptr);

/**
 * @brief Disconnects the platform transport.
 *
 * @param[in] c NoxMQTT client instance.
 *
 * @return 0 on success, otherwise `-1`.
 */
extern int noxmqtt_transport_disconnect(noxmqtt_client_t* c);

/**
 * @brief Waits for the transport receive thread to terminate.
 */
extern void noxmqtt_transport_wait(void);

/**
 * @brief Gets the current platform time in milliseconds.
 *
 * @return Current monotonic time in milliseconds.
 */
extern uint32_t noxmqtt_tal_time_ms(void);

/**
 * @brief Prints a debug string using the platform logger.
 *
 * @param[in] str Null-terminated string to print.
 */
extern void noxmqtt_hal_debug_printf(const char* str);

/*
 * Backward-compatible aliases for the older TCP-oriented transport API names.
 * Keep these for existing ports while new code uses the generic transport names.
 */
#define noxmqtt_tcp_rcv_t             noxmqtt_transport_rcv_t
#define noxmqtt_tcp_init              noxmqtt_transport_init
#define noxmqtt_tcp_connect           noxmqtt_transport_connect
#define noxmqtt_tcp_send              noxmqtt_transport_send
#define noxmqtt_tcp_receive_thread    noxmqtt_transport_receive_thread
#define noxmqtt_tcp_disconnect        noxmqtt_transport_disconnect
#define noxmqtt_wait_thread           noxmqtt_transport_wait
#ifdef __cplusplus
}
#endif


#endif /* _NOXMQTT_TAL_H_ */
