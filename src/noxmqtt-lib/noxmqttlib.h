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
* File: noxmqttlib.h
* Summary: NoxMQTT Internal Library definitions
*
*****************************************************************************/

#ifndef _NOXMQTTLIB_H_
#define _NOXMQTTLIB_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "noxmqtt_err.h"
#include "noxmqtt_common.h"

/** Flag inidicating initialization */
#define NOXMQTT_INIT_FLAG 0x41474E58

#define MQTT_CONN_PROTOCOL_NAME_LEN   4
#define MQTT_CONN_DEFAULT_KEEPALIVE   60
#define MQTT_CONN_PROTOCOL_NAME       "MQTT"
#define MQTT_PROTO_LVL_VERSION_V3_1_1 4
#define MQTT_MAX_DEVICE_ID_LEN        65535
#define MAX_STR_LEN                   64
#define MQTT_LENGTH_FIELD_OFFSET      1
#define NOXMQTT_MAX_REMAIN_LEN_BYTES  4U
#define NOXMQTT_FIXED_HEADER_MAX_LEN  (1U + NOXMQTT_MAX_REMAIN_LEN_BYTES)

typedef enum
{
    NOXMQTT_CONNECTION_RC_ACCEPTED                = 0x00,
    NOXMQTT_CONNECTION_RC_REFUSED_UNACCP_PROT_VER = 0x01, /* Connection Refused, unacceptable protocol version */
    NOXMQTT_CONNECTION_RC_REFUSED_IDENT_REJECTED  = 0x02, /* Connection Refused, identifier rejected */
    NOXMQTT_CONNECTION_RC_REFUSED_SERVER_UNAVAIL  = 0x03, /* Connection Refused, Server Unavailable */
    NOXMQTT_CONNECTION_RC_REFUSED_BAD_USER_PASS   = 0x04, /* Connection Refused, Bad Username or Password */
    NOXMQTT_CONNECTION_RC_REFUSED_NOT_AUTH        = 0x05, /* Connection Refused, Not authorized */

} noxmqtt_connect_rc_t;


typedef enum
{
    NOXMQTT_DEBUG_LVL_NONE    = 0,
    NOXMQTT_DEBUG_LVL_ERROR   = 1,
    NOXMQTT_DEBUG_LVL_WARNING = 2,
    NOXMQTT_DEBUG_LVL_INFO    = 3,
    NOXMQTT_DEBUG_LVL_DEBUG   = 4,
    NOXMQTT_DEBUG_LVL_ALL     = 5,

} noxmqtt_debug_lvl_t;


#pragma pack(push, 1)
typedef struct {

    uint8_t name_len_msb;
    uint8_t name_len_lsb;
    char     name_val[MQTT_CONN_PROTOCOL_NAME_LEN];

    uint8_t level_val;

    /* Bitfield order is reversed */
    uint8_t flag_reserved : 1;
    uint8_t flag_clean_session : 1;
    uint8_t flag_will : 1;
    uint8_t flag_will_qos : 2;
    uint8_t flag_will_retain : 1;
    uint8_t flag_password : 1;
    uint8_t flag_user_name : 1;

    uint8_t keepalive_msb;
    uint8_t keepalive_lsb;

} noxmqtt_connect_var_hdr_t;

#pragma pack(pop)


#pragma pack(push, 1)
typedef struct {

    uint8_t name_len_msb;
    uint8_t name_len_lsb;
    char     name_val[MQTT_CONN_PROTOCOL_NAME_LEN];

    uint8_t level_val;

    uint8_t flag_user_name : 1;
    uint8_t flag_password  : 1;
    uint8_t flag_will_retain : 1;
    uint8_t flag_will_qos : 2;
    uint8_t flag_will : 1;
    uint8_t flag_clean_session : 1;
    uint8_t flag_reserved : 1;

    uint8_t keepalive_msb;
    uint8_t keepalive_lsb;

} noxmqtt_publish_var_hdr_t;

#pragma pack(pop)

#pragma pack(push, 1)
typedef struct {

    uint8_t flag_session_present: 1;
    uint8_t flag_user_name : 7;
    uint8_t conn_return_code;

} noxmqtt_connack_var_hdr_t;

#pragma pack(pop)


#pragma pack(push, 1)
typedef struct {

    uint8_t msb;
    uint8_t lsb;    

} noxmqtt_suback_var_hdr_t;

#pragma pack(pop)

#pragma pack(push, 1)
typedef struct {

    uint8_t msb;
    uint8_t lsb;

} noxmqtt_puback_var_hdr_t;

#pragma pack(pop)

#pragma pack(push, 1)
typedef struct {

    uint8_t msb;
    uint8_t lsb;    

} noxmqtt_unsuback_var_hdr_t;

#pragma pack(pop)


typedef union
{
    noxmqtt_connack_var_hdr_t  conn_ack;
    noxmqtt_suback_var_hdr_t   sub_ack;
    noxmqtt_unsuback_var_hdr_t unsub_ack;


} noxmqtt_response_var_hdr_t;



#pragma pack(push, 1)
typedef enum {

    NOXMQTT_CTRL_PKT_TYPE_RESERVED    = 0,  /* Reserved and must not be used */
    NOXMQTT_CTRL_PKT_TYPE_CONNECT     = 1,  /* MQTT CONNECT */
    NOXMQTT_CTRL_PKT_TYPE_CONNACK     = 2,  /* MQTT CONNACK */
    NOXMQTT_CTRL_PKT_TYPE_PUBLISH     = 3,  /* MQTT PUBLISH */
    NOXMQTT_CTRL_PKT_TYPE_PUBACK      = 4,  /* MQTT PUBACK */
    NOXMQTT_CTRL_PKT_TYPE_PUBREC      = 5,  /* MQTT Publish Received */
    NOXMQTT_CTRL_PKT_TYPE_PUBREL      = 6,  /* MQTT Publish Released */
    NOXMQTT_CTRL_PKT_TYPE_PUBCOMP     = 7,  /* MQTT Publish Complete */
    NOXMQTT_CTRL_PKT_TYPE_SUBSCRIBE   = 8,  /* MQTT SUBSCRIBE */
    NOXMQTT_CTRL_PKT_TYPE_SUBACK      = 9,  /* MQTT SUBACK */
    NOXMQTT_CTRL_PKT_TYPE_UNSUBSCRIBE = 10, /* MQTT UNSUBSCRIBE */
    NOXMQTT_CTRL_PKT_TYPE_UNSUBACK    = 11, /* MQTT UNSUBACK */
    NOXMQTT_CTRL_PKT_TYPE_PINGREQ     = 12, /* MQTT PINGREQ */
    NOXMQTT_CTRL_PKT_TYPE_PINGRESP    = 13, /* MQTT PINGRESP */
    NOXMQTT_CTRL_PKT_TYPE_DISCONNECT  = 14, /* MQTT DISCONNECT */
    NOXMQTT_CTRL_PKT_TYPE_AUTH        = 15, /* MQTT AUTH */

} noxmqtt_ctrl_pkt_type_t;
#pragma pack(pop)

/* Although the C standard does not enfoce bit fields, on platforms */
#pragma pack(push, 1)

typedef struct
{
    uint8_t retain               : 1;    
    uint8_t qos                  : 2;
    uint8_t dup                  : 1;
    uint8_t type : 4;

} _COMPILER_PACK noxmqtt_hdr_t;

#pragma pack(pop)


/**
 * @brief Validates an MQTT client identifier string.
 *
 * @param[in] str Null-terminated client identifier string.
 *
 * @return 0 when the identifier is valid, otherwise `-1`.
 */
extern int noxmqttlib_validate_device_id(const char* str);


#ifdef __cplusplus
}
#endif


#endif /* _NOXMQTTLIB_H_ */
