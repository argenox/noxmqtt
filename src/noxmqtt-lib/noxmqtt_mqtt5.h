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
* File:    noxmqtt_mqtt5.h
* Summary: NoxMQTT MQTT5 Definitions
*
*****************************************************************************/

#ifndef _NOXMQTT_MQTT5_H_
#define _NOXMQTT_MQTT5_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

#include "noxmqtt.h"

#define MQTT5_REASON_SUCCESS                              0x00U
#define MQTT5_REASON_NORMAL_DISCONNECTION                 0x00U
#define MQTT5_REASON_GRANTED_QOS0                         0x00U
#define MQTT5_REASON_GRANTED_QOS1                         0x01U
#define MQTT5_REASON_GRANTED_QOS2                         0x02U
#define MQTT5_REASON_DISCONNECT_WITH_WILL_MESSAGE         0x04U
#define MQTT5_REASON_NO_MATCHING_SUBSCRIBERS              0x10U
#define MQTT5_REASON_NO_SUBSCRIPTION_EXISTED              0x11U
#define MQTT5_REASON_CONTINUE_AUTHENTICATION              0x18U
#define MQTT5_REASON_REAUTHENTICATE                       0x19U
#define MQTT5_REASON_UNSPECIFIED_ERROR                    0x80U
#define MQTT5_REASON_MALFORMED_PACKET                     0x81U
#define MQTT5_REASON_PROTOCOL_ERROR                       0x82U
#define MQTT5_REASON_IMPLEMENTATION_SPECIFIC_ERROR        0x83U
#define MQTT5_REASON_UNSUPPORTED_PROTOCOL_VERSION         0x84U
#define MQTT5_REASON_CLIENT_IDENTIFIER_NOT_VALID          0x85U
#define MQTT5_REASON_BAD_USER_NAME_OR_PASSWORD            0x86U
#define MQTT5_REASON_NOT_AUTHORIZED                       0x87U
#define MQTT5_REASON_SERVER_UNAVAILABLE                   0x88U
#define MQTT5_REASON_SERVER_BUSY                          0x89U
#define MQTT5_REASON_BANNED                               0x8AU
#define MQTT5_REASON_SERVER_SHUTTING_DOWN                 0x8BU
#define MQTT5_REASON_BAD_AUTHENTICATION_METHOD            0x8CU
#define MQTT5_REASON_KEEP_ALIVE_TIMEOUT                   0x8DU
#define MQTT5_REASON_SESSION_TAKEN_OVER                   0x8EU
#define MQTT5_REASON_TOPIC_FILTER_INVALID                 0x8FU
#define MQTT5_REASON_TOPIC_NAME_INVALID                   0x90U
#define MQTT5_REASON_PACKET_IDENTIFIER_IN_USE             0x91U
#define MQTT5_REASON_PACKET_IDENTIFIER_NOT_FOUND          0x92U
#define MQTT5_REASON_RECEIVE_MAXIMUM_EXCEEDED             0x93U
#define MQTT5_REASON_TOPIC_ALIAS_INVALID                  0x94U
#define MQTT5_REASON_PACKET_TOO_LARGE                     0x95U
#define MQTT5_REASON_MESSAGE_RATE_TOO_HIGH                0x96U
#define MQTT5_REASON_QUOTA_EXCEEDED                       0x97U
#define MQTT5_REASON_ADMINISTRATIVE_ACTION                0x98U
#define MQTT5_REASON_PAYLOAD_FORMAT_INVALID               0x99U
#define MQTT5_REASON_RETAIN_NOT_SUPPORTED                 0x9AU
#define MQTT5_REASON_QOS_NOT_SUPPORTED                    0x9BU
#define MQTT5_REASON_USE_ANOTHER_SERVER                   0x9CU
#define MQTT5_REASON_SERVER_MOVED                         0x9DU
#define MQTT5_REASON_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED   0x9EU
#define MQTT5_REASON_CONNECTION_RATE_EXCEEDED             0x9FU
#define MQTT5_REASON_MAXIMUM_CONNECT_TIME                 0xA0U
#define MQTT5_REASON_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED 0xA1U
#define MQTT5_REASON_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED 0xA2U

#define MQTT5_PROPERTY_PAYLOAD_FORMAT_INDICATOR           0x01U
#define MQTT5_PROPERTY_MESSAGE_EXPIRY_INTERVAL            0x02U
#define MQTT5_PROPERTY_CONTENT_TYPE                       0x03U
#define MQTT5_PROPERTY_RESPONSE_TOPIC                     0x08U
#define MQTT5_PROPERTY_CORRELATION_DATA                   0x09U
#define MQTT5_PROPERTY_SUBSCRIPTION_IDENTIFIER            0x0BU
#define MQTT5_PROPERTY_SESSION_EXPIRY_INTERVAL            0x11U
#define MQTT5_PROPERTY_ASSIGNED_CLIENT_IDENTIFIER         0x12U
#define MQTT5_PROPERTY_SERVER_KEEP_ALIVE                  0x13U
#define MQTT5_PROPERTY_AUTH_METHOD                        0x15U
#define MQTT5_PROPERTY_AUTH_DATA                          0x16U
#define MQTT5_PROPERTY_REQUEST_PROBLEM_INFO               0x17U
#define MQTT5_PROPERTY_WILL_DELAY_INTERVAL                0x18U
#define MQTT5_PROPERTY_REQUEST_RESPONSE_INFO              0x19U
#define MQTT5_PROPERTY_RESPONSE_INFORMATION               0x1AU
#define MQTT5_PROPERTY_SERVER_REFERENCE                   0x1CU
#define MQTT5_PROPERTY_REASON_STRING                      0x1FU
#define MQTT5_PROPERTY_RECEIVE_MAXIMUM                    0x21U
#define MQTT5_PROPERTY_TOPIC_ALIAS_MAXIMUM                0x22U
#define MQTT5_PROPERTY_TOPIC_ALIAS                        0x23U
#define MQTT5_PROPERTY_MAXIMUM_QOS                        0x24U
#define MQTT5_PROPERTY_RETAIN_AVAILABLE                   0x25U
#define MQTT5_PROPERTY_USER_PROPERTY                      0x26U
#define MQTT5_PROPERTY_MAXIMUM_PACKET_SIZE                0x27U
#define MQTT5_PROPERTY_WILDCARD_SUBSCRIPTION_AVAILABLE    0x28U
#define MQTT5_PROPERTY_SUBSCRIPTION_IDENTIFIERS_AVAILABLE 0x29U
#define MQTT5_PROPERTY_SHARED_SUBSCRIPTION_AVAILABLE      0x2AU

typedef struct
{
    uint8_t has_payload_format_indicator;
    uint8_t payload_format_indicator;
    uint8_t has_message_expiry_interval;
    uint32_t message_expiry_interval;
    uint8_t has_will_delay_interval;
    uint32_t will_delay_interval;
    uint8_t has_content_type;
    const char* content_type;
    uint16_t content_type_len;
    uint8_t has_response_topic;
    const char* response_topic;
    uint16_t response_topic_len;
    uint8_t has_correlation_data;
    const uint8_t* correlation_data;
    uint16_t correlation_data_len;
    uint8_t has_subscription_identifier;
    uint32_t subscription_identifier;
    uint32_t subscription_identifiers[NOXMQTT_MAX_SUBSCRIPTION_IDENTIFIERS];
    uint16_t subscription_identifier_count;
    uint8_t has_session_expiry_interval;
    uint32_t session_expiry_interval;
    uint8_t has_assigned_client_identifier;
    const char* assigned_client_identifier;
    uint16_t assigned_client_identifier_len;
    uint8_t has_server_keep_alive;
    uint16_t server_keep_alive;
    uint8_t has_auth_method;
    const char* auth_method;
    uint16_t auth_method_len;
    uint8_t has_auth_data;
    const uint8_t* auth_data;
    uint16_t auth_data_len;
    uint8_t has_request_problem_info;
    uint8_t request_problem_info;
    uint8_t has_request_response_info;
    uint8_t request_response_info;
    uint8_t has_response_information;
    const char* response_information;
    uint16_t response_information_len;
    uint8_t has_server_reference;
    const char* server_reference;
    uint16_t server_reference_len;
    uint8_t has_reason_string;
    const char* reason_string;
    uint16_t reason_string_len;
    uint8_t has_receive_maximum;
    uint16_t receive_maximum;
    uint8_t has_topic_alias_maximum;
    uint16_t topic_alias_maximum;
    uint8_t has_topic_alias;
    uint16_t topic_alias;
    uint8_t has_maximum_qos;
    uint8_t maximum_qos;
    uint8_t has_retain_available;
    uint8_t retain_available;
    uint8_t has_maximum_packet_size;
    uint32_t maximum_packet_size;
    uint8_t has_wildcard_subscription_available;
    uint8_t wildcard_subscription_available;
    uint8_t has_subscription_identifiers_available;
    uint8_t subscription_identifiers_available;
    uint8_t has_shared_subscription_available;
    uint8_t shared_subscription_available;
    noxmqtt_mqtt5_user_property_t user_properties[NOXMQTT_MAX_USER_PROPERTIES];
    uint16_t user_property_count;
} noxmqtt_mqtt5_property_view_t;

typedef struct
{
    uint16_t packet_identifier;
    uint8_t reason_code;
    noxmqtt_mqtt5_property_view_t properties;
} noxmqtt_mqtt5_ack_view_t;

/**
 * @brief Encodes MQTT 5 CONNECT properties.
 *
 * @param[out] buffer Output buffer.
 * @param[in] buffer_len Output buffer length in bytes.
 * @param[in] conf Client configuration containing MQTT 5 settings.
 * @param[out] out_len Number of encoded bytes.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_mqtt5_encode_connect_properties(uint8_t* buffer,
                                                     uint16_t buffer_len,
                                                     const noxmqtt_client_conf_t* conf,
                                                     uint16_t* out_len);
/**
 * @brief Encodes MQTT 5 will properties.
 *
 * @param[out] buffer Output buffer.
 * @param[in] buffer_len Output buffer length in bytes.
 * @param[in] conf Client configuration containing will properties.
 * @param[out] out_len Number of encoded bytes.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_mqtt5_encode_will_properties(uint8_t* buffer,
                                                  uint16_t buffer_len,
                                                  const noxmqtt_client_conf_t* conf,
                                                  uint16_t* out_len);
/**
 * @brief Encodes MQTT 5 PUBLISH properties.
 *
 * @param[out] buffer Output buffer.
 * @param[in] buffer_len Output buffer length in bytes.
 * @param[in] props Publish properties to encode.
 * @param[out] out_len Number of encoded bytes.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_mqtt5_encode_publish_properties(uint8_t* buffer,
                                                     uint16_t buffer_len,
                                                     const noxmqtt_mqtt5_publish_props_t* props,
                                                     uint16_t* out_len);
/**
 * @brief Encodes MQTT 5 SUBSCRIBE properties.
 *
 * @param[out] buffer Output buffer.
 * @param[in] buffer_len Output buffer length in bytes.
 * @param[in] props Subscribe properties to encode.
 * @param[out] out_len Number of encoded bytes.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_mqtt5_encode_subscribe_properties(uint8_t* buffer,
                                                       uint16_t buffer_len,
                                                       const noxmqtt_mqtt5_subscribe_props_t* props,
                                                       uint16_t* out_len);
/**
 * @brief Encodes MQTT 5 DISCONNECT properties.
 *
 * @param[out] buffer Output buffer.
 * @param[in] buffer_len Output buffer length in bytes.
 * @param[in] props Disconnect properties to encode.
 * @param[out] out_len Number of encoded bytes.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_mqtt5_encode_disconnect_properties(uint8_t* buffer,
                                                        uint16_t buffer_len,
                                                        const noxmqtt_mqtt5_disconnect_props_t* props,
                                                        uint16_t* out_len);
/**
 * @brief Encodes MQTT 5 AUTH properties.
 *
 * @param[out] buffer Output buffer.
 * @param[in] buffer_len Output buffer length in bytes.
 * @param[in] props AUTH properties to encode.
 * @param[out] out_len Number of encoded bytes.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_mqtt5_encode_auth_properties(uint8_t* buffer,
                                                  uint16_t buffer_len,
                                                  const noxmqtt_mqtt5_auth_props_t* props,
                                                  uint16_t* out_len);
/**
 * @brief Parses an MQTT 5 property block.
 *
 * @param[in] data Packet buffer.
 * @param[in] len Packet buffer length in bytes.
 * @param[in,out] offset Current parse offset and resulting offset.
 * @param[out] props Parsed property view.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_mqtt5_parse_properties(const uint8_t* data,
                                            uint16_t len,
                                            uint16_t* offset,
                                            noxmqtt_mqtt5_property_view_t* props);
/**
 * @brief Skips an MQTT 5 property block.
 *
 * @param[in] data Packet buffer.
 * @param[in] len Packet buffer length in bytes.
 * @param[in,out] offset Current parse offset and resulting offset.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_mqtt5_skip_properties(const uint8_t* data,
                                           uint16_t len,
                                           uint16_t* offset);
/**
 * @brief Parses an MQTT 5 ACK-style packet body.
 *
 * @param[in] data Packet buffer.
 * @param[in] len Packet buffer length in bytes.
 * @param[in] default_reason_code Reason code to use when the packet omits one.
 * @param[out] ack Parsed ACK view.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_mqtt5_parse_ack(const uint8_t* data,
                                     uint16_t len,
                                     uint8_t default_reason_code,
                                     noxmqtt_mqtt5_ack_view_t* ack);
/**
 * @brief Validates property usage for a specific MQTT packet type.
 *
 * @param[in] packet_type MQTT control packet type.
 * @param[in] props Parsed property view.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_mqtt5_validate_properties(noxmqtt_ctrl_pkt_type_t packet_type,
                                               const noxmqtt_mqtt5_property_view_t* props);
/**
 * @brief Checks whether a reason code is valid for a packet type.
 *
 * @param[in] packet_type MQTT control packet type.
 * @param[in] reason_code Reason code to validate.
 *
 * @return Non-zero when valid, otherwise zero.
 */
uint8_t noxmqtt_mqtt5_is_valid_reason_code(noxmqtt_ctrl_pkt_type_t packet_type, uint8_t reason_code);
/**
 * @brief Checks whether a list of reason codes is valid for a packet type.
 *
 * @param[in] packet_type MQTT control packet type.
 * @param[in] reason_codes Reason code array.
 * @param[in] reason_code_count Number of reason codes in the array.
 *
 * @return Non-zero when valid, otherwise zero.
 */
uint8_t noxmqtt_mqtt5_are_valid_reason_codes(noxmqtt_ctrl_pkt_type_t packet_type,
                                             const uint8_t* reason_codes,
                                             uint16_t reason_code_count);

#ifdef __cplusplus
}
#endif

#endif
