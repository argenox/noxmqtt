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
* Summary: NoxMQTT MQTT5 Implementation
*
*****************************************************************************/

#include <string.h>

#include "noxmqtt_common.h"
#include "noxmqtt_mqtt5.h"

static noxmqtt_rc_t mqtt5_write_bytes(uint8_t* buffer,
                                      uint16_t buffer_len,
                                      uint16_t* offset,
                                      const uint8_t* data,
                                      uint16_t data_len);
static noxmqtt_rc_t mqtt5_write_u16(uint8_t* buffer, uint16_t buffer_len, uint16_t* offset, uint16_t value);
static noxmqtt_rc_t mqtt5_write_u32(uint8_t* buffer, uint16_t buffer_len, uint16_t* offset, uint32_t value);
static noxmqtt_rc_t mqtt5_write_varint(uint8_t* buffer, uint16_t buffer_len, uint16_t* offset, uint32_t value);
static noxmqtt_rc_t mqtt5_write_utf8_string(uint8_t* buffer,
                                            uint16_t buffer_len,
                                            uint16_t* offset,
                                            const char* str);
static noxmqtt_rc_t mqtt5_write_binary(uint8_t* buffer,
                                       uint16_t buffer_len,
                                       uint16_t* offset,
                                       const uint8_t* data,
                                       uint16_t data_len);
static noxmqtt_rc_t mqtt5_write_user_properties(uint8_t* buffer,
                                                uint16_t buffer_len,
                                                uint16_t* offset,
                                                const noxmqtt_mqtt5_user_property_t* props,
                                                uint16_t count);
static noxmqtt_rc_t mqtt5_read_varint(const uint8_t* data,
                                      uint16_t len,
                                      uint16_t* offset,
                                      uint32_t* value);
static noxmqtt_rc_t mqtt5_read_u16(const uint8_t* data, uint16_t len, uint16_t* offset, uint16_t* value);
static noxmqtt_rc_t mqtt5_read_u32(const uint8_t* data, uint16_t len, uint16_t* offset, uint32_t* value);
static noxmqtt_rc_t mqtt5_read_utf8_string(const uint8_t* data,
                                           uint16_t len,
                                           uint16_t* offset,
                                           const char** str,
                                           uint16_t* str_len);
static noxmqtt_rc_t mqtt5_read_binary(const uint8_t* data,
                                      uint16_t len,
                                      uint16_t* offset,
                                      const uint8_t** value,
                                      uint16_t* value_len);

/**
 * @brief Encodes MQTT 5 CONNECT properties into a buffer.
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
                                                     uint16_t* out_len)
{
    uint16_t offset = 0;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    if (buffer == NULL || conf == NULL || out_len == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (conf->mqtt5.session_expiry_interval > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_SESSION_EXPIRY_INTERVAL;
        rc = mqtt5_write_u32(buffer, buffer_len, &offset, conf->mqtt5.session_expiry_interval);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (conf->mqtt5.receive_maximum > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_RECEIVE_MAXIMUM;
        rc = mqtt5_write_u16(buffer, buffer_len, &offset, conf->mqtt5.receive_maximum);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (conf->mqtt5.topic_alias_maximum > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_TOPIC_ALIAS_MAXIMUM;
        rc = mqtt5_write_u16(buffer, buffer_len, &offset, conf->mqtt5.topic_alias_maximum);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (conf->mqtt5.maximum_packet_size > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_MAXIMUM_PACKET_SIZE;
        rc = mqtt5_write_u32(buffer, buffer_len, &offset, conf->mqtt5.maximum_packet_size);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    buffer[offset++] = MQTT5_PROPERTY_REQUEST_RESPONSE_INFO;
    rc = mqtt5_write_bytes(buffer, buffer_len, &offset, &conf->mqtt5.request_response_info, 1U);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    buffer[offset++] = MQTT5_PROPERTY_REQUEST_PROBLEM_INFO;
    rc = mqtt5_write_bytes(buffer, buffer_len, &offset, &conf->mqtt5.request_problem_info, 1U);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    if (conf->mqtt5.auth_method != NULL && conf->mqtt5.auth_method[0] != '\0') {
        buffer[offset++] = MQTT5_PROPERTY_AUTH_METHOD;
        rc = mqtt5_write_utf8_string(buffer, buffer_len, &offset, conf->mqtt5.auth_method);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (conf->mqtt5.auth_data != NULL && conf->mqtt5.auth_data_len > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_AUTH_DATA;
        rc = mqtt5_write_binary(buffer, buffer_len, &offset, conf->mqtt5.auth_data, conf->mqtt5.auth_data_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    rc = mqtt5_write_user_properties(buffer,
                                     buffer_len,
                                     &offset,
                                     conf->mqtt5.user_properties,
                                     conf->mqtt5.user_property_count);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    *out_len = offset;
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Encodes MQTT 5 will properties into a buffer.
 *
 * @param[out] buffer Output buffer.
 * @param[in] buffer_len Output buffer length in bytes.
 * @param[in] conf Client configuration containing will settings.
 * @param[out] out_len Number of encoded bytes.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_mqtt5_encode_will_properties(uint8_t* buffer,
                                                  uint16_t buffer_len,
                                                  const noxmqtt_client_conf_t* conf,
                                                  uint16_t* out_len)
{
    uint16_t offset = 0;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    if (buffer == NULL || conf == NULL || out_len == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (conf->will_topic.mqtt5.will_delay_interval > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_WILL_DELAY_INTERVAL;
        rc = mqtt5_write_u32(buffer, buffer_len, &offset, conf->will_topic.mqtt5.will_delay_interval);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (conf->will_topic.mqtt5.payload_format_indicator > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_PAYLOAD_FORMAT_INDICATOR;
        rc = mqtt5_write_bytes(buffer,
                               buffer_len,
                               &offset,
                               &conf->will_topic.mqtt5.payload_format_indicator,
                               1U);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (conf->will_topic.mqtt5.message_expiry_interval > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_MESSAGE_EXPIRY_INTERVAL;
        rc = mqtt5_write_u32(buffer, buffer_len, &offset, conf->will_topic.mqtt5.message_expiry_interval);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (conf->will_topic.mqtt5.response_topic != NULL && conf->will_topic.mqtt5.response_topic[0] != '\0') {
        buffer[offset++] = MQTT5_PROPERTY_RESPONSE_TOPIC;
        rc = mqtt5_write_utf8_string(buffer, buffer_len, &offset, conf->will_topic.mqtt5.response_topic);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (conf->will_topic.mqtt5.correlation_data != NULL && conf->will_topic.mqtt5.correlation_data_len > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_CORRELATION_DATA;
        rc = mqtt5_write_binary(buffer,
                                buffer_len,
                                &offset,
                                conf->will_topic.mqtt5.correlation_data,
                                conf->will_topic.mqtt5.correlation_data_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (conf->will_topic.mqtt5.content_type != NULL && conf->will_topic.mqtt5.content_type[0] != '\0') {
        buffer[offset++] = MQTT5_PROPERTY_CONTENT_TYPE;
        rc = mqtt5_write_utf8_string(buffer, buffer_len, &offset, conf->will_topic.mqtt5.content_type);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    rc = mqtt5_write_user_properties(buffer,
                                     buffer_len,
                                     &offset,
                                     conf->will_topic.mqtt5.user_properties,
                                     conf->will_topic.mqtt5.user_property_count);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    *out_len = offset;
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Encodes MQTT 5 PUBLISH properties into a buffer.
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
                                                     uint16_t* out_len)
{
    uint16_t offset = 0;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    if (buffer == NULL || out_len == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (props == NULL) {
        *out_len = 0;
        return NOXMQTT_SUCCESS;
    }

    if (props->payload_format_indicator > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_PAYLOAD_FORMAT_INDICATOR;
        rc = mqtt5_write_bytes(buffer, buffer_len, &offset, &props->payload_format_indicator, 1U);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (props->message_expiry_interval > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_MESSAGE_EXPIRY_INTERVAL;
        rc = mqtt5_write_u32(buffer, buffer_len, &offset, props->message_expiry_interval);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (props->topic_alias > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_TOPIC_ALIAS;
        rc = mqtt5_write_u16(buffer, buffer_len, &offset, props->topic_alias);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (props->response_topic != NULL && props->response_topic[0] != '\0') {
        buffer[offset++] = MQTT5_PROPERTY_RESPONSE_TOPIC;
        rc = mqtt5_write_utf8_string(buffer, buffer_len, &offset, props->response_topic);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (props->correlation_data != NULL && props->correlation_data_len > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_CORRELATION_DATA;
        rc = mqtt5_write_binary(buffer, buffer_len, &offset, props->correlation_data, props->correlation_data_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (props->content_type != NULL && props->content_type[0] != '\0') {
        buffer[offset++] = MQTT5_PROPERTY_CONTENT_TYPE;
        rc = mqtt5_write_utf8_string(buffer, buffer_len, &offset, props->content_type);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    rc = mqtt5_write_user_properties(buffer, buffer_len, &offset, props->user_properties, props->user_property_count);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    *out_len = offset;
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Encodes MQTT 5 SUBSCRIBE properties into a buffer.
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
                                                       uint16_t* out_len)
{
    uint16_t offset = 0;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    if (buffer == NULL || out_len == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (props == NULL) {
        *out_len = 0;
        return NOXMQTT_SUCCESS;
    }

    if (props->subscribe_identifier > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_SUBSCRIPTION_IDENTIFIER;
        rc = mqtt5_write_varint(buffer, buffer_len, &offset, props->subscribe_identifier);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    rc = mqtt5_write_user_properties(buffer, buffer_len, &offset, props->user_properties, props->user_property_count);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    *out_len = offset;
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Encodes MQTT 5 DISCONNECT properties into a buffer.
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
                                                        uint16_t* out_len)
{
    uint16_t offset = 0;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    if (buffer == NULL || out_len == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (props == NULL) {
        *out_len = 0;
        return NOXMQTT_SUCCESS;
    }

    if (props->session_expiry_interval > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_SESSION_EXPIRY_INTERVAL;
        rc = mqtt5_write_u32(buffer, buffer_len, &offset, props->session_expiry_interval);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (props->reason_string != NULL && props->reason_string[0] != '\0') {
        buffer[offset++] = MQTT5_PROPERTY_REASON_STRING;
        rc = mqtt5_write_utf8_string(buffer, buffer_len, &offset, props->reason_string);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (props->server_reference != NULL && props->server_reference[0] != '\0') {
        buffer[offset++] = MQTT5_PROPERTY_SERVER_REFERENCE;
        rc = mqtt5_write_utf8_string(buffer, buffer_len, &offset, props->server_reference);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    rc = mqtt5_write_user_properties(buffer, buffer_len, &offset, props->user_properties, props->user_property_count);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    *out_len = offset;
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Encodes MQTT 5 AUTH properties into a buffer.
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
                                                  uint16_t* out_len)
{
    uint16_t offset = 0;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    if (buffer == NULL || out_len == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (props == NULL) {
        *out_len = 0;
        return NOXMQTT_SUCCESS;
    }

    if (props->auth_method != NULL && props->auth_method[0] != '\0') {
        buffer[offset++] = MQTT5_PROPERTY_AUTH_METHOD;
        rc = mqtt5_write_utf8_string(buffer, buffer_len, &offset, props->auth_method);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (props->auth_data != NULL && props->auth_data_len > 0U) {
        buffer[offset++] = MQTT5_PROPERTY_AUTH_DATA;
        rc = mqtt5_write_binary(buffer, buffer_len, &offset, props->auth_data, props->auth_data_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }
    if (props->reason_string != NULL && props->reason_string[0] != '\0') {
        buffer[offset++] = MQTT5_PROPERTY_REASON_STRING;
        rc = mqtt5_write_utf8_string(buffer, buffer_len, &offset, props->reason_string);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    rc = mqtt5_write_user_properties(buffer, buffer_len, &offset, props->user_properties, props->user_property_count);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    *out_len = offset;
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Parses an MQTT 5 property block from a packet buffer.
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
                                            noxmqtt_mqtt5_property_view_t* props)
{
    uint32_t props_len = 0;
    uint16_t props_end = 0;

    if (data == NULL || offset == NULL || props == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    MEMZERO_S(*props);

    if (*offset > len) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    if (mqtt5_read_varint(data, len, offset, &props_len) != NOXMQTT_SUCCESS) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    if ((uint32_t)*offset + props_len > len) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    props_end = (uint16_t)(*offset + (uint16_t)props_len);
    while (*offset < props_end) {
        uint8_t property_id = data[(*offset)++];

        switch (property_id) {
            case MQTT5_PROPERTY_PAYLOAD_FORMAT_INDICATOR:
                if (props->has_payload_format_indicator) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                if (*offset >= props_end) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_payload_format_indicator = 1;
                props->payload_format_indicator = data[(*offset)++];
                break;
            case MQTT5_PROPERTY_MESSAGE_EXPIRY_INTERVAL:
                if (props->has_message_expiry_interval) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_message_expiry_interval = 1;
                if (mqtt5_read_u32(data, props_end, offset, &props->message_expiry_interval) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_CONTENT_TYPE:
                if (props->has_content_type) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_content_type = 1;
                if (mqtt5_read_utf8_string(data, props_end, offset, &props->content_type, &props->content_type_len) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_RESPONSE_TOPIC:
                if (props->has_response_topic) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_response_topic = 1;
                if (mqtt5_read_utf8_string(data, props_end, offset, &props->response_topic, &props->response_topic_len) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_CORRELATION_DATA:
                if (props->has_correlation_data) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_correlation_data = 1;
                if (mqtt5_read_binary(data, props_end, offset, &props->correlation_data, &props->correlation_data_len) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_SUBSCRIPTION_IDENTIFIER:
            {
                uint32_t subscription_identifier = 0;

                if (mqtt5_read_varint(data, props_end, offset, &subscription_identifier) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                if (props->subscription_identifier_count >= NOXMQTT_MAX_SUBSCRIPTION_IDENTIFIERS) {
                    return NOXMQTT_RC_ERROR_OVERFLOW;
                }
                props->has_subscription_identifier = 1;
                if (props->subscription_identifier_count == 0U) {
                    props->subscription_identifier = subscription_identifier;
                }
                props->subscription_identifiers[props->subscription_identifier_count++] = subscription_identifier;
                break;
            }
            case MQTT5_PROPERTY_SESSION_EXPIRY_INTERVAL:
                if (props->has_session_expiry_interval) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_session_expiry_interval = 1;
                if (mqtt5_read_u32(data, props_end, offset, &props->session_expiry_interval) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_ASSIGNED_CLIENT_IDENTIFIER:
                if (props->has_assigned_client_identifier) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_assigned_client_identifier = 1;
                if (mqtt5_read_utf8_string(data,
                                           props_end,
                                           offset,
                                           &props->assigned_client_identifier,
                                           &props->assigned_client_identifier_len) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_SERVER_KEEP_ALIVE:
                if (props->has_server_keep_alive) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_server_keep_alive = 1;
                if (mqtt5_read_u16(data, props_end, offset, &props->server_keep_alive) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_AUTH_METHOD:
                if (props->has_auth_method) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_auth_method = 1;
                if (mqtt5_read_utf8_string(data, props_end, offset, &props->auth_method, &props->auth_method_len) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_AUTH_DATA:
                if (props->has_auth_data) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_auth_data = 1;
                if (mqtt5_read_binary(data, props_end, offset, &props->auth_data, &props->auth_data_len) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_REQUEST_PROBLEM_INFO:
            case MQTT5_PROPERTY_REQUEST_RESPONSE_INFO:
            case MQTT5_PROPERTY_MAXIMUM_QOS:
            case MQTT5_PROPERTY_RETAIN_AVAILABLE:
            case MQTT5_PROPERTY_WILDCARD_SUBSCRIPTION_AVAILABLE:
            case MQTT5_PROPERTY_SUBSCRIPTION_IDENTIFIERS_AVAILABLE:
            case MQTT5_PROPERTY_SHARED_SUBSCRIPTION_AVAILABLE:
                if (*offset >= props_end) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                switch (property_id) {
                    case MQTT5_PROPERTY_REQUEST_PROBLEM_INFO:
                        if (props->has_request_problem_info) {
                            return NOXMQTT_RC_ERROR_BAD_PACKET;
                        }
                        props->has_request_problem_info = 1;
                        props->request_problem_info = data[*offset];
                        break;
                    case MQTT5_PROPERTY_REQUEST_RESPONSE_INFO:
                        if (props->has_request_response_info) {
                            return NOXMQTT_RC_ERROR_BAD_PACKET;
                        }
                        props->has_request_response_info = 1;
                        props->request_response_info = data[*offset];
                        break;
                    case MQTT5_PROPERTY_MAXIMUM_QOS:
                        if (props->has_maximum_qos) {
                            return NOXMQTT_RC_ERROR_BAD_PACKET;
                        }
                        props->has_maximum_qos = 1;
                        props->maximum_qos = data[*offset];
                        break;
                    case MQTT5_PROPERTY_RETAIN_AVAILABLE:
                        if (props->has_retain_available) {
                            return NOXMQTT_RC_ERROR_BAD_PACKET;
                        }
                        props->has_retain_available = 1;
                        props->retain_available = data[*offset];
                        break;
                    case MQTT5_PROPERTY_WILDCARD_SUBSCRIPTION_AVAILABLE:
                        if (props->has_wildcard_subscription_available) {
                            return NOXMQTT_RC_ERROR_BAD_PACKET;
                        }
                        props->has_wildcard_subscription_available = 1;
                        props->wildcard_subscription_available = data[*offset];
                        break;
                    case MQTT5_PROPERTY_SUBSCRIPTION_IDENTIFIERS_AVAILABLE:
                        if (props->has_subscription_identifiers_available) {
                            return NOXMQTT_RC_ERROR_BAD_PACKET;
                        }
                        props->has_subscription_identifiers_available = 1;
                        props->subscription_identifiers_available = data[*offset];
                        break;
                    case MQTT5_PROPERTY_SHARED_SUBSCRIPTION_AVAILABLE:
                        if (props->has_shared_subscription_available) {
                            return NOXMQTT_RC_ERROR_BAD_PACKET;
                        }
                        props->has_shared_subscription_available = 1;
                        props->shared_subscription_available = data[*offset];
                        break;
                    default:
                        break;
                }
                (*offset)++;
                break;
            case MQTT5_PROPERTY_RESPONSE_INFORMATION:
                if (props->has_response_information) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_response_information = 1;
                if (mqtt5_read_utf8_string(data,
                                           props_end,
                                           offset,
                                           &props->response_information,
                                           &props->response_information_len) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_SERVER_REFERENCE:
                if (props->has_server_reference) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_server_reference = 1;
                if (mqtt5_read_utf8_string(data,
                                           props_end,
                                           offset,
                                           &props->server_reference,
                                           &props->server_reference_len) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_REASON_STRING:
                if (props->has_reason_string) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_reason_string = 1;
                if (mqtt5_read_utf8_string(data, props_end, offset, &props->reason_string, &props->reason_string_len) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_RECEIVE_MAXIMUM:
                if (props->has_receive_maximum) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_receive_maximum = 1;
                if (mqtt5_read_u16(data, props_end, offset, &props->receive_maximum) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_TOPIC_ALIAS_MAXIMUM:
                if (props->has_topic_alias_maximum) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_topic_alias_maximum = 1;
                if (mqtt5_read_u16(data, props_end, offset, &props->topic_alias_maximum) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_TOPIC_ALIAS:
                if (props->has_topic_alias) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_topic_alias = 1;
                if (mqtt5_read_u16(data, props_end, offset, &props->topic_alias) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_USER_PROPERTY:
            {
                const char* name = NULL;
                const char* value = NULL;
                uint16_t name_len = 0;
                uint16_t value_len = 0;

                if (mqtt5_read_utf8_string(data, props_end, offset, &name, &name_len) != NOXMQTT_SUCCESS ||
                    mqtt5_read_utf8_string(data, props_end, offset, &value, &value_len) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }

                if (props->user_property_count < NOXMQTT_MAX_USER_PROPERTIES) {
                    noxmqtt_mqtt5_user_property_t* user_prop = &props->user_properties[props->user_property_count++];
                    user_prop->name = name;
                    user_prop->name_len = name_len;
                    user_prop->value = value;
                    user_prop->value_len = value_len;
                }
                break;
            }
            case MQTT5_PROPERTY_MAXIMUM_PACKET_SIZE:
                if (props->has_maximum_packet_size) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_maximum_packet_size = 1;
                if (mqtt5_read_u32(data, props_end, offset, &props->maximum_packet_size) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            case MQTT5_PROPERTY_WILL_DELAY_INTERVAL:
                if (props->has_will_delay_interval) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                props->has_will_delay_interval = 1;
                if (mqtt5_read_u32(data, props_end, offset, &props->will_delay_interval) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                break;
            default:
                return NOXMQTT_RC_ERROR_NOT_SUPPORTED;
        }
    }

    return NOXMQTT_SUCCESS;
}

/**
 * @brief Skips over an MQTT 5 property block.
 *
 * @param[in] data Packet buffer.
 * @param[in] len Packet buffer length in bytes.
 * @param[in,out] offset Current parse offset and resulting offset.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_mqtt5_skip_properties(const uint8_t* data,
                                           uint16_t len,
                                           uint16_t* offset)
{
    uint32_t props_len = 0;

    if (data == NULL || offset == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (mqtt5_read_varint(data, len, offset, &props_len) != NOXMQTT_SUCCESS) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    if ((uint32_t)*offset + props_len > len) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    *offset = (uint16_t)(*offset + (uint16_t)props_len);
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Parses an MQTT 5 acknowledgement packet body.
 *
 * @param[in] data Packet buffer.
 * @param[in] len Packet buffer length in bytes.
 * @param[in] default_reason_code Reason code to use when omitted.
 * @param[out] ack Parsed acknowledgement view.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_mqtt5_parse_ack(const uint8_t* data,
                                     uint16_t len,
                                     uint8_t default_reason_code,
                                     noxmqtt_mqtt5_ack_view_t* ack)
{
    uint32_t remain_length = 0;
    uint16_t offset = 1U;
    uint16_t body_end = 0;

    if (data == NULL || ack == NULL || len < 4U) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    MEMZERO_S(*ack);
    ack->reason_code = default_reason_code;

    if (mqtt5_read_varint(data, len, &offset, &remain_length) != NOXMQTT_SUCCESS ||
        remain_length < 2U ||
        (uint32_t)offset + remain_length > len) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    body_end = (uint16_t)(offset + (uint16_t)remain_length);
    if (mqtt5_read_u16(data, body_end, &offset, &ack->packet_identifier) != NOXMQTT_SUCCESS) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    if (offset < body_end) {
        ack->reason_code = data[offset++];
    }
    if (offset < body_end) {
        if (noxmqtt_mqtt5_parse_properties(data, body_end, &offset, &ack->properties) != NOXMQTT_SUCCESS) {
            return NOXMQTT_RC_ERROR_BAD_PACKET;
        }
    }

    if (offset != body_end) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    return NOXMQTT_SUCCESS;
}

/**
 * @brief Validates MQTT 5 property usage for a packet type.
 *
 * @param[in] packet_type MQTT control packet type.
 * @param[in] props Parsed property view.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_mqtt5_validate_properties(noxmqtt_ctrl_pkt_type_t packet_type,
                                               const noxmqtt_mqtt5_property_view_t* props)
{
    if (props == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    switch (packet_type) {
        case NOXMQTT_CTRL_PKT_TYPE_CONNACK:
            if (props->has_payload_format_indicator ||
                props->has_message_expiry_interval ||
                props->has_will_delay_interval ||
                props->has_content_type ||
                props->has_response_topic ||
                props->has_correlation_data ||
                props->has_subscription_identifier ||
                props->has_request_problem_info ||
                props->has_request_response_info ||
                props->has_topic_alias) {
                return NOXMQTT_RC_ERROR_BAD_PACKET;
            }
            break;
        case NOXMQTT_CTRL_PKT_TYPE_PUBLISH:
            if (props->has_session_expiry_interval ||
                props->has_will_delay_interval ||
                props->has_assigned_client_identifier ||
                props->has_server_keep_alive ||
                props->has_auth_method ||
                props->has_auth_data ||
                props->has_request_problem_info ||
                props->has_request_response_info ||
                props->has_response_information ||
                props->has_server_reference ||
                props->has_reason_string ||
                props->has_receive_maximum ||
                props->has_topic_alias_maximum ||
                props->has_maximum_qos ||
                props->has_retain_available ||
                props->has_maximum_packet_size ||
                props->has_wildcard_subscription_available ||
                props->has_subscription_identifiers_available ||
                props->has_shared_subscription_available) {
                return NOXMQTT_RC_ERROR_BAD_PACKET;
            }
            break;
        case NOXMQTT_CTRL_PKT_TYPE_PUBACK:
        case NOXMQTT_CTRL_PKT_TYPE_PUBREC:
        case NOXMQTT_CTRL_PKT_TYPE_PUBREL:
        case NOXMQTT_CTRL_PKT_TYPE_PUBCOMP:
        case NOXMQTT_CTRL_PKT_TYPE_SUBACK:
        case NOXMQTT_CTRL_PKT_TYPE_UNSUBACK:
            if (props->has_payload_format_indicator ||
                props->has_message_expiry_interval ||
                props->has_will_delay_interval ||
                props->has_content_type ||
                props->has_response_topic ||
                props->has_correlation_data ||
                props->has_subscription_identifier ||
                props->has_session_expiry_interval ||
                props->has_assigned_client_identifier ||
                props->has_server_keep_alive ||
                props->has_auth_method ||
                props->has_auth_data ||
                props->has_request_problem_info ||
                props->has_request_response_info ||
                props->has_response_information ||
                props->has_server_reference ||
                props->has_receive_maximum ||
                props->has_topic_alias_maximum ||
                props->has_topic_alias ||
                props->has_maximum_qos ||
                props->has_retain_available ||
                props->has_maximum_packet_size ||
                props->has_wildcard_subscription_available ||
                props->has_subscription_identifiers_available ||
                props->has_shared_subscription_available) {
                return NOXMQTT_RC_ERROR_BAD_PACKET;
            }
            break;
        case NOXMQTT_CTRL_PKT_TYPE_DISCONNECT:
            if (props->has_payload_format_indicator ||
                props->has_message_expiry_interval ||
                props->has_will_delay_interval ||
                props->has_content_type ||
                props->has_response_topic ||
                props->has_correlation_data ||
                props->has_subscription_identifier ||
                props->has_assigned_client_identifier ||
                props->has_server_keep_alive ||
                props->has_auth_method ||
                props->has_auth_data ||
                props->has_request_problem_info ||
                props->has_request_response_info ||
                props->has_response_information ||
                props->has_receive_maximum ||
                props->has_topic_alias_maximum ||
                props->has_topic_alias ||
                props->has_maximum_qos ||
                props->has_retain_available ||
                props->has_maximum_packet_size ||
                props->has_wildcard_subscription_available ||
                props->has_subscription_identifiers_available ||
                props->has_shared_subscription_available) {
                return NOXMQTT_RC_ERROR_BAD_PACKET;
            }
            break;
        case NOXMQTT_CTRL_PKT_TYPE_AUTH:
            if (props->has_payload_format_indicator ||
                props->has_message_expiry_interval ||
                props->has_will_delay_interval ||
                props->has_content_type ||
                props->has_response_topic ||
                props->has_correlation_data ||
                props->has_subscription_identifier ||
                props->has_session_expiry_interval ||
                props->has_assigned_client_identifier ||
                props->has_server_keep_alive ||
                props->has_request_problem_info ||
                props->has_request_response_info ||
                props->has_response_information ||
                props->has_server_reference ||
                props->has_receive_maximum ||
                props->has_topic_alias_maximum ||
                props->has_topic_alias ||
                props->has_maximum_qos ||
                props->has_retain_available ||
                props->has_maximum_packet_size ||
                props->has_wildcard_subscription_available ||
                props->has_subscription_identifiers_available ||
                props->has_shared_subscription_available) {
                return NOXMQTT_RC_ERROR_BAD_PACKET;
            }
            break;
        default:
            break;
    }

    if (props->has_auth_data && !props->has_auth_method) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (props->has_payload_format_indicator && props->payload_format_indicator > 1U) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (props->has_subscription_identifier) {
        uint16_t i = 0;

        if (props->subscription_identifier_count == 0U || props->subscription_identifier == 0U) {
            return NOXMQTT_RC_ERROR_BAD_PACKET;
        }
        for (i = 0; i < props->subscription_identifier_count; i++) {
            if (props->subscription_identifiers[i] == 0U) {
                return NOXMQTT_RC_ERROR_BAD_PACKET;
            }
        }
    }
    if (props->has_request_problem_info && props->request_problem_info > 1U) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (props->has_request_response_info && props->request_response_info > 1U) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (props->has_maximum_qos && props->maximum_qos > 1U) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (props->has_retain_available && props->retain_available > 1U) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (props->has_wildcard_subscription_available && props->wildcard_subscription_available > 1U) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (props->has_subscription_identifiers_available && props->subscription_identifiers_available > 1U) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (props->has_shared_subscription_available && props->shared_subscription_available > 1U) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (props->has_receive_maximum && props->receive_maximum == 0U) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (props->has_topic_alias && props->topic_alias == 0U) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (props->has_maximum_packet_size && props->maximum_packet_size == 0U) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    return NOXMQTT_SUCCESS;
}

/**
 * @brief Checks whether a reason code is valid for a packet type.
 *
 * @param[in] packet_type MQTT control packet type.
 * @param[in] reason_code Reason code to validate.
 *
 * @return Non-zero when valid, otherwise zero.
 */
uint8_t noxmqtt_mqtt5_is_valid_reason_code(noxmqtt_ctrl_pkt_type_t packet_type, uint8_t reason_code)
{
    switch (packet_type) {
        case NOXMQTT_CTRL_PKT_TYPE_CONNACK:
            switch (reason_code) {
                case MQTT5_REASON_SUCCESS:
                case MQTT5_REASON_UNSPECIFIED_ERROR:
                case MQTT5_REASON_MALFORMED_PACKET:
                case MQTT5_REASON_PROTOCOL_ERROR:
                case MQTT5_REASON_IMPLEMENTATION_SPECIFIC_ERROR:
                case MQTT5_REASON_UNSUPPORTED_PROTOCOL_VERSION:
                case MQTT5_REASON_CLIENT_IDENTIFIER_NOT_VALID:
                case MQTT5_REASON_BAD_USER_NAME_OR_PASSWORD:
                case MQTT5_REASON_NOT_AUTHORIZED:
                case MQTT5_REASON_SERVER_UNAVAILABLE:
                case MQTT5_REASON_SERVER_BUSY:
                case MQTT5_REASON_BANNED:
                case MQTT5_REASON_BAD_AUTHENTICATION_METHOD:
                case MQTT5_REASON_TOPIC_NAME_INVALID:
                case MQTT5_REASON_PACKET_TOO_LARGE:
                case MQTT5_REASON_QUOTA_EXCEEDED:
                case MQTT5_REASON_PAYLOAD_FORMAT_INVALID:
                case MQTT5_REASON_RETAIN_NOT_SUPPORTED:
                case MQTT5_REASON_QOS_NOT_SUPPORTED:
                case MQTT5_REASON_USE_ANOTHER_SERVER:
                case MQTT5_REASON_SERVER_MOVED:
                case MQTT5_REASON_CONNECTION_RATE_EXCEEDED:
                    return 1U;
                default:
                    return 0U;
            }
        case NOXMQTT_CTRL_PKT_TYPE_PUBACK:
        case NOXMQTT_CTRL_PKT_TYPE_PUBREC:
            switch (reason_code) {
                case 0x00U:
                case 0x10U:
                case 0x80U:
                case 0x83U:
                case 0x87U:
                case 0x90U:
                case 0x91U:
                case 0x97U:
                case 0x99U:
                    return 1U;
                default:
                    return 0U;
            }
        case NOXMQTT_CTRL_PKT_TYPE_PUBREL:
        case NOXMQTT_CTRL_PKT_TYPE_PUBCOMP:
            return (reason_code == 0x00U || reason_code == 0x92U) ? 1U : 0U;
        case NOXMQTT_CTRL_PKT_TYPE_DISCONNECT:
            switch (reason_code) {
                case MQTT5_REASON_SUCCESS:
                case MQTT5_REASON_DISCONNECT_WITH_WILL_MESSAGE:
                case MQTT5_REASON_UNSPECIFIED_ERROR:
                case MQTT5_REASON_MALFORMED_PACKET:
                case MQTT5_REASON_PROTOCOL_ERROR:
                case MQTT5_REASON_IMPLEMENTATION_SPECIFIC_ERROR:
                case MQTT5_REASON_NOT_AUTHORIZED:
                case MQTT5_REASON_SERVER_BUSY:
                case MQTT5_REASON_SERVER_SHUTTING_DOWN:
                case MQTT5_REASON_KEEP_ALIVE_TIMEOUT:
                case MQTT5_REASON_SESSION_TAKEN_OVER:
                case MQTT5_REASON_TOPIC_FILTER_INVALID:
                case MQTT5_REASON_TOPIC_NAME_INVALID:
                case MQTT5_REASON_RECEIVE_MAXIMUM_EXCEEDED:
                case MQTT5_REASON_TOPIC_ALIAS_INVALID:
                case MQTT5_REASON_PACKET_TOO_LARGE:
                case MQTT5_REASON_MESSAGE_RATE_TOO_HIGH:
                case MQTT5_REASON_QUOTA_EXCEEDED:
                case MQTT5_REASON_ADMINISTRATIVE_ACTION:
                case MQTT5_REASON_PAYLOAD_FORMAT_INVALID:
                case MQTT5_REASON_RETAIN_NOT_SUPPORTED:
                case MQTT5_REASON_QOS_NOT_SUPPORTED:
                case MQTT5_REASON_USE_ANOTHER_SERVER:
                case MQTT5_REASON_SERVER_MOVED:
                case MQTT5_REASON_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED:
                case MQTT5_REASON_CONNECTION_RATE_EXCEEDED:
                case MQTT5_REASON_MAXIMUM_CONNECT_TIME:
                case MQTT5_REASON_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED:
                case MQTT5_REASON_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED:
                    return 1U;
                default:
                    return 0U;
            }
        case NOXMQTT_CTRL_PKT_TYPE_AUTH:
            return (reason_code == MQTT5_REASON_SUCCESS ||
                    reason_code == MQTT5_REASON_CONTINUE_AUTHENTICATION ||
                    reason_code == MQTT5_REASON_REAUTHENTICATE) ? 1U : 0U;
        case NOXMQTT_CTRL_PKT_TYPE_SUBACK:
            switch (reason_code) {
                case 0x00U:
                case 0x01U:
                case 0x02U:
                case 0x80U:
                case 0x83U:
                case 0x87U:
                case 0x8FU:
                case 0x91U:
                case 0x97U:
                case 0x9EU:
                case 0xA1U:
                case 0xA2U:
                    return 1U;
                default:
                    return 0U;
            }
        case NOXMQTT_CTRL_PKT_TYPE_UNSUBACK:
            switch (reason_code) {
                case 0x00U:
                case 0x11U:
                case 0x80U:
                case 0x83U:
                case 0x87U:
                case 0x8FU:
                case 0x91U:
                    return 1U;
                default:
                    return 0U;
            }
        default:
            return 1U;
    }
}

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
                                             uint16_t reason_code_count)
{
    uint16_t i = 0;

    if (reason_codes == NULL) {
        return (reason_code_count == 0U) ? 1U : 0U;
    }

    for (i = 0; i < reason_code_count; i++) {
        if (!noxmqtt_mqtt5_is_valid_reason_code(packet_type, reason_codes[i])) {
            return 0U;
        }
    }

    return 1U;
}

/**
 * @brief Appends raw bytes to an encoding buffer.
 *
 * @param[out] buffer Output buffer.
 * @param[in] buffer_len Output buffer length in bytes.
 * @param[in,out] offset Current write offset and resulting offset.
 * @param[in] data Bytes to append.
 * @param[in] data_len Number of bytes to append.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t mqtt5_write_bytes(uint8_t* buffer,
                                      uint16_t buffer_len,
                                      uint16_t* offset,
                                      const uint8_t* data,
                                      uint16_t data_len)
{
    if (buffer == NULL || offset == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }
    if (data_len == 0U) {
        return NOXMQTT_SUCCESS;
    }
    if (data == NULL || (uint32_t)*offset + data_len > buffer_len) {
        return NOXMQTT_RC_ERROR_OVERFLOW;
    }

    memcpy(&buffer[*offset], data, data_len);
    *offset = (uint16_t)(*offset + data_len);
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Writes a big-endian 16-bit value.
 *
 * @param[out] buffer Output buffer.
 * @param[in] buffer_len Output buffer length in bytes.
 * @param[in,out] offset Current write offset and resulting offset.
 * @param[in] value Value to encode.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t mqtt5_write_u16(uint8_t* buffer, uint16_t buffer_len, uint16_t* offset, uint16_t value)
{
    uint8_t bytes[2];

    bytes[0] = MSB(value);
    bytes[1] = LSB(value);
    return mqtt5_write_bytes(buffer, buffer_len, offset, bytes, sizeof(bytes));
}

/**
 * @brief Writes a big-endian 32-bit value.
 *
 * @param[out] buffer Output buffer.
 * @param[in] buffer_len Output buffer length in bytes.
 * @param[in,out] offset Current write offset and resulting offset.
 * @param[in] value Value to encode.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t mqtt5_write_u32(uint8_t* buffer, uint16_t buffer_len, uint16_t* offset, uint32_t value)
{
    uint8_t bytes[4];

    bytes[0] = (uint8_t)((value >> 24) & 0xFFU);
    bytes[1] = (uint8_t)((value >> 16) & 0xFFU);
    bytes[2] = (uint8_t)((value >> 8) & 0xFFU);
    bytes[3] = (uint8_t)(value & 0xFFU);
    return mqtt5_write_bytes(buffer, buffer_len, offset, bytes, sizeof(bytes));
}

/**
 * @brief Writes an MQTT variable-length integer.
 *
 * @param[out] buffer Output buffer.
 * @param[in] buffer_len Output buffer length in bytes.
 * @param[in,out] offset Current write offset and resulting offset.
 * @param[in] value Value to encode.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t mqtt5_write_varint(uint8_t* buffer, uint16_t buffer_len, uint16_t* offset, uint32_t value)
{
    uint8_t encoded[4];
    uint8_t count = 0;

    if (value > 268435455U) {
        return NOXMQTT_RC_ERROR_OVERFLOW;
    }

    do {
        uint8_t digit = (uint8_t)(value % 128U);
        value /= 128U;
        if (value > 0U) {
            digit |= 0x80U;
        }
        encoded[count++] = digit;
    } while (value > 0U && count < 4U);

    return mqtt5_write_bytes(buffer, buffer_len, offset, encoded, count);
}

/**
 * @brief Writes an MQTT UTF-8 string field.
 *
 * @param[out] buffer Output buffer.
 * @param[in] buffer_len Output buffer length in bytes.
 * @param[in,out] offset Current write offset and resulting offset.
 * @param[in] str Null-terminated string to encode.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t mqtt5_write_utf8_string(uint8_t* buffer,
                                            uint16_t buffer_len,
                                            uint16_t* offset,
                                            const char* str)
{
    uint16_t str_len = 0;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    if (str == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    str_len = (uint16_t)strlen(str);
    rc = mqtt5_write_u16(buffer, buffer_len, offset, str_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    return mqtt5_write_bytes(buffer, buffer_len, offset, (const uint8_t*)str, str_len);
}

/**
 * @brief Writes an MQTT binary data field.
 *
 * @param[out] buffer Output buffer.
 * @param[in] buffer_len Output buffer length in bytes.
 * @param[in,out] offset Current write offset and resulting offset.
 * @param[in] data Binary buffer to encode.
 * @param[in] data_len Number of bytes to encode.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t mqtt5_write_binary(uint8_t* buffer,
                                       uint16_t buffer_len,
                                       uint16_t* offset,
                                       const uint8_t* data,
                                       uint16_t data_len)
{
    noxmqtt_rc_t rc = mqtt5_write_u16(buffer, buffer_len, offset, data_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    return mqtt5_write_bytes(buffer, buffer_len, offset, data, data_len);
}

/**
 * @brief Writes MQTT 5 user properties to an encoding buffer.
 *
 * @param[out] buffer Output buffer.
 * @param[in] buffer_len Output buffer length in bytes.
 * @param[in,out] offset Current write offset and resulting offset.
 * @param[in] props User property array.
 * @param[in] count Number of user properties.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t mqtt5_write_user_properties(uint8_t* buffer,
                                                uint16_t buffer_len,
                                                uint16_t* offset,
                                                const noxmqtt_mqtt5_user_property_t* props,
                                                uint16_t count)
{
    uint16_t i = 0;

    if (count == 0U) {
        return NOXMQTT_SUCCESS;
    }
    if (props == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    for (i = 0; i < count; i++) {
        noxmqtt_rc_t rc;

        buffer[(*offset)++] = MQTT5_PROPERTY_USER_PROPERTY;
        rc = mqtt5_write_u16(buffer, buffer_len, offset, props[i].name_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
        rc = mqtt5_write_bytes(buffer, buffer_len, offset, (const uint8_t*)props[i].name, props[i].name_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
        rc = mqtt5_write_u16(buffer, buffer_len, offset, props[i].value_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
        rc = mqtt5_write_bytes(buffer, buffer_len, offset, (const uint8_t*)props[i].value, props[i].value_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    return NOXMQTT_SUCCESS;
}

/**
 * @brief Reads an MQTT variable-length integer from a buffer.
 *
 * @param[in] data Packet buffer.
 * @param[in] len Packet buffer length in bytes.
 * @param[in,out] offset Current parse offset and resulting offset.
 * @param[out] value Decoded value.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t mqtt5_read_varint(const uint8_t* data,
                                      uint16_t len,
                                      uint16_t* offset,
                                      uint32_t* value)
{
    uint32_t multiplier = 1U;
    uint32_t result = 0U;

    if (data == NULL || offset == NULL || value == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    do {
        uint8_t byte = 0;

        if (*offset >= len) {
            return NOXMQTT_RC_ERROR_BAD_PACKET;
        }

        byte = data[(*offset)++];
        result += (uint32_t)(byte & 0x7FU) * multiplier;
        if ((byte & 0x80U) == 0U) {
            *value = result;
            return NOXMQTT_SUCCESS;
        }
        multiplier *= 128U;
    } while (multiplier <= 128U * 128U * 128U);

    return NOXMQTT_RC_ERROR_BAD_PACKET;
}

/**
 * @brief Reads a big-endian 16-bit value from a buffer.
 *
 * @param[in] data Packet buffer.
 * @param[in] len Packet buffer length in bytes.
 * @param[in,out] offset Current parse offset and resulting offset.
 * @param[out] value Decoded value.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t mqtt5_read_u16(const uint8_t* data, uint16_t len, uint16_t* offset, uint16_t* value)
{
    if (data == NULL || offset == NULL || value == NULL || (uint16_t)(*offset + 2U) > len) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    *value = (uint16_t)((data[*offset] << 8) | data[*offset + 1U]);
    *offset = (uint16_t)(*offset + 2U);
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Reads a big-endian 32-bit value from a buffer.
 *
 * @param[in] data Packet buffer.
 * @param[in] len Packet buffer length in bytes.
 * @param[in,out] offset Current parse offset and resulting offset.
 * @param[out] value Decoded value.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t mqtt5_read_u32(const uint8_t* data, uint16_t len, uint16_t* offset, uint32_t* value)
{
    if (data == NULL || offset == NULL || value == NULL || (uint16_t)(*offset + 4U) > len) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    *value = ((uint32_t)data[*offset] << 24) |
             ((uint32_t)data[*offset + 1U] << 16) |
             ((uint32_t)data[*offset + 2U] << 8) |
             (uint32_t)data[*offset + 3U];
    *offset = (uint16_t)(*offset + 4U);
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Reads an MQTT UTF-8 string field from a buffer.
 *
 * @param[in] data Packet buffer.
 * @param[in] len Packet buffer length in bytes.
 * @param[in,out] offset Current parse offset and resulting offset.
 * @param[out] str Pointer to the decoded string bytes.
 * @param[out] str_len Decoded string length in bytes.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t mqtt5_read_utf8_string(const uint8_t* data,
                                           uint16_t len,
                                           uint16_t* offset,
                                           const char** str,
                                           uint16_t* str_len)
{
    uint16_t length = 0;

    if (str == NULL || str_len == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (mqtt5_read_u16(data, len, offset, &length) != NOXMQTT_SUCCESS ||
        (uint16_t)(*offset + length) > len) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    *str = (const char*)&data[*offset];
    *str_len = length;
    *offset = (uint16_t)(*offset + length);
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Reads an MQTT binary data field from a buffer.
 *
 * @param[in] data Packet buffer.
 * @param[in] len Packet buffer length in bytes.
 * @param[in,out] offset Current parse offset and resulting offset.
 * @param[out] value Pointer to the decoded binary bytes.
 * @param[out] value_len Decoded binary length in bytes.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t mqtt5_read_binary(const uint8_t* data,
                                      uint16_t len,
                                      uint16_t* offset,
                                      const uint8_t** value,
                                      uint16_t* value_len)
{
    uint16_t length = 0;

    if (value == NULL || value_len == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (mqtt5_read_u16(data, len, offset, &length) != NOXMQTT_SUCCESS ||
        (uint16_t)(*offset + length) > len) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    *value = &data[*offset];
    *value_len = length;
    *offset = (uint16_t)(*offset + length);
    return NOXMQTT_SUCCESS;
}
