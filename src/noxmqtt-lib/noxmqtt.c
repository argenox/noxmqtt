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
* File: noxmqtt.c
* Summary: NoxMQTT External APIs
*
*****************************************************************************/

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "noxmqtt.h"
#include "noxmqttlib.h"
#include "noxmqtt_err.h"
#include "noxmqtt_common.h"
#include "noxmqtt_tal.h"
#include "noxmqtt_config.h"
#include "noxmqtt_debug.h"
#include "noxmqtt_mqtt5.h"

/* Internal helper functions */
static void noxmqtt_send_event(noxmqtt_client_t* c, noxmqtt_evt_data_t* data);
static void noxmqtt_send_error(noxmqtt_client_t* c, noxmqtt_rc_t rc);
static noxmqtt_rc_t noxmqtt_transport_write_packet(noxmqtt_client_t* c, const uint8_t* data, uint16_t len);
static noxmqtt_rc_t noxmqtt_send_simple_packet(noxmqtt_client_t* c, noxmqtt_ctrl_pkt_type_t type, uint8_t flags);
static noxmqtt_rc_t noxmqtt_puback(noxmqtt_client_t* c, uint16_t identifier);
static noxmqtt_rc_t noxmqtt_pubrec(noxmqtt_client_t* c, uint16_t identifier);
static noxmqtt_rc_t noxmqtt_pubcomp(noxmqtt_client_t* c, uint16_t identifier);
static noxmqtt_rc_t noxmqtt_pubrel(noxmqtt_client_t* c, uint16_t identifier);
static noxmqtt_rc_t noxmqtt_pingreq(noxmqtt_client_t* c);
static noxmqtt_rc_t noxmqtt_publish_internal(noxmqtt_client_t* c,
                                             noxmqtt_qos_t qos,
                                             uint8_t retain,
                                             uint8_t dup,
                                             const char* topic,
                                             const uint8_t* payload,
                                             uint16_t payload_len,
                                             uint16_t packet_identifier,
                                             const noxmqtt_mqtt5_publish_props_t* props);
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
static noxmqtt_rc_t noxmqtt_write_bytes(uint8_t* buffer,
                                        uint16_t buffer_len,
                                        uint16_t* offset,
                                        const uint8_t* data,
                                        uint16_t data_len);
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
static noxmqtt_rc_t noxmqtt_write_utf8_string(uint8_t* buffer,
                                              uint16_t buffer_len,
                                              uint16_t* offset,
                                              const char* str);
static noxmqtt_rc_t noxmqtt_write_u16(uint8_t* buffer, uint16_t buffer_len, uint16_t* offset, uint16_t value);
static noxmqtt_rc_t noxmqtt_write_varint(uint8_t* buffer, uint16_t buffer_len, uint16_t* offset, uint32_t value);
static uint16_t noxmqtt_packet_start_offset(uint8_t remain_len_bytes);
static noxmqtt_rc_t noxmqtt_finalize_packet(noxmqtt_client_t* c,
                                            noxmqtt_hdr_t hdr,
                                            uint16_t body_len,
                                            uint8_t** out_start,
                                            uint16_t* out_len);
static noxmqtt_rc_t noxmqtt_validate_client(const noxmqtt_client_t* c);
static uint8_t noxmqtt_validate_received_header_flags(const noxmqtt_hdr_t* hdr);
static uint32_t noxmqtt_last_activity_ms(noxmqtt_client_t* c);
static void noxmqtt_subscription_cache_clear(noxmqtt_client_t* c);
static void noxmqtt_outbox_clear(noxmqtt_client_t* c);
static noxmqtt_rc_t noxmqtt_subscription_cache_add(noxmqtt_client_t* c, const noxmqtt_topic_sub_t* topic);
static void noxmqtt_subscription_cache_remove(noxmqtt_client_t* c, const char* topic);
static noxmqtt_rc_t noxmqtt_outbox_store(noxmqtt_client_t* c,
                                         uint16_t packet_identifier,
                                         const char* topic,
                                         const uint8_t* payload,
                                         uint16_t payload_len,
                                         noxmqtt_qos_t qos,
                                         uint8_t retain,
                                         const noxmqtt_mqtt5_publish_props_t* props,
                                         noxmqtt_outbox_state_t initial_state);
static noxmqtt_outbox_item_t* noxmqtt_outbox_find(noxmqtt_client_t* c, uint16_t packet_identifier);
static void noxmqtt_outbox_remove(noxmqtt_client_t* c, uint16_t packet_identifier);
static void noxmqtt_outbox_item_reset(noxmqtt_outbox_item_t* item);
static noxmqtt_rc_t noxmqtt_outbox_clone_publish_props(noxmqtt_outbox_item_t* item,
                                                       const noxmqtt_mqtt5_publish_props_t* props);
static uint16_t noxmqtt_outbox_inflight_count(const noxmqtt_client_t* c);
static noxmqtt_rc_t noxmqtt_outbox_pump(noxmqtt_client_t* c);
static noxmqtt_rc_t noxmqtt_resubscribe_all(noxmqtt_client_t* c);
static noxmqtt_rc_t noxmqtt_replay_outbox(noxmqtt_client_t* c);
static char* noxmqtt_strdup_local(const char* src);
static char* noxmqtt_strndup_local(const char* src, uint16_t len);
static uint8_t* noxmqtt_memdup_local(const uint8_t* src, uint16_t len);
static void noxmqtt_clear_assigned_client_identifier(noxmqtt_client_t* c);
static noxmqtt_rc_t noxmqtt_set_assigned_client_identifier(noxmqtt_client_t* c, const char* identifier, uint16_t len);
static void noxmqtt_topic_alias_cache_clear(noxmqtt_client_t* c);
static noxmqtt_rc_t noxmqtt_topic_alias_store(noxmqtt_client_t* c, uint16_t alias, const char* topic, uint16_t topic_len);
static const char* noxmqtt_topic_alias_lookup(noxmqtt_client_t* c, uint16_t alias, uint16_t* topic_len);
static void noxmqtt_publish_topic_alias_cache_clear(noxmqtt_client_t* c);
static uint16_t noxmqtt_publish_topic_alias_find(const noxmqtt_client_t* c, const char* topic);
static const char* noxmqtt_publish_topic_alias_lookup(noxmqtt_client_t* c, uint16_t alias);
static noxmqtt_rc_t noxmqtt_publish_topic_alias_store(noxmqtt_client_t* c, uint16_t alias, const char* topic);
static uint16_t noxmqtt_publish_topic_alias_assign(noxmqtt_client_t* c, const char* topic);
static void noxmqtt_clear_active_auth_method(noxmqtt_client_t* c);
static noxmqtt_rc_t noxmqtt_set_active_auth_method(noxmqtt_client_t* c, const char* method, uint16_t len);
static uint8_t noxmqtt_active_auth_method_matches(const noxmqtt_client_t* c, const char* method, uint16_t len);
static uint8_t noxmqtt_is_auth_exchange_active(uint8_t reason_code);
static void noxmqtt_reset_broker_capabilities(noxmqtt_client_t* c);
static uint8_t noxmqtt_topic_has_wildcard(const char* topic);
static uint8_t noxmqtt_topic_is_shared_subscription(const char* topic);
static void noxmqtt_clear_tx_buf(noxmqtt_client_t* c);

/* MQTT Response Handlers */
static void noxmqtt_handler_connack(noxmqtt_client_t* c, uint8_t* data, uint16_t len);
static void noxmqtt_handler_publish(noxmqtt_client_t* c, uint8_t* data, uint16_t len);
static void noxmqtt_handler_puback(noxmqtt_client_t* c, uint8_t* data, uint16_t len);
static void noxmqtt_handler_pubrec(noxmqtt_client_t* c, uint8_t* data, uint16_t len);
static void noxmqtt_handler_pubrel(noxmqtt_client_t* c, uint8_t* data, uint16_t len);
static void noxmqtt_handler_pubcomp(noxmqtt_client_t* c, uint8_t* data, uint16_t len);
static void noxmqtt_handler_suback(noxmqtt_client_t* c, uint8_t* data, uint16_t len);
static void noxmqtt_handler_unsuback(noxmqtt_client_t* c, uint8_t* data, uint16_t len);
static void noxmqtt_handler_disconnect_packet(noxmqtt_client_t* c, uint8_t* data, uint16_t len);
static void noxmqtt_handler_auth(noxmqtt_client_t* c, uint8_t* data, uint16_t len);
static void noxmqtt_handler_pingresp(noxmqtt_client_t* c, uint8_t* data, uint16_t len);

int noxmqtt_set_remain_len(uint8_t* buffer, uint32_t len);
int noxmqtt_decode_remain_len(const uint8_t* buffer, uint32_t* len);

/**
 * @brief Initializes an NoxMQTT client instance.
 *
 * @param[in] c Client instance to initialize.
 * @param[in] lvl Initial debug verbosity level.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_init(noxmqtt_client_t* c, noxmqtt_debug_lvl_t lvl)
{
    if (c == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    memset((void*)c, 0, sizeof(noxmqtt_client_t));

    c->packet_ident = 1;
    c->debug_lvl = lvl;
    c->keepalive = MQTT_CONN_DEFAULT_KEEPALIVE;
    c->tx_buf_size = NOXMQTT_TX_BUF_SIZE;
    c->rcv_buf_size = NOXMQTT_RX_BUF_SIZE;

    c->tx_buf = (uint8_t*)malloc(c->tx_buf_size);
    c->rcv_buf = (uint8_t*)malloc(c->rcv_buf_size);
    c->subscriptions = (noxmqtt_subscription_state_t*)calloc(NOXMQTT_MAX_SUBSCRIPTIONS, sizeof(*c->subscriptions));
    c->outbox = (noxmqtt_outbox_item_t*)calloc(NOXMQTT_MAX_OUTBOX_MESSAGES, sizeof(*c->outbox));
    c->topic_alias_capacity = NOXMQTT_MAX_TOPIC_ALIASES;
    c->topic_aliases = (char**)calloc((size_t)c->topic_alias_capacity + 1U, sizeof(*c->topic_aliases));
    c->publish_topic_aliases = (char**)calloc((size_t)c->topic_alias_capacity + 1U, sizeof(*c->publish_topic_aliases));
    c->next_publish_topic_alias = 1U;
    if (c->tx_buf == NULL || c->rcv_buf == NULL || c->subscriptions == NULL || c->outbox == NULL ||
        c->topic_aliases == NULL || c->publish_topic_aliases == NULL) {
        noxmqtt_deinit(c);
        return NOXMQTT_RC_ERROR_NO_MEMORY;
    }

    noxmqtt_reset_broker_capabilities(c);
    c->flag_initialized = NOXMQTT_INIT_FLAG;

    return NOXMQTT_SUCCESS;
}

/**
 * @brief Releases resources owned by an NoxMQTT client instance.
 *
 * @param[in] c Client instance to deinitialize.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_deinit(noxmqtt_client_t* c)
{
    if (c == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (c->status.connected) {
        (void)noxmqtt_disconnect(c);
    }

    if (c->tx_buf != NULL) {
        free(c->tx_buf);
        c->tx_buf = NULL;
    }

    if (c->rcv_buf != NULL) {
        free(c->rcv_buf);
        c->rcv_buf = NULL;
    }

    noxmqtt_subscription_cache_clear(c);
    if (c->subscriptions != NULL) {
        free(c->subscriptions);
        c->subscriptions = NULL;
    }

    noxmqtt_outbox_clear(c);
    if (c->outbox != NULL) {
        free(c->outbox);
        c->outbox = NULL;
    }

    noxmqtt_topic_alias_cache_clear(c);
    if (c->topic_aliases != NULL) {
        free(c->topic_aliases);
        c->topic_aliases = NULL;
    }

    noxmqtt_publish_topic_alias_cache_clear(c);
    if (c->publish_topic_aliases != NULL) {
        free(c->publish_topic_aliases);
        c->publish_topic_aliases = NULL;
    }

    noxmqtt_clear_active_auth_method(c);
    noxmqtt_clear_assigned_client_identifier(c);

    if (c->transport_ctx != NULL) {
        free(c->transport_ctx);
        c->transport_ctx = NULL;
    }

    memset(c, 0, sizeof(*c));

    return NOXMQTT_SUCCESS;
}

/**
 * @brief Processes bytes received from the active transport.
 *
 * @param[in] c Client instance.
 * @param[in] data Received packet bytes.
 * @param[in] len Number of received bytes.
 */
void noxmqtt_transport_rcv_func(noxmqtt_client_t* c, const uint8_t* data, uint16_t len)
{
    uint32_t remain_length = 0;
    uint16_t parse_offset = 0;

    if (noxmqtt_validate_client(c) != NOXMQTT_SUCCESS || data == NULL || len == 0) {
        return;
    }

    if ((uint32_t)c->rcv_offset + (uint32_t)len > c->rcv_buf_size) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_OVERFLOW);
        c->rcv_offset = 0;
        return;
    }

    memcpy(&c->rcv_buf[c->rcv_offset], data, len);
    c->rcv_offset = (uint16_t)(c->rcv_offset + len);
    c->last_rx_ms = noxmqtt_tal_time_ms();

    while ((uint16_t)(c->rcv_offset - parse_offset) >= 2U) {
        const noxmqtt_hdr_t* hdr = (const noxmqtt_hdr_t*)&c->rcv_buf[parse_offset];
        uint16_t available = (uint16_t)(c->rcv_offset - parse_offset);
        uint16_t packet_len = 0;
        int remain_len_bytes = 0;

        if (!noxmqtt_validate_received_header_flags(hdr)) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            c->rcv_offset = 0;
            return;
        }

        remain_len_bytes = noxmqtt_decode_remain_len(&c->rcv_buf[parse_offset + sizeof(noxmqtt_hdr_t)], &remain_length);
        if (remain_len_bytes < 0 || remain_len_bytes > (int)NOXMQTT_MAX_REMAIN_LEN_BYTES) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            c->rcv_offset = 0;
            return;
        }

        if (available < (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes)) {
            break;
        }

        if (remain_length > 0xFFFFU) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            c->rcv_offset = 0;
            return;
        }

        packet_len = (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes + remain_length);
        if (available < packet_len) {
            break;
        }

        noxmqtt_debug_printf(c, NOXMQTT_DEBUG_LVL_DEBUG,
                             "Received packet type %s len %u\n",
                             get_mqtt_packet_type_str(hdr->type),
                             packet_len);

        switch (hdr->type) {
            case NOXMQTT_CTRL_PKT_TYPE_CONNACK:
                noxmqtt_handler_connack(c, &c->rcv_buf[parse_offset], packet_len);
                break;
            case NOXMQTT_CTRL_PKT_TYPE_PUBLISH:
                noxmqtt_handler_publish(c, &c->rcv_buf[parse_offset], packet_len);
                break;
            case NOXMQTT_CTRL_PKT_TYPE_PUBACK:
                noxmqtt_handler_puback(c, &c->rcv_buf[parse_offset], packet_len);
                break;
            case NOXMQTT_CTRL_PKT_TYPE_PUBREC:
                noxmqtt_handler_pubrec(c, &c->rcv_buf[parse_offset], packet_len);
                break;
            case NOXMQTT_CTRL_PKT_TYPE_PUBREL:
                noxmqtt_handler_pubrel(c, &c->rcv_buf[parse_offset], packet_len);
                break;
            case NOXMQTT_CTRL_PKT_TYPE_PUBCOMP:
                noxmqtt_handler_pubcomp(c, &c->rcv_buf[parse_offset], packet_len);
                break;
            case NOXMQTT_CTRL_PKT_TYPE_SUBACK:
                noxmqtt_handler_suback(c, &c->rcv_buf[parse_offset], packet_len);
                break;
            case NOXMQTT_CTRL_PKT_TYPE_UNSUBACK:
                noxmqtt_handler_unsuback(c, &c->rcv_buf[parse_offset], packet_len);
                break;
            case NOXMQTT_CTRL_PKT_TYPE_PINGRESP:
                noxmqtt_handler_pingresp(c, &c->rcv_buf[parse_offset], packet_len);
                break;
            case NOXMQTT_CTRL_PKT_TYPE_DISCONNECT:
                noxmqtt_handler_disconnect_packet(c, &c->rcv_buf[parse_offset], packet_len);
                break;
            case NOXMQTT_CTRL_PKT_TYPE_AUTH:
                noxmqtt_handler_auth(c, &c->rcv_buf[parse_offset], packet_len);
                break;
            default:
                noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
                break;
        }

        parse_offset = (uint16_t)(parse_offset + packet_len);
    }

    if (parse_offset > 0) {
        uint16_t left = (uint16_t)(c->rcv_offset - parse_offset);
        if (left > 0) {
            memmove(c->rcv_buf, &c->rcv_buf[parse_offset], left);
        }
        c->rcv_offset = left;
    }
}

/**
 * @brief Handles an incoming CONNACK packet.
 *
 * @param[in] c Client instance.
 * @param[in] data Packet buffer.
 * @param[in] len Packet length in bytes.
 */
static void noxmqtt_handler_connack(noxmqtt_client_t* c, uint8_t* data, uint16_t len)
{
    noxmqtt_evt_data_t evt_data;
    const noxmqtt_response_var_hdr_t* var_hdr = NULL;
    uint32_t remain_length = 0;
    int remain_len_bytes = 0;

    MEMZERO_S(evt_data);

    if (len < 4U) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }

    remain_len_bytes = noxmqtt_decode_remain_len(&data[sizeof(noxmqtt_hdr_t)], &remain_length);
    if (remain_len_bytes != 1 || remain_length < 2U) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }

    var_hdr = (noxmqtt_response_var_hdr_t*)(data + sizeof(noxmqtt_hdr_t) + remain_len_bytes);

    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        uint16_t offset = (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes);
        uint8_t ack_flags = data[offset];
        uint8_t reason_code = data[offset + 1];
        uint8_t session_present = 0U;
        noxmqtt_mqtt5_property_view_t props;

        if ((ack_flags & 0xFEU) != 0U || (reason_code != MQTT5_REASON_SUCCESS && (ack_flags & 0x01U) != 0U)) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }
        if (!noxmqtt_mqtt5_is_valid_reason_code(NOXMQTT_CTRL_PKT_TYPE_CONNACK, reason_code)) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        offset = (uint16_t)(offset + 2U);
        if (noxmqtt_mqtt5_parse_properties(data, len, &offset, &props) != NOXMQTT_SUCCESS || offset != len) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }
        if (noxmqtt_mqtt5_validate_properties(NOXMQTT_CTRL_PKT_TYPE_CONNACK, &props) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        session_present = (uint8_t)(ack_flags & 0x01U);
        if (reason_code == MQTT5_REASON_SUCCESS &&
            c->last_conf.clean_session &&
            session_present != 0U) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        if (reason_code == MQTT5_REASON_SUCCESS) {
            if (c->active_auth_method != NULL) {
                if (!props.has_auth_method ||
                    !noxmqtt_active_auth_method_matches(c, props.auth_method, props.auth_method_len)) {
                    noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
                    return;
                }
            } else if (props.has_auth_method) {
                if (noxmqtt_set_active_auth_method(c, props.auth_method, props.auth_method_len) != NOXMQTT_SUCCESS) {
                    noxmqtt_send_error(c, NOXMQTT_RC_ERROR_NO_MEMORY);
                    return;
                }
            }
            if (props.has_assigned_client_identifier) {
                if (noxmqtt_set_assigned_client_identifier(c,
                                                           props.assigned_client_identifier,
                                                           props.assigned_client_identifier_len) != NOXMQTT_SUCCESS) {
                    noxmqtt_send_error(c, NOXMQTT_RC_ERROR_NO_MEMORY);
                    return;
                }
            }

            c->status.connected = 1;
            c->status.ping_outstanding = 0;
            c->status.reconnect_pending = 0;
            c->status.auth_in_progress = 0;
            c->last_rx_ms = noxmqtt_tal_time_ms();
            if (props.has_server_keep_alive) {
                c->keepalive = props.server_keep_alive;
            }
            if (props.has_receive_maximum) {
                c->broker_receive_maximum = props.receive_maximum;
            }
            if (props.has_topic_alias_maximum) {
                c->broker_topic_alias_maximum = props.topic_alias_maximum;
            }
            if (props.has_maximum_packet_size) {
                c->broker_maximum_packet_size = props.maximum_packet_size;
            }
            if (props.has_session_expiry_interval) {
                c->broker_session_expiry_interval = props.session_expiry_interval;
            }
            if (props.has_maximum_qos) {
                c->broker_maximum_qos = props.maximum_qos;
            }
            if (props.has_retain_available) {
                c->broker_retain_available = props.retain_available;
            }
            if (props.has_wildcard_subscription_available) {
                c->broker_wildcard_subscriptions_available = props.wildcard_subscription_available;
            }
            if (props.has_subscription_identifiers_available) {
                c->broker_subscription_identifiers_available = props.subscription_identifiers_available;
            }
            if (props.has_shared_subscription_available) {
                c->broker_shared_subscriptions_available = props.shared_subscription_available;
            }
            evt_data.evt_id = NOXMQTT_EVT_CONNECT;
            evt_data.evt.connect_evt.session_present = session_present;
            evt_data.evt.connect_evt.reason_code = reason_code;
            evt_data.evt.connect_evt.maximum_qos = c->broker_maximum_qos;
            evt_data.evt.connect_evt.retain_available = c->broker_retain_available;
            evt_data.evt.connect_evt.wildcard_subscriptions_available =
                c->broker_wildcard_subscriptions_available;
            evt_data.evt.connect_evt.subscription_identifiers_available =
                c->broker_subscription_identifiers_available;
            evt_data.evt.connect_evt.shared_subscriptions_available =
                c->broker_shared_subscriptions_available;
            evt_data.evt.connect_evt.session_expiry_interval = props.session_expiry_interval;
            evt_data.evt.connect_evt.maximum_packet_size = props.maximum_packet_size;
            evt_data.evt.connect_evt.receive_maximum = props.receive_maximum;
            evt_data.evt.connect_evt.topic_alias_maximum = props.topic_alias_maximum;
            evt_data.evt.connect_evt.assigned_client_identifier = props.assigned_client_identifier;
            evt_data.evt.connect_evt.assigned_client_identifier_len = props.assigned_client_identifier_len;
            evt_data.evt.connect_evt.response_information = props.response_information;
            evt_data.evt.connect_evt.response_information_len = props.response_information_len;
            evt_data.evt.connect_evt.server_reference = props.server_reference;
            evt_data.evt.connect_evt.server_reference_len = props.server_reference_len;
            evt_data.evt.connect_evt.auth_method = props.auth_method;
            evt_data.evt.connect_evt.auth_method_len = props.auth_method_len;
            evt_data.evt.connect_evt.auth_data = props.auth_data;
            evt_data.evt.connect_evt.auth_data_len = props.auth_data_len;
            evt_data.evt.connect_evt.user_properties = props.user_properties;
            evt_data.evt.connect_evt.user_property_count = props.user_property_count;
            noxmqtt_send_event(c, &evt_data);
            {
                noxmqtt_rc_t resume_rc = NOXMQTT_SUCCESS;
                if (session_present == 0U) {
                    resume_rc = noxmqtt_resubscribe_all(c);
                    if (resume_rc != NOXMQTT_SUCCESS) {
                        noxmqtt_send_error(c, resume_rc);
                    }
                }
                resume_rc = noxmqtt_replay_outbox(c);
                if (resume_rc != NOXMQTT_SUCCESS) {
                    noxmqtt_send_error(c, resume_rc);
                }
            }
            return;
        }

        evt_data.evt_id = NOXMQTT_EVT_CONNECT_ERROR;
        switch (reason_code) {
            case MQTT5_REASON_UNSUPPORTED_PROTOCOL_VERSION:
                evt_data.evt.conn_err_evt.reason = NOXMQTT_CONN_ERR_REFUSED_UNACCP_PROT_VER;
                break;
            case MQTT5_REASON_CLIENT_IDENTIFIER_NOT_VALID:
                evt_data.evt.conn_err_evt.reason = NOXMQTT_CONN_ERR_REFUSED_IDENT_REJECTED;
                break;
            case MQTT5_REASON_BAD_USER_NAME_OR_PASSWORD:
                evt_data.evt.conn_err_evt.reason = NOXMQTT_CONN_ERR_REFUSED_BAD_USER_PASS;
                break;
            case MQTT5_REASON_NOT_AUTHORIZED:
                evt_data.evt.conn_err_evt.reason = NOXMQTT_CONN_ERR_REFUSED_NOT_AUTH;
                break;
            default:
                evt_data.evt.conn_err_evt.reason = NOXMQTT_CONN_ERR_REFUSED_SERVER_UNAVAIL;
                break;
        }
        evt_data.evt.conn_err_evt.raw_reason_code = reason_code;
        evt_data.evt.conn_err_evt.reason_string = props.reason_string;
        evt_data.evt.conn_err_evt.reason_string_len = props.reason_string_len;
        evt_data.evt.conn_err_evt.server_reference = props.server_reference;
        evt_data.evt.conn_err_evt.server_reference_len = props.server_reference_len;
        evt_data.evt.conn_err_evt.user_properties = props.user_properties;
        evt_data.evt.conn_err_evt.user_property_count = props.user_property_count;
        noxmqtt_send_event(c, &evt_data);
        return;
    }

    if (remain_length != 2U || len != (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes + remain_length)) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }
    if ((var_hdr->conn_ack.flag_session_present & 0xFEU) != 0U ||
        (c->last_conf.clean_session && var_hdr->conn_ack.flag_session_present != 0U) ||
        (var_hdr->conn_ack.conn_return_code != NOXMQTT_CONNECTION_RC_ACCEPTED &&
         var_hdr->conn_ack.flag_session_present != 0U)) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }

    switch (var_hdr->conn_ack.conn_return_code) {
        case NOXMQTT_CONNECTION_RC_ACCEPTED:
            c->status.connected = 1;
            c->status.ping_outstanding = 0;
            c->status.reconnect_pending = 0;
            c->last_rx_ms = noxmqtt_tal_time_ms();
            evt_data.evt_id = NOXMQTT_EVT_CONNECT;
            evt_data.evt.connect_evt.session_present = var_hdr->conn_ack.flag_session_present;
            noxmqtt_send_event(c, &evt_data);
            {
                noxmqtt_rc_t resume_rc = NOXMQTT_SUCCESS;
                if (var_hdr->conn_ack.flag_session_present == 0U) {
                    resume_rc = noxmqtt_resubscribe_all(c);
                    if (resume_rc != NOXMQTT_SUCCESS) {
                        noxmqtt_send_error(c, resume_rc);
                    }
                }
                resume_rc = noxmqtt_replay_outbox(c);
                if (resume_rc != NOXMQTT_SUCCESS) {
                    noxmqtt_send_error(c, resume_rc);
                }
            }
            break;
        case NOXMQTT_CONNECTION_RC_REFUSED_UNACCP_PROT_VER:
            evt_data.evt_id = NOXMQTT_EVT_CONNECT_ERROR;
            evt_data.evt.conn_err_evt.reason = NOXMQTT_CONN_ERR_REFUSED_UNACCP_PROT_VER;
            evt_data.evt.conn_err_evt.raw_reason_code = var_hdr->conn_ack.conn_return_code;
            noxmqtt_send_event(c, &evt_data);
            break;
        case NOXMQTT_CONNECTION_RC_REFUSED_IDENT_REJECTED:
            evt_data.evt_id = NOXMQTT_EVT_CONNECT_ERROR;
            evt_data.evt.conn_err_evt.reason = NOXMQTT_CONN_ERR_REFUSED_IDENT_REJECTED;
            evt_data.evt.conn_err_evt.raw_reason_code = var_hdr->conn_ack.conn_return_code;
            noxmqtt_send_event(c, &evt_data);
            break;
        case NOXMQTT_CONNECTION_RC_REFUSED_SERVER_UNAVAIL:
            evt_data.evt_id = NOXMQTT_EVT_CONNECT_ERROR;
            evt_data.evt.conn_err_evt.reason = NOXMQTT_CONN_ERR_REFUSED_SERVER_UNAVAIL;
            evt_data.evt.conn_err_evt.raw_reason_code = var_hdr->conn_ack.conn_return_code;
            noxmqtt_send_event(c, &evt_data);
            break;
        case NOXMQTT_CONNECTION_RC_REFUSED_BAD_USER_PASS:
            evt_data.evt_id = NOXMQTT_EVT_CONNECT_ERROR;
            evt_data.evt.conn_err_evt.reason = NOXMQTT_CONN_ERR_REFUSED_BAD_USER_PASS;
            evt_data.evt.conn_err_evt.raw_reason_code = var_hdr->conn_ack.conn_return_code;
            noxmqtt_send_event(c, &evt_data);
            break;
        case NOXMQTT_CONNECTION_RC_REFUSED_NOT_AUTH:
            evt_data.evt_id = NOXMQTT_EVT_CONNECT_ERROR;
            evt_data.evt.conn_err_evt.reason = NOXMQTT_CONN_ERR_REFUSED_NOT_AUTH;
            evt_data.evt.conn_err_evt.raw_reason_code = var_hdr->conn_ack.conn_return_code;
            noxmqtt_send_event(c, &evt_data);
            break;
        default:
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            break;
    }
}

/**
 * @brief Handles an incoming PUBLISH packet.
 *
 * @param[in] c Client instance.
 * @param[in] data Packet buffer.
 * @param[in] len Packet length in bytes.
 */
static void noxmqtt_handler_publish(noxmqtt_client_t* c, uint8_t* data, uint16_t len)
{
    noxmqtt_evt_data_t evt_data;
    const noxmqtt_hdr_t* hdr = (const noxmqtt_hdr_t*)data;
    uint32_t remain_length = 0;
    int remain_len_bytes = 0;
    uint16_t offset = 0;
    uint16_t topic_len = 0;
    uint16_t payload_len = 0;
    const char* topic_ptr = NULL;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    MEMZERO_S(evt_data);

    remain_len_bytes = noxmqtt_decode_remain_len(&data[sizeof(noxmqtt_hdr_t)], &remain_length);
    if (remain_len_bytes < 0 || len < (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes + remain_length)) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }

    offset = (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes);
    if (remain_length < 2U || (uint16_t)(offset + 2U) > len) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }

    topic_len = (uint16_t)((data[offset] << 8) | data[offset + 1]);
    offset = (uint16_t)(offset + 2U);
    if ((uint32_t)offset + topic_len > len) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }

    topic_ptr = (const char*)&data[offset];
    evt_data.evt_id = NOXMQTT_EVT_RECEIVED;
    offset = (uint16_t)(offset + topic_len);

    evt_data.evt.received_evt.packet_identifier = 0;
    if (hdr->qos != NOXMQTT_QOS0_AT_MOST_ONCE_DELIV) {
        if ((uint16_t)(offset + NOXMQTT_PACKET_IDENT_BYTE_LEN) > len) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        evt_data.evt.received_evt.packet_identifier = (uint16_t)((data[offset] << 8) | data[offset + 1]);
        offset = (uint16_t)(offset + NOXMQTT_PACKET_IDENT_BYTE_LEN);
    }

    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        noxmqtt_mqtt5_property_view_t props;

        if (noxmqtt_mqtt5_parse_properties(data, len, &offset, &props) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }
        if (noxmqtt_mqtt5_validate_properties(NOXMQTT_CTRL_PKT_TYPE_PUBLISH, &props) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        if (props.has_topic_alias) {
            if (topic_len > 0U) {
                if (noxmqtt_topic_alias_store(c, props.topic_alias, topic_ptr, topic_len) != NOXMQTT_SUCCESS) {
                    noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
                    return;
                }
            } else {
                topic_ptr = noxmqtt_topic_alias_lookup(c, props.topic_alias, &topic_len);
                if (topic_ptr == NULL || topic_len == 0U) {
                    noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
                    return;
                }
            }
            evt_data.evt.received_evt.topic_alias = props.topic_alias;
        }

        evt_data.evt.received_evt.payload_format_indicator = props.payload_format_indicator;
        evt_data.evt.received_evt.message_expiry_interval = props.message_expiry_interval;
        evt_data.evt.received_evt.subscription_identifier = props.subscription_identifier;
        evt_data.evt.received_evt.subscription_identifiers = props.subscription_identifiers;
        evt_data.evt.received_evt.subscription_identifier_count = props.subscription_identifier_count;
        evt_data.evt.received_evt.response_topic = props.response_topic;
        evt_data.evt.received_evt.response_topic_len = props.response_topic_len;
        evt_data.evt.received_evt.correlation_data = props.correlation_data;
        evt_data.evt.received_evt.correlation_data_len = props.correlation_data_len;
        evt_data.evt.received_evt.content_type = props.content_type;
        evt_data.evt.received_evt.content_type_len = props.content_type_len;
        evt_data.evt.received_evt.user_properties = props.user_properties;
        evt_data.evt.received_evt.user_property_count = props.user_property_count;
    }

    if (offset > len) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }

    if (topic_ptr == NULL || topic_len == 0U) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }

    evt_data.evt.received_evt.topic = (char*)topic_ptr;
    evt_data.evt.received_evt.topic_len = topic_len;
    payload_len = (uint16_t)(len - offset);
    evt_data.evt.received_evt.payload = (char*)&data[offset];
    evt_data.evt.received_evt.payload_len = payload_len;

    switch (hdr->qos) {
        case NOXMQTT_QOS0_AT_MOST_ONCE_DELIV:
            break;
        case NOXMQTT_QOS1_AT_LEAST_ONCE_DELIV:
            rc = noxmqtt_puback(c, evt_data.evt.received_evt.packet_identifier);
            break;
        case NOXMQTT_QOS2_EXACTLY_ONCE_DELIV:
            rc = noxmqtt_pubrec(c, evt_data.evt.received_evt.packet_identifier);
            break;
        default:
            rc = NOXMQTT_RC_ERROR_BAD_PACKET;
            break;
    }

    if (rc != NOXMQTT_SUCCESS) {
        noxmqtt_send_error(c, rc);
    }

    noxmqtt_send_event(c, &evt_data);
}

/**
 * @brief Handles an incoming PUBACK packet.
 *
 * @param[in] c Client instance.
 * @param[in] data Packet buffer.
 * @param[in] len Packet length in bytes.
 */
static void noxmqtt_handler_puback(noxmqtt_client_t* c, uint8_t* data, uint16_t len)
{
    noxmqtt_evt_data_t evt_data;
    uint16_t packet_identifier = 0;

    MEMZERO_S(evt_data);

    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        noxmqtt_mqtt5_ack_view_t ack;
        if (noxmqtt_mqtt5_parse_ack(data, len, MQTT5_REASON_SUCCESS, &ack) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }
        if (!noxmqtt_mqtt5_is_valid_reason_code(NOXMQTT_CTRL_PKT_TYPE_PUBACK, ack.reason_code) ||
            noxmqtt_mqtt5_validate_properties(NOXMQTT_CTRL_PKT_TYPE_PUBACK, &ack.properties) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        packet_identifier = ack.packet_identifier;
        evt_data.evt.published_evt.reason_code = ack.reason_code;
        evt_data.evt.published_evt.mqtt5.reason_string = ack.properties.reason_string;
        evt_data.evt.published_evt.mqtt5.reason_string_len = ack.properties.reason_string_len;
        evt_data.evt.published_evt.mqtt5.user_properties = ack.properties.user_properties;
        evt_data.evt.published_evt.mqtt5.user_property_count = ack.properties.user_property_count;
    } else {
        uint32_t remain_length = 0;
        int remain_len_bytes = 0;
        uint16_t offset = 0;

        remain_len_bytes = noxmqtt_decode_remain_len(&data[sizeof(noxmqtt_hdr_t)], &remain_length);
        if (remain_len_bytes < 0 ||
            remain_length < 2U ||
            len < (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes + remain_length)) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        offset = (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes);
        packet_identifier = (uint16_t)((data[offset] << 8) | data[offset + 1U]);
    }

    evt_data.evt_id = NOXMQTT_EVT_PUBLISHED;
    evt_data.evt.published_evt.packet_identifier = packet_identifier;
    evt_data.evt.published_evt.packet_identified_msb = MSB(packet_identifier);
    evt_data.evt.published_evt.packet_identified_lsb = LSB(packet_identifier);
    noxmqtt_outbox_remove(c, packet_identifier);
    (void)noxmqtt_outbox_pump(c);
    noxmqtt_send_event(c, &evt_data);
}

/**
 * @brief Handles an incoming PUBREC packet.
 *
 * @param[in] c Client instance.
 * @param[in] data Packet buffer.
 * @param[in] len Packet length in bytes.
 */
static void noxmqtt_handler_pubrec(noxmqtt_client_t* c, uint8_t* data, uint16_t len)
{
    uint16_t packet_identifier = 0;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        noxmqtt_mqtt5_ack_view_t ack;
        if (noxmqtt_mqtt5_parse_ack(data, len, MQTT5_REASON_SUCCESS, &ack) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }
        if (!noxmqtt_mqtt5_is_valid_reason_code(NOXMQTT_CTRL_PKT_TYPE_PUBREC, ack.reason_code) ||
            noxmqtt_mqtt5_validate_properties(NOXMQTT_CTRL_PKT_TYPE_PUBREC, &ack.properties) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        packet_identifier = ack.packet_identifier;
        if (ack.reason_code >= 0x80U) {
            noxmqtt_outbox_remove(c, packet_identifier);
            (void)noxmqtt_outbox_pump(c);
            return;
        }
    } else {
        uint32_t remain_length = 0;
        int remain_len_bytes = 0;
        uint16_t offset = 0;

        remain_len_bytes = noxmqtt_decode_remain_len(&data[sizeof(noxmqtt_hdr_t)], &remain_length);
        if (remain_len_bytes < 0 ||
            remain_length < 2U ||
            len < (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes + remain_length)) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        offset = (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes);
        packet_identifier = (uint16_t)((data[offset] << 8) | data[offset + 1]);
    }

    {
        noxmqtt_outbox_item_t* item = noxmqtt_outbox_find(c, packet_identifier);
        if (item != NULL) {
            item->state = NOXMQTT_OUTBOX_STATE_PUBREL_SENT;
        }
    }
    rc = noxmqtt_pubrel(c, packet_identifier);
    if (rc != NOXMQTT_SUCCESS) {
        noxmqtt_send_error(c, rc);
    }
}

/**
 * @brief Handles an incoming PUBREL packet.
 *
 * @param[in] c Client instance.
 * @param[in] data Packet buffer.
 * @param[in] len Packet length in bytes.
 */
static void noxmqtt_handler_pubrel(noxmqtt_client_t* c, uint8_t* data, uint16_t len)
{
    uint16_t packet_identifier = 0;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        noxmqtt_mqtt5_ack_view_t ack;
        if (noxmqtt_mqtt5_parse_ack(data, len, MQTT5_REASON_SUCCESS, &ack) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }
        if (!noxmqtt_mqtt5_is_valid_reason_code(NOXMQTT_CTRL_PKT_TYPE_PUBREL, ack.reason_code) ||
            noxmqtt_mqtt5_validate_properties(NOXMQTT_CTRL_PKT_TYPE_PUBREL, &ack.properties) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }
        packet_identifier = ack.packet_identifier;
    } else {
        uint32_t remain_length = 0;
        int remain_len_bytes = 0;
        uint16_t offset = 0;

        remain_len_bytes = noxmqtt_decode_remain_len(&data[sizeof(noxmqtt_hdr_t)], &remain_length);
        if (remain_len_bytes < 0 ||
            remain_length < 2U ||
            len < (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes + remain_length)) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        offset = (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes);
        packet_identifier = (uint16_t)((data[offset] << 8) | data[offset + 1]);
    }

    rc = noxmqtt_pubcomp(c, packet_identifier);
    if (rc != NOXMQTT_SUCCESS) {
        noxmqtt_send_error(c, rc);
    }
}

/**
 * @brief Handles an incoming PUBCOMP packet.
 *
 * @param[in] c Client instance.
 * @param[in] data Packet buffer.
 * @param[in] len Packet length in bytes.
 */
static void noxmqtt_handler_pubcomp(noxmqtt_client_t* c, uint8_t* data, uint16_t len)
{
    uint16_t packet_identifier = 0;

    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        noxmqtt_mqtt5_ack_view_t ack;
        if (noxmqtt_mqtt5_parse_ack(data, len, MQTT5_REASON_SUCCESS, &ack) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }
        if (!noxmqtt_mqtt5_is_valid_reason_code(NOXMQTT_CTRL_PKT_TYPE_PUBCOMP, ack.reason_code) ||
            noxmqtt_mqtt5_validate_properties(NOXMQTT_CTRL_PKT_TYPE_PUBCOMP, &ack.properties) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }
        packet_identifier = ack.packet_identifier;
    } else {
        uint32_t remain_length = 0;
        int remain_len_bytes = 0;
        uint16_t offset = 0;

        remain_len_bytes = noxmqtt_decode_remain_len(&data[sizeof(noxmqtt_hdr_t)], &remain_length);
        if (remain_len_bytes < 0 ||
            remain_length < 2U ||
            len < (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes + remain_length)) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        offset = (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes);
        packet_identifier = (uint16_t)((data[offset] << 8) | data[offset + 1]);
    }

    noxmqtt_outbox_remove(c, packet_identifier);
    (void)noxmqtt_outbox_pump(c);
}

/**
 * @brief Handles an incoming SUBACK packet.
 *
 * @param[in] c Client instance.
 * @param[in] data Packet buffer.
 * @param[in] len Packet length in bytes.
 */
static void noxmqtt_handler_suback(noxmqtt_client_t* c, uint8_t* data, uint16_t len)
{
    noxmqtt_evt_data_t evt_data;
    uint32_t remain_length = 0;
    int remain_len_bytes = 0;
    uint16_t offset = 0;

    MEMZERO_S(evt_data);

    remain_len_bytes = noxmqtt_decode_remain_len(&data[sizeof(noxmqtt_hdr_t)], &remain_length);
    if (remain_len_bytes < 0 || remain_length < 3U || len < (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes + remain_length)) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }

    offset = (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes);
    evt_data.evt_id = NOXMQTT_EVT_SUBSCRIBED;
    evt_data.evt.subscribed_evt.packet_identifier = (uint16_t)((data[offset] << 8) | data[offset + 1U]);
    offset = (uint16_t)(offset + 2U);
    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        noxmqtt_mqtt5_property_view_t props;

        if (noxmqtt_mqtt5_parse_properties(data, len, &offset, &props) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }
        if (noxmqtt_mqtt5_validate_properties(NOXMQTT_CTRL_PKT_TYPE_SUBACK, &props) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        evt_data.evt.subscribed_evt.mqtt5.reason_string = props.reason_string;
        evt_data.evt.subscribed_evt.mqtt5.reason_string_len = props.reason_string_len;
        evt_data.evt.subscribed_evt.mqtt5.user_properties = props.user_properties;
        evt_data.evt.subscribed_evt.mqtt5.user_property_count = props.user_property_count;
    }
    evt_data.evt.subscribed_evt.reason_codes = &data[offset];
    evt_data.evt.subscribed_evt.reason_code_count = (uint16_t)(len - offset);
    if (!noxmqtt_mqtt5_are_valid_reason_codes(NOXMQTT_CTRL_PKT_TYPE_SUBACK,
                                              evt_data.evt.subscribed_evt.reason_codes,
                                              evt_data.evt.subscribed_evt.reason_code_count)) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }
    evt_data.evt.subscribed_evt.return_code = *(noxmqtt_suback_return_t*)&data[offset];
    noxmqtt_send_event(c, &evt_data);
}

/**
 * @brief Handles an incoming UNSUBACK packet.
 *
 * @param[in] c Client instance.
 * @param[in] data Packet buffer.
 * @param[in] len Packet length in bytes.
 */
static void noxmqtt_handler_unsuback(noxmqtt_client_t* c, uint8_t* data, uint16_t len)
{
    noxmqtt_evt_data_t evt_data;
    uint32_t remain_length = 0;
    int remain_len_bytes = 0;
    uint16_t offset = 0;

    MEMZERO_S(evt_data);

    remain_len_bytes = noxmqtt_decode_remain_len(&data[sizeof(noxmqtt_hdr_t)], &remain_length);
    if (remain_len_bytes < 0 || remain_length < 2U || len < (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes + remain_length)) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }

    offset = (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes);
    evt_data.evt_id = NOXMQTT_EVT_UNSUBSCRIBED;
    evt_data.evt.unsubscribed_evt.packet_identifier = (uint16_t)((data[offset] << 8) | data[offset + 1U]);
    evt_data.evt.unsubscribed_evt.packet_identified_msb = data[offset];
    evt_data.evt.unsubscribed_evt.packet_identified_lsb = data[offset + 1U];
    offset = (uint16_t)(offset + 2U);
    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        noxmqtt_mqtt5_property_view_t props;

        if (noxmqtt_mqtt5_parse_properties(data, len, &offset, &props) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }
        if (noxmqtt_mqtt5_validate_properties(NOXMQTT_CTRL_PKT_TYPE_UNSUBACK, &props) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        evt_data.evt.unsubscribed_evt.mqtt5.reason_string = props.reason_string;
        evt_data.evt.unsubscribed_evt.mqtt5.reason_string_len = props.reason_string_len;
        evt_data.evt.unsubscribed_evt.mqtt5.user_properties = props.user_properties;
        evt_data.evt.unsubscribed_evt.mqtt5.user_property_count = props.user_property_count;
    }
    evt_data.evt.unsubscribed_evt.reason_codes = &data[offset];
    evt_data.evt.unsubscribed_evt.reason_code_count = (uint16_t)(len - offset);
    if (!noxmqtt_mqtt5_are_valid_reason_codes(NOXMQTT_CTRL_PKT_TYPE_UNSUBACK,
                                              evt_data.evt.unsubscribed_evt.reason_codes,
                                              evt_data.evt.unsubscribed_evt.reason_code_count)) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }
    noxmqtt_send_event(c, &evt_data);
}

/**
 * @brief Handles an incoming DISCONNECT packet.
 *
 * @param[in] c Client instance.
 * @param[in] data Packet buffer.
 * @param[in] len Packet length in bytes.
 */
static void noxmqtt_handler_disconnect_packet(noxmqtt_client_t* c, uint8_t* data, uint16_t len)
{
    noxmqtt_evt_data_t evt_data;

    MEMZERO_S(evt_data);
    evt_data.evt_id = NOXMQTT_EVT_DISCONNECT;

    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        uint32_t remain_length = 0;
        int remain_len_bytes = 0;
        uint16_t offset = 0;

        remain_len_bytes = noxmqtt_decode_remain_len(&data[sizeof(noxmqtt_hdr_t)], &remain_length);
        if (remain_len_bytes < 0 || len < (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes + remain_length)) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        offset = (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes);
        evt_data.evt.disconnect_evt.reason_code = MQTT5_REASON_SUCCESS;
        if (remain_length > 0U) {
            noxmqtt_mqtt5_property_view_t props;
            evt_data.evt.disconnect_evt.reason_code = data[offset++];
            if (!noxmqtt_mqtt5_is_valid_reason_code(NOXMQTT_CTRL_PKT_TYPE_DISCONNECT,
                                                    evt_data.evt.disconnect_evt.reason_code)) {
                noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
                return;
            }
            if (offset < len && noxmqtt_mqtt5_parse_properties(data, len, &offset, &props) != NOXMQTT_SUCCESS) {
                noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
                return;
            }
            if (noxmqtt_mqtt5_validate_properties(NOXMQTT_CTRL_PKT_TYPE_DISCONNECT, &props) != NOXMQTT_SUCCESS) {
                noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
                return;
            }
            evt_data.evt.disconnect_evt.session_expiry_interval = props.session_expiry_interval;
            evt_data.evt.disconnect_evt.server_reference = props.server_reference;
            evt_data.evt.disconnect_evt.server_reference_len = props.server_reference_len;
            evt_data.evt.disconnect_evt.mqtt5.reason_string = props.reason_string;
            evt_data.evt.disconnect_evt.mqtt5.reason_string_len = props.reason_string_len;
            evt_data.evt.disconnect_evt.mqtt5.user_properties = props.user_properties;
            evt_data.evt.disconnect_evt.mqtt5.user_property_count = props.user_property_count;
        }
    }

    c->status.connected = 0;
    c->status.ping_outstanding = 0;
    c->status.auth_in_progress = 0;
    noxmqtt_reset_broker_capabilities(c);
    noxmqtt_topic_alias_cache_clear(c);
    noxmqtt_publish_topic_alias_cache_clear(c);
    noxmqtt_clear_active_auth_method(c);
    if (!c->last_conf.server.disable_auto_reconnect && c->last_conf.server.addr != NULL) {
        c->status.reconnect_pending = 1;
        c->next_reconnect_ms = noxmqtt_tal_time_ms() +
            (c->last_conf.server.reconnect_timeout_ms ? c->last_conf.server.reconnect_timeout_ms
                                                      : NOXMQTT_DEFAULT_RECONNECT_TIMEOUT_MS);
    } else {
        c->status.reconnect_pending = 0;
    }
    (void)noxmqtt_transport_disconnect(c);
    noxmqtt_send_event(c, &evt_data);
}

/**
 * @brief Handles an incoming AUTH packet.
 *
 * @param[in] c Client instance.
 * @param[in] data Packet buffer.
 * @param[in] len Packet length in bytes.
 */
static void noxmqtt_handler_auth(noxmqtt_client_t* c, uint8_t* data, uint16_t len)
{
    noxmqtt_evt_data_t evt_data;
    uint32_t remain_length = 0;
    int remain_len_bytes = 0;
    uint16_t offset = 0;
    noxmqtt_mqtt5_property_view_t props;

    MEMZERO_S(evt_data);

    if (c->last_conf.protocol_version != NOXMQTT_PROTOCOL_V5_0) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }

    remain_len_bytes = noxmqtt_decode_remain_len(&data[sizeof(noxmqtt_hdr_t)], &remain_length);
    if (remain_len_bytes < 0 || len < (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes + remain_length)) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }

    evt_data.evt_id = NOXMQTT_EVT_AUTH;
    offset = (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes);
    evt_data.evt.auth_evt.reason_code = MQTT5_REASON_SUCCESS;
    if (remain_length > 0U) {
        evt_data.evt.auth_evt.reason_code = data[offset++];
        if (offset < len && noxmqtt_mqtt5_parse_properties(data, len, &offset, &props) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }
        if (!noxmqtt_mqtt5_is_valid_reason_code(NOXMQTT_CTRL_PKT_TYPE_AUTH, evt_data.evt.auth_evt.reason_code) ||
            noxmqtt_mqtt5_validate_properties(NOXMQTT_CTRL_PKT_TYPE_AUTH, &props) != NOXMQTT_SUCCESS) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        if (props.has_auth_method) {
            if (c->active_auth_method != NULL &&
                !noxmqtt_active_auth_method_matches(c, props.auth_method, props.auth_method_len)) {
                noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
                return;
            }
            if (c->active_auth_method == NULL &&
                noxmqtt_set_active_auth_method(c, props.auth_method, props.auth_method_len) != NOXMQTT_SUCCESS) {
                noxmqtt_send_error(c, NOXMQTT_RC_ERROR_NO_MEMORY);
                return;
            }
        } else if (c->active_auth_method == NULL) {
            noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
            return;
        }

        c->status.auth_in_progress = noxmqtt_is_auth_exchange_active(evt_data.evt.auth_evt.reason_code);
        evt_data.evt.auth_evt.auth_method = props.has_auth_method ? props.auth_method : c->active_auth_method;
        evt_data.evt.auth_evt.auth_method_len = props.has_auth_method
                                                    ? props.auth_method_len
                                                    : (uint16_t)strlen(c->active_auth_method);
        evt_data.evt.auth_evt.auth_data = props.auth_data;
        evt_data.evt.auth_evt.auth_data_len = props.auth_data_len;
        evt_data.evt.auth_evt.mqtt5.reason_string = props.reason_string;
        evt_data.evt.auth_evt.mqtt5.reason_string_len = props.reason_string_len;
        evt_data.evt.auth_evt.mqtt5.user_properties = props.user_properties;
        evt_data.evt.auth_evt.mqtt5.user_property_count = props.user_property_count;
    } else if (c->active_auth_method != NULL) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }
    noxmqtt_send_event(c, &evt_data);
}

/**
 * @brief Handles an incoming PINGRESP packet.
 *
 * @param[in] c Client instance.
 * @param[in] data Packet buffer.
 * @param[in] len Packet length in bytes.
 */
static void noxmqtt_handler_pingresp(noxmqtt_client_t* c, uint8_t* data, uint16_t len)
{
    noxmqtt_evt_data_t evt_data;
    uint32_t remain_length = 0;
    int remain_len_bytes = 0;

    MEMZERO_S(evt_data);

    remain_len_bytes = noxmqtt_decode_remain_len(&data[sizeof(noxmqtt_hdr_t)], &remain_length);
    if (remain_len_bytes < 0 || remain_length != 0U || len < (uint16_t)(sizeof(noxmqtt_hdr_t) + remain_len_bytes)) {
        noxmqtt_send_error(c, NOXMQTT_RC_ERROR_BAD_PACKET);
        return;
    }

    c->status.ping_outstanding = 0;
    evt_data.evt_id = NOXMQTT_EVT_PINGRESP;
    noxmqtt_send_event(c, &evt_data);
}

/**
 * @brief Dispatches an event through the client callback.
 *
 * @param[in] c Client instance.
 * @param[in] data Event payload to deliver.
 */
static void noxmqtt_send_event(noxmqtt_client_t* c, noxmqtt_evt_data_t* data)
{
    if (c != NULL && c->callback != NULL && data != NULL) {
        c->callback(data);
    }
}

/**
 * @brief Emits an error event for a client.
 *
 * @param[in] c Client instance.
 * @param[in] rc NoxMQTT error code to report.
 */
static void noxmqtt_send_error(noxmqtt_client_t* c, noxmqtt_rc_t rc)
{
    noxmqtt_evt_data_t evt_data;

    MEMZERO_S(evt_data);
    evt_data.evt_id = NOXMQTT_EVT_ERROR;
    evt_data.evt.error_evt.rc = rc;
    noxmqtt_send_event(c, &evt_data);
}

/**
 * @brief Connects a client to an MQTT broker.
 *
 * @param[in] c Client instance.
 * @param[in] conf Connection configuration.
 * @param[in] keepalive Keepalive interval in seconds.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_connect(noxmqtt_client_t* c, noxmqtt_client_conf_t* conf, uint16_t keepalive)
{
    noxmqtt_rc_t rc = NOXMQTT_RC_ERROR;
    noxmqtt_hdr_t hdr;
    noxmqtt_connect_var_hdr_t var_hdr;
    uint16_t body_offset = 0;
    uint16_t body_len = 0;
    uint8_t* send_ptr = NULL;
    uint16_t send_len = 0;
    uint16_t effective_keepalive = keepalive;
    const char* client_identifier = NULL;

    if (noxmqtt_validate_client(c) != NOXMQTT_SUCCESS || conf == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (conf->protocol_version == 0) {
        conf->protocol_version = NOXMQTT_PROTOCOL_V3_1_1;
    }

    if (conf->protocol_version != NOXMQTT_PROTOCOL_V3_1_1 &&
        conf->protocol_version != NOXMQTT_PROTOCOL_V5_0) {
        return NOXMQTT_RC_ERROR_NOT_SUPPORTED;
    }

    if (conf != &c->last_conf) {
        noxmqtt_clear_assigned_client_identifier(c);
    }

    client_identifier = (conf->client_identifier != NULL) ? conf->client_identifier : "";

    if (noxmqttlib_validate_device_id(client_identifier) != 0) {
        return NOXMQTT_RC_ERROR_BAD_CLIENT_IDENT;
    }
    if (conf->protocol_version == NOXMQTT_PROTOCOL_V3_1_1 &&
        client_identifier[0] == '\0' &&
        conf->clean_session == 0U) {
        return NOXMQTT_RC_ERROR_BAD_CLIENT_IDENT;
    }

    if (effective_keepalive == 0U) {
        effective_keepalive = MQTT_CONN_DEFAULT_KEEPALIVE;
    }
    if ((uint8_t)conf->will_topic.qos > (uint8_t)NOXMQTT_QOS2_EXACTLY_ONCE_DELIV) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (conf->protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        if (conf->mqtt5.request_response_info > 1U || conf->mqtt5.request_problem_info > 1U) {
            return NOXMQTT_RC_ERROR_BAD_PACKET;
        }
        if (conf->mqtt5.auth_data_len > 0U &&
            (conf->mqtt5.auth_data == NULL || conf->mqtt5.auth_method == NULL || conf->mqtt5.auth_method[0] == '\0')) {
            return NOXMQTT_RC_ERROR_BAD_PACKET;
        }
        if (conf->will_topic.mqtt5.payload_format_indicator > 1U) {
            return NOXMQTT_RC_ERROR_BAD_PACKET;
        }
    }

    MEMZERO_S(hdr);
    MEMZERO_S(var_hdr);
    noxmqtt_clear_tx_buf(c);

    if (conf->callback != NULL) {
        c->callback = conf->callback;
    }

    c->last_conf = *conf;
    c->last_conf.client_identifier = client_identifier;
    c->keepalive = effective_keepalive;
    c->status.connected = 0;
    c->status.ping_outstanding = 0;
    c->status.reconnect_pending = 0;
    c->status.auth_in_progress = 0;
    noxmqtt_reset_broker_capabilities(c);
    noxmqtt_topic_alias_cache_clear(c);
    noxmqtt_publish_topic_alias_cache_clear(c);
    noxmqtt_clear_active_auth_method(c);
    if (conf->protocol_version == NOXMQTT_PROTOCOL_V5_0 &&
        conf->mqtt5.auth_method != NULL &&
        conf->mqtt5.auth_method[0] != '\0') {
        rc = noxmqtt_set_active_auth_method(c, conf->mqtt5.auth_method, (uint16_t)strlen(conf->mqtt5.auth_method));
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
        c->status.auth_in_progress = 1;
    }

    hdr.type = NOXMQTT_CTRL_PKT_TYPE_CONNECT;

    var_hdr.name_len_msb = MSB(MQTT_CONN_PROTOCOL_NAME_LEN);
    var_hdr.name_len_lsb = LSB(MQTT_CONN_PROTOCOL_NAME_LEN);
    memcpy(var_hdr.name_val, MQTT_CONN_PROTOCOL_NAME, MQTT_CONN_PROTOCOL_NAME_LEN);
    var_hdr.level_val = (uint8_t)conf->protocol_version;
    var_hdr.keepalive_msb = MSB(effective_keepalive);
    var_hdr.keepalive_lsb = LSB(effective_keepalive);
    var_hdr.flag_clean_session = conf->clean_session ? 1U : 0U;
    var_hdr.flag_user_name = (conf->auth.username != NULL && strlen(conf->auth.username) > 0U) ? 1U : 0U;
    var_hdr.flag_password = (conf->auth.password != NULL && strlen(conf->auth.password) > 0U) ? 1U : 0U;

    if ((conf->will_topic.topic != NULL && strlen(conf->will_topic.topic) > 0U) &&
        ((conf->will_topic.payload != NULL && conf->will_topic.payload_len > 0U) ||
         (conf->will_topic.msg != NULL && strlen(conf->will_topic.msg) > 0U))) {
        var_hdr.flag_will = 1U;
        var_hdr.flag_will_qos = (uint8_t)(conf->will_topic.qos & 0x03);
        var_hdr.flag_will_retain = (uint8_t)(conf->will_topic.retain & 0x01);
    }

    body_offset = NOXMQTT_FIXED_HEADER_MAX_LEN;
    memcpy(&c->tx_buf[body_offset], &var_hdr, sizeof(var_hdr));
    body_offset = (uint16_t)(body_offset + sizeof(var_hdr));

    if (conf->protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        uint8_t props_buf[128];
        uint16_t props_len = 0;

        rc = noxmqtt_mqtt5_encode_connect_properties(props_buf, sizeof(props_buf), conf, &props_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }

        rc = noxmqtt_write_varint(c->tx_buf, c->tx_buf_size, &body_offset, props_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }

        rc = noxmqtt_write_bytes(c->tx_buf, c->tx_buf_size, &body_offset, props_buf, props_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    rc = noxmqtt_write_utf8_string(c->tx_buf, c->tx_buf_size, &body_offset, client_identifier);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    if (var_hdr.flag_will) {
        if (conf->protocol_version == NOXMQTT_PROTOCOL_V5_0) {
            uint8_t will_props_buf[192];
            uint16_t will_props_len = 0;

            rc = noxmqtt_mqtt5_encode_will_properties(will_props_buf, sizeof(will_props_buf), conf, &will_props_len);
            if (rc != NOXMQTT_SUCCESS) {
                return rc;
            }

            rc = noxmqtt_write_varint(c->tx_buf, c->tx_buf_size, &body_offset, will_props_len);
            if (rc != NOXMQTT_SUCCESS) {
                return rc;
            }

            rc = noxmqtt_write_bytes(c->tx_buf, c->tx_buf_size, &body_offset, will_props_buf, will_props_len);
            if (rc != NOXMQTT_SUCCESS) {
                return rc;
            }
        }

        rc = noxmqtt_write_utf8_string(c->tx_buf, c->tx_buf_size, &body_offset, conf->will_topic.topic);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }

        if (conf->will_topic.payload != NULL && conf->will_topic.payload_len > 0U) {
            rc = noxmqtt_write_u16(c->tx_buf, c->tx_buf_size, &body_offset, conf->will_topic.payload_len);
            if (rc != NOXMQTT_SUCCESS) {
                return rc;
            }
            rc = noxmqtt_write_bytes(c->tx_buf, c->tx_buf_size, &body_offset, conf->will_topic.payload, conf->will_topic.payload_len);
        } else {
            rc = noxmqtt_write_utf8_string(c->tx_buf, c->tx_buf_size, &body_offset, conf->will_topic.msg);
        }
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    if (var_hdr.flag_user_name) {
        rc = noxmqtt_write_utf8_string(c->tx_buf, c->tx_buf_size, &body_offset, conf->auth.username);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    if (var_hdr.flag_password) {
        rc = noxmqtt_write_utf8_string(c->tx_buf, c->tx_buf_size, &body_offset, conf->auth.password);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    body_len = (uint16_t)(body_offset - NOXMQTT_FIXED_HEADER_MAX_LEN);
    rc = noxmqtt_finalize_packet(c, hdr, body_len, &send_ptr, &send_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    if (noxmqtt_transport_init(c, noxmqtt_transport_rcv_func) != 0) {
        return NOXMQTT_RC_ERROR_TRANSPORT;
    }

    if (noxmqtt_transport_connect(c, conf) != 0) {
        return NOXMQTT_RC_ERROR_TRANSPORT;
    }

    c->last_tx_ms = noxmqtt_tal_time_ms();
    c->last_rx_ms = c->last_tx_ms;
    return noxmqtt_transport_write_packet(c, send_ptr, send_len);
}

/**
 * @brief Publishes a binary payload.
 *
 * @param[in] c Client instance.
 * @param[in] qos Requested QoS level.
 * @param[in] retain Retain flag.
 * @param[in] dup DUP flag.
 * @param[in] topic Topic name.
 * @param[in] payload Payload buffer.
 * @param[in] payload_len Payload length in bytes.
 * @param[in] props Optional MQTT 5 publish properties.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_publish_data(noxmqtt_client_t* c,
                                  noxmqtt_qos_t qos,
                                  uint8_t retain,
                                  uint8_t dup,
                                  char* topic,
                                  const uint8_t* payload,
                                  uint16_t payload_len,
                                  const noxmqtt_mqtt5_publish_props_t* props)
{
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;
    uint16_t packet_identifier = 0;
    noxmqtt_outbox_state_t initial_state = NOXMQTT_OUTBOX_STATE_FREE;
    const char* outbox_topic = topic;

    if (noxmqtt_validate_client(c) != NOXMQTT_SUCCESS) {
        return NOXMQTT_RC_ERROR_NOT_INIT;
    }

    if (!c->status.connected) {
        return NOXMQTT_RC_ERROR_NOT_CONNECTED;
    }
    if ((uint8_t)qos > (uint8_t)NOXMQTT_QOS2_EXACTLY_ONCE_DELIV) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    if (topic == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }
    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        if (props != NULL && props->payload_format_indicator > 1U) {
            return NOXMQTT_RC_ERROR_BAD_PACKET;
        }
        if ((uint8_t)qos > c->broker_maximum_qos) {
            return NOXMQTT_RC_ERROR_NOT_SUPPORTED;
        }
        if (retain && !c->broker_retain_available) {
            return NOXMQTT_RC_ERROR_NOT_SUPPORTED;
        }
        if (props != NULL && props->topic_alias > 0U &&
            (c->broker_topic_alias_maximum == 0U || props->topic_alias > c->broker_topic_alias_maximum)) {
            return NOXMQTT_RC_ERROR_OVERFLOW;
        }
        if (topic[0] == '\0') {
            if (props == NULL || props->topic_alias == 0U) {
                return NOXMQTT_RC_ERROR_BAD_PACKET;
            }
            outbox_topic = noxmqtt_publish_topic_alias_lookup(c, props->topic_alias);
            if (outbox_topic == NULL || outbox_topic[0] == '\0') {
                return NOXMQTT_RC_ERROR_BAD_PACKET;
            }
        }
    } else if (topic[0] == '\0') {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0 &&
        qos != NOXMQTT_QOS0_AT_MOST_ONCE_DELIV &&
        c->broker_receive_maximum > 0U &&
        noxmqtt_outbox_inflight_count(c) >= c->broker_receive_maximum) {
        initial_state = NOXMQTT_OUTBOX_STATE_QUEUED;
    }

    if (qos == NOXMQTT_QOS1_AT_LEAST_ONCE_DELIV || qos == NOXMQTT_QOS2_EXACTLY_ONCE_DELIV) {
        packet_identifier = c->packet_ident;
        if (initial_state == NOXMQTT_OUTBOX_STATE_FREE) {
            initial_state = NOXMQTT_OUTBOX_STATE_PUBLISH_SENT;
        }
        rc = noxmqtt_outbox_store(c, packet_identifier, (char*)outbox_topic, payload, payload_len, qos, retain, props, initial_state);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
        c->packet_ident++;
    }

    if (initial_state == NOXMQTT_OUTBOX_STATE_QUEUED) {
        return NOXMQTT_SUCCESS;
    }

    rc = noxmqtt_publish_internal(c, qos, retain, dup, topic, payload, payload_len, packet_identifier, props);
    if (rc != NOXMQTT_SUCCESS && packet_identifier != 0U) {
        noxmqtt_outbox_remove(c, packet_identifier);
    }

    return rc;
}

/**
 * @brief Publishes a null-terminated string payload.
 *
 * @param[in] c Client instance.
 * @param[in] qos Requested QoS level.
 * @param[in] retain Retain flag.
 * @param[in] dup DUP flag.
 * @param[in] topic Topic name.
 * @param[in] msg Null-terminated message string.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_publish(noxmqtt_client_t* c,
                             noxmqtt_qos_t qos,
                             uint8_t retain,
                             uint8_t dup,
                             char* topic,
                             char* msg)
{
    const uint8_t* payload = NULL;
    uint16_t payload_len = 0;

    if (msg != NULL) {
        payload = (const uint8_t*)msg;
        payload_len = (uint16_t)strlen(msg);
    }

    return noxmqtt_publish_data(c, qos, retain, dup, topic, payload, payload_len, NULL);
}

/**
 * @brief Subscribes to one or more topics.
 *
 * @param[in] c Client instance.
 * @param[in] topics Topic subscription array.
 * @param[in] topic_cnt Number of topic entries.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_subscribe(noxmqtt_client_t* c, const noxmqtt_topic_sub_t* topics, uint8_t topic_cnt)
{
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;
    noxmqtt_hdr_t hdr;
    uint16_t offset = NOXMQTT_FIXED_HEADER_MAX_LEN;
    uint16_t body_len = 0;
    uint8_t* send_ptr = NULL;
    uint16_t send_len = 0;
    uint8_t i = 0;

    if (noxmqtt_validate_client(c) != NOXMQTT_SUCCESS || topics == NULL || topic_cnt == 0U) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (!c->status.connected) {
        return NOXMQTT_RC_ERROR_NOT_CONNECTED;
    }

    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        if (topics[0].mqtt5.subscribe_identifier > 0U &&
            !c->broker_subscription_identifiers_available) {
            return NOXMQTT_RC_ERROR_NOT_SUPPORTED;
        }
    }

    for (i = 0; i < topic_cnt; i++) {
        if (topics[i].topic == NULL || topics[i].topic[0] == '\0') {
            return NOXMQTT_RC_ERROR_NULL;
        }
        if ((uint8_t)topics[i].qos > (uint8_t)NOXMQTT_QOS2_EXACTLY_ONCE_DELIV) {
            return NOXMQTT_RC_ERROR_BAD_PACKET;
        }
        if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
            if (topics[i].mqtt5.no_local > 1U || topics[i].mqtt5.retain_as_published > 1U) {
                return NOXMQTT_RC_ERROR_BAD_PACKET;
            }
            if (topics[i].mqtt5.retain_handling > 2U) {
                return NOXMQTT_RC_ERROR_BAD_PACKET;
            }
            if (topics[i].mqtt5.subscribe_identifier != topics[0].mqtt5.subscribe_identifier) {
                return NOXMQTT_RC_ERROR_BAD_PACKET;
            }
            if ((uint8_t)topics[i].qos > c->broker_maximum_qos) {
                return NOXMQTT_RC_ERROR_NOT_SUPPORTED;
            }
            if (noxmqtt_topic_is_shared_subscription(topics[i].topic) &&
                !c->broker_shared_subscriptions_available) {
                return NOXMQTT_RC_ERROR_NOT_SUPPORTED;
            }
            if (noxmqtt_topic_is_shared_subscription(topics[i].topic) &&
                topics[i].mqtt5.no_local) {
                return NOXMQTT_RC_ERROR_BAD_PACKET;
            }
            if (noxmqtt_topic_has_wildcard(topics[i].topic) &&
                !c->broker_wildcard_subscriptions_available) {
                return NOXMQTT_RC_ERROR_NOT_SUPPORTED;
            }
        }
    }

    MEMZERO_S(hdr);
    noxmqtt_clear_tx_buf(c);

    hdr.type = NOXMQTT_CTRL_PKT_TYPE_SUBSCRIBE;
    rc = noxmqtt_write_u16(c->tx_buf, c->tx_buf_size, &offset, c->packet_ident);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }
    c->packet_ident++;

    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        uint8_t props_buf[32];
        uint16_t props_len = 0;

        rc = noxmqtt_mqtt5_encode_subscribe_properties(props_buf, sizeof(props_buf), &topics[0].mqtt5, &props_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }

        rc = noxmqtt_write_varint(c->tx_buf, c->tx_buf_size, &offset, props_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }

        rc = noxmqtt_write_bytes(c->tx_buf, c->tx_buf_size, &offset, props_buf, props_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    for (i = 0; i < topic_cnt; i++) {
        rc = noxmqtt_subscription_cache_add(c, &topics[i]);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }

        rc = noxmqtt_write_utf8_string(c->tx_buf, c->tx_buf_size, &offset, topics[i].topic);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }

        {
            uint8_t sub_options = (uint8_t)(topics[i].qos & 0x03U);
            if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
                sub_options |= (uint8_t)((topics[i].mqtt5.no_local & 0x01U) << 2);
                sub_options |= (uint8_t)((topics[i].mqtt5.retain_as_published & 0x01U) << 3);
                sub_options |= (uint8_t)((topics[i].mqtt5.retain_handling & 0x03U) << 4);
            }

            rc = noxmqtt_write_bytes(c->tx_buf,
                                     c->tx_buf_size,
                                     &offset,
                                     &sub_options,
                                     1U);
        }
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    body_len = (uint16_t)(offset - NOXMQTT_FIXED_HEADER_MAX_LEN);
    rc = noxmqtt_finalize_packet(c, hdr, body_len, &send_ptr, &send_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    send_ptr[0] |= 0x02U;
    return noxmqtt_transport_write_packet(c, send_ptr, send_len);
}

/**
 * @brief Unsubscribes from one or more topics.
 *
 * @param[in] c Client instance.
 * @param[in] topics Topic subscription array.
 * @param[in] topic_cnt Number of topic entries.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_unsubscribe(noxmqtt_client_t* c, const noxmqtt_topic_sub_t* topics, uint8_t topic_cnt)
{
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;
    noxmqtt_hdr_t hdr;
    uint16_t offset = NOXMQTT_FIXED_HEADER_MAX_LEN;
    uint16_t body_len = 0;
    uint8_t* send_ptr = NULL;
    uint16_t send_len = 0;
    uint8_t i = 0;

    if (noxmqtt_validate_client(c) != NOXMQTT_SUCCESS || topics == NULL || topic_cnt == 0U) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (!c->status.connected) {
        return NOXMQTT_RC_ERROR_NOT_CONNECTED;
    }

    MEMZERO_S(hdr);
    noxmqtt_clear_tx_buf(c);

    hdr.type = NOXMQTT_CTRL_PKT_TYPE_UNSUBSCRIBE;
    rc = noxmqtt_write_u16(c->tx_buf, c->tx_buf_size, &offset, c->packet_ident);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }
    c->packet_ident++;

    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        rc = noxmqtt_write_varint(c->tx_buf, c->tx_buf_size, &offset, 0U);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    for (i = 0; i < topic_cnt; i++) {
        noxmqtt_subscription_cache_remove(c, topics[i].topic);
        rc = noxmqtt_write_utf8_string(c->tx_buf, c->tx_buf_size, &offset, topics[i].topic);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    body_len = (uint16_t)(offset - NOXMQTT_FIXED_HEADER_MAX_LEN);
    rc = noxmqtt_finalize_packet(c, hdr, body_len, &send_ptr, &send_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    send_ptr[0] |= 0x02U;
    return noxmqtt_transport_write_packet(c, send_ptr, send_len);
}

/**
 * @brief Sends a PUBACK packet.
 *
 * @param[in] c Client instance.
 * @param[in] identifier Packet identifier to acknowledge.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_puback(noxmqtt_client_t* c, uint16_t identifier)
{
    noxmqtt_hdr_t hdr;
    uint16_t offset = NOXMQTT_FIXED_HEADER_MAX_LEN;
    uint16_t body_len = 0;
    uint8_t* send_ptr = NULL;
    uint16_t send_len = 0;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    MEMZERO_S(hdr);
    noxmqtt_clear_tx_buf(c);
    hdr.type = NOXMQTT_CTRL_PKT_TYPE_PUBACK;

    rc = noxmqtt_write_u16(c->tx_buf, c->tx_buf_size, &offset, identifier);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    body_len = (uint16_t)(offset - NOXMQTT_FIXED_HEADER_MAX_LEN);
    rc = noxmqtt_finalize_packet(c, hdr, body_len, &send_ptr, &send_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    return noxmqtt_transport_write_packet(c, send_ptr, send_len);
}

/**
 * @brief Sends a PUBREC packet.
 *
 * @param[in] c Client instance.
 * @param[in] identifier Packet identifier to acknowledge.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_pubrec(noxmqtt_client_t* c, uint16_t identifier)
{
    noxmqtt_hdr_t hdr;
    uint16_t offset = NOXMQTT_FIXED_HEADER_MAX_LEN;
    uint16_t body_len = 0;
    uint8_t* send_ptr = NULL;
    uint16_t send_len = 0;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    MEMZERO_S(hdr);
    noxmqtt_clear_tx_buf(c);
    hdr.type = NOXMQTT_CTRL_PKT_TYPE_PUBREC;

    rc = noxmqtt_write_u16(c->tx_buf, c->tx_buf_size, &offset, identifier);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    body_len = (uint16_t)(offset - NOXMQTT_FIXED_HEADER_MAX_LEN);
    rc = noxmqtt_finalize_packet(c, hdr, body_len, &send_ptr, &send_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    return noxmqtt_transport_write_packet(c, send_ptr, send_len);
}

/**
 * @brief Sends a PUBREL packet.
 *
 * @param[in] c Client instance.
 * @param[in] identifier Packet identifier to acknowledge.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_pubrel(noxmqtt_client_t* c, uint16_t identifier)
{
    noxmqtt_hdr_t hdr;
    uint16_t offset = NOXMQTT_FIXED_HEADER_MAX_LEN;
    uint16_t body_len = 0;
    uint8_t* send_ptr = NULL;
    uint16_t send_len = 0;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    MEMZERO_S(hdr);
    noxmqtt_clear_tx_buf(c);
    hdr.type = NOXMQTT_CTRL_PKT_TYPE_PUBREL;

    rc = noxmqtt_write_u16(c->tx_buf, c->tx_buf_size, &offset, identifier);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    body_len = (uint16_t)(offset - NOXMQTT_FIXED_HEADER_MAX_LEN);
    rc = noxmqtt_finalize_packet(c, hdr, body_len, &send_ptr, &send_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    send_ptr[0] |= 0x02U;
    return noxmqtt_transport_write_packet(c, send_ptr, send_len);
}

/**
 * @brief Sends a PUBCOMP packet.
 *
 * @param[in] c Client instance.
 * @param[in] identifier Packet identifier to acknowledge.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_pubcomp(noxmqtt_client_t* c, uint16_t identifier)
{
    noxmqtt_hdr_t hdr;
    uint16_t offset = NOXMQTT_FIXED_HEADER_MAX_LEN;
    uint16_t body_len = 0;
    uint8_t* send_ptr = NULL;
    uint16_t send_len = 0;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    MEMZERO_S(hdr);
    noxmqtt_clear_tx_buf(c);
    hdr.type = NOXMQTT_CTRL_PKT_TYPE_PUBCOMP;

    rc = noxmqtt_write_u16(c->tx_buf, c->tx_buf_size, &offset, identifier);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    body_len = (uint16_t)(offset - NOXMQTT_FIXED_HEADER_MAX_LEN);
    rc = noxmqtt_finalize_packet(c, hdr, body_len, &send_ptr, &send_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    return noxmqtt_transport_write_packet(c, send_ptr, send_len);
}

/**
 * @brief Sends a PINGREQ packet.
 *
 * @param[in] c Client instance.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_pingreq(noxmqtt_client_t* c)
{
    return noxmqtt_send_simple_packet(c, NOXMQTT_CTRL_PKT_TYPE_PINGREQ, 0U);
}

/**
 * @brief Builds and sends an MQTT PUBLISH packet.
 *
 * @param[in] c Client instance.
 * @param[in] qos Requested QoS level.
 * @param[in] retain Retain flag.
 * @param[in] dup DUP flag.
 * @param[in] topic Topic name.
 * @param[in] payload Payload buffer.
 * @param[in] payload_len Payload length in bytes.
 * @param[in] packet_identifier Packet identifier for QoS flows.
 * @param[in] props Optional MQTT 5 publish properties.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_publish_internal(noxmqtt_client_t* c,
                                             noxmqtt_qos_t qos,
                                             uint8_t retain,
                                             uint8_t dup,
                                             const char* topic,
                                             const uint8_t* payload,
                                             uint16_t payload_len,
                                             uint16_t packet_identifier,
                                             const noxmqtt_mqtt5_publish_props_t* props)
{
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;
    noxmqtt_hdr_t hdr;
    uint16_t offset = NOXMQTT_FIXED_HEADER_MAX_LEN;
    uint16_t body_len = 0;
    uint8_t* send_ptr = NULL;
    uint16_t send_len = 0;
    noxmqtt_mqtt5_publish_props_t effective_props;
    const noxmqtt_mqtt5_publish_props_t* props_to_send = props;
    const char* topic_to_send = topic;

    MEMZERO_S(hdr);
    noxmqtt_clear_tx_buf(c);
    MEMZERO_S(effective_props);

    hdr.type = NOXMQTT_CTRL_PKT_TYPE_PUBLISH;
    hdr.dup = (uint8_t)(dup & 0x01);
    hdr.qos = (uint8_t)(qos & 0x03);
    hdr.retain = (uint8_t)(retain & 0x01);

    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        if ((uint8_t)qos > c->broker_maximum_qos) {
            return NOXMQTT_RC_ERROR_NOT_SUPPORTED;
        }
        if (retain && !c->broker_retain_available) {
            return NOXMQTT_RC_ERROR_NOT_SUPPORTED;
        }
        if (effective_props.payload_format_indicator > 1U) {
            return NOXMQTT_RC_ERROR_BAD_PACKET;
        }

        uint16_t topic_alias;

        if (props != NULL) {
            effective_props = *props;
        }
        if (effective_props.payload_format_indicator > 1U) {
            return NOXMQTT_RC_ERROR_BAD_PACKET;
        }

        if (effective_props.topic_alias > 0U) {
            if (c->broker_topic_alias_maximum == 0U || effective_props.topic_alias > c->broker_topic_alias_maximum) {
                return NOXMQTT_RC_ERROR_OVERFLOW;
            }
            topic_alias = effective_props.topic_alias;
            if (topic != NULL && topic[0] != '\0') {
                if (noxmqtt_publish_topic_alias_store(c, topic_alias, topic) != NOXMQTT_SUCCESS) {
                    return NOXMQTT_RC_ERROR_OVERFLOW;
                }
            } else if (noxmqtt_publish_topic_alias_lookup(c, topic_alias) == NULL) {
                return NOXMQTT_RC_ERROR_BAD_PACKET;
            }
        } else if (topic != NULL && topic[0] != '\0' && c->broker_topic_alias_maximum > 0U) {
            topic_alias = noxmqtt_publish_topic_alias_find(c, topic);
            if (topic_alias > 0U) {
                effective_props.topic_alias = topic_alias;
                topic_to_send = "";
            } else {
                topic_alias = noxmqtt_publish_topic_alias_assign(c, topic);
                if (topic_alias > 0U) {
                    effective_props.topic_alias = topic_alias;
                }
            }
        }

        props_to_send = &effective_props;
    }
    if (topic_to_send == NULL || (topic_to_send[0] == '\0' &&
        (c->last_conf.protocol_version != NOXMQTT_PROTOCOL_V5_0 || props_to_send == NULL || props_to_send->topic_alias == 0U))) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    rc = noxmqtt_write_utf8_string(c->tx_buf, c->tx_buf_size, &offset, topic_to_send);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    if (packet_identifier != 0U) {
        rc = noxmqtt_write_u16(c->tx_buf, c->tx_buf_size, &offset, packet_identifier);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
        uint8_t props_buf[192];
        uint16_t props_len = 0U;

        rc = noxmqtt_mqtt5_encode_publish_properties(props_buf, sizeof(props_buf), props_to_send, &props_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
        rc = noxmqtt_write_varint(c->tx_buf, c->tx_buf_size, &offset, props_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }

        rc = noxmqtt_write_bytes(c->tx_buf, c->tx_buf_size, &offset, props_buf, props_len);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    rc = noxmqtt_write_bytes(c->tx_buf, c->tx_buf_size, &offset, payload, payload_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    body_len = (uint16_t)(offset - NOXMQTT_FIXED_HEADER_MAX_LEN);
    rc = noxmqtt_finalize_packet(c, hdr, body_len, &send_ptr, &send_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    return noxmqtt_transport_write_packet(c, send_ptr, send_len);
}

/**
 * @brief Sends a packet that only contains a fixed header.
 *
 * @param[in] c Client instance.
 * @param[in] type MQTT control packet type.
 * @param[in] flags Control packet flags to apply.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_send_simple_packet(noxmqtt_client_t* c, noxmqtt_ctrl_pkt_type_t type, uint8_t flags)
{
    noxmqtt_hdr_t hdr;
    uint8_t* send_ptr = NULL;
    uint16_t send_len = 0;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    if (noxmqtt_validate_client(c) != NOXMQTT_SUCCESS) {
        return NOXMQTT_RC_ERROR_NOT_INIT;
    }

    MEMZERO_S(hdr);
    noxmqtt_clear_tx_buf(c);

    hdr.type = type;
    rc = noxmqtt_finalize_packet(c, hdr, 0U, &send_ptr, &send_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    send_ptr[0] |= flags;
    return noxmqtt_transport_write_packet(c, send_ptr, send_len);
}

/**
 * @brief Disconnects from the broker.
 *
 * @param[in] c Client instance.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_disconnect(noxmqtt_client_t* c)
{
    return noxmqtt_disconnect_ex(c, NULL);
}

/**
 * @brief Disconnects from the broker with MQTT 5 properties.
 *
 * @param[in] c Client instance.
 * @param[in] props Optional MQTT 5 disconnect properties.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_disconnect_ex(noxmqtt_client_t* c, const noxmqtt_mqtt5_disconnect_props_t* props)
{
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

    if (noxmqtt_validate_client(c) != NOXMQTT_SUCCESS) {
        return NOXMQTT_RC_ERROR_NOT_INIT;
    }

    if (c->status.connected) {
        if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
            noxmqtt_hdr_t hdr;
            uint16_t offset = NOXMQTT_FIXED_HEADER_MAX_LEN;
            uint8_t* send_ptr = NULL;
            uint16_t send_len = 0;
            uint8_t props_buf[128];
            uint16_t props_len = 0;
            uint8_t reason_code = MQTT5_REASON_SUCCESS;

            if (props != NULL) {
                reason_code = props->reason_code;
                if (!noxmqtt_mqtt5_is_valid_reason_code(NOXMQTT_CTRL_PKT_TYPE_DISCONNECT, reason_code)) {
                    return NOXMQTT_RC_ERROR_BAD_PACKET;
                }
                rc = noxmqtt_mqtt5_encode_disconnect_properties(props_buf, sizeof(props_buf), props, &props_len);
                if (rc != NOXMQTT_SUCCESS) {
                    return rc;
                }
            }

            if (reason_code == MQTT5_REASON_SUCCESS && props_len == 0U) {
                rc = noxmqtt_send_simple_packet(c, NOXMQTT_CTRL_PKT_TYPE_DISCONNECT, 0U);
            } else {
                MEMZERO_S(hdr);
                noxmqtt_clear_tx_buf(c);
                hdr.type = NOXMQTT_CTRL_PKT_TYPE_DISCONNECT;

                rc = noxmqtt_write_bytes(c->tx_buf, c->tx_buf_size, &offset, &reason_code, 1U);
                if (rc != NOXMQTT_SUCCESS) {
                    return rc;
                }
                rc = noxmqtt_write_varint(c->tx_buf, c->tx_buf_size, &offset, props_len);
                if (rc != NOXMQTT_SUCCESS) {
                    return rc;
                }
                rc = noxmqtt_write_bytes(c->tx_buf, c->tx_buf_size, &offset, props_buf, props_len);
                if (rc != NOXMQTT_SUCCESS) {
                    return rc;
                }

                uint16_t body_len = (uint16_t)(offset - NOXMQTT_FIXED_HEADER_MAX_LEN);
                rc = noxmqtt_finalize_packet(c, hdr, body_len, &send_ptr, &send_len);
                if (rc != NOXMQTT_SUCCESS) {
                    return rc;
                }
                rc = noxmqtt_transport_write_packet(c, send_ptr, send_len);
            }
        } else {
            rc = noxmqtt_send_simple_packet(c, NOXMQTT_CTRL_PKT_TYPE_DISCONNECT, 0U);
        }
    }

    (void)noxmqtt_transport_disconnect(c);
    c->status.connected = 0;
    c->status.ping_outstanding = 0;
    c->status.reconnect_pending = 0;
    c->status.auth_in_progress = 0;
    noxmqtt_reset_broker_capabilities(c);
    noxmqtt_topic_alias_cache_clear(c);
    noxmqtt_publish_topic_alias_cache_clear(c);
    noxmqtt_clear_active_auth_method(c);

    return rc;
}

/**
 * @brief Sends an MQTT 5 AUTH packet.
 *
 * @param[in] c Client instance.
 * @param[in] props MQTT 5 AUTH properties to send.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_auth(noxmqtt_client_t* c, const noxmqtt_mqtt5_auth_props_t* props)
{
    noxmqtt_hdr_t hdr;
    uint16_t offset = NOXMQTT_FIXED_HEADER_MAX_LEN;
    uint16_t body_len = 0;
    uint8_t* send_ptr = NULL;
    uint16_t send_len = 0;
    uint8_t props_buf[128];
    uint16_t props_len = 0;
    uint8_t reason_code = MQTT5_REASON_SUCCESS;
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;
    noxmqtt_mqtt5_auth_props_t effective_props;

    if (noxmqtt_validate_client(c) != NOXMQTT_SUCCESS) {
        return NOXMQTT_RC_ERROR_NOT_INIT;
    }
    if (!c->status.connected) {
        return NOXMQTT_RC_ERROR_NOT_CONNECTED;
    }
    if (c->last_conf.protocol_version != NOXMQTT_PROTOCOL_V5_0) {
        return NOXMQTT_RC_ERROR_NOT_SUPPORTED;
    }
    if (props == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }
    effective_props = *props;
    if (props->reason_code != MQTT5_REASON_CONTINUE_AUTHENTICATION &&
        props->reason_code != MQTT5_REASON_REAUTHENTICATE) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (props->auth_data_len > 0U && props->auth_data == NULL) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (c->status.auth_in_progress && props->reason_code != MQTT5_REASON_CONTINUE_AUTHENTICATION) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }
    if (!c->status.auth_in_progress && props->reason_code == MQTT5_REASON_CONTINUE_AUTHENTICATION &&
        c->active_auth_method == NULL &&
        (props->auth_method == NULL || props->auth_method[0] == '\0')) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    if (c->active_auth_method != NULL) {
        if (props->auth_method != NULL &&
            !noxmqtt_active_auth_method_matches(c, props->auth_method, (uint16_t)strlen(props->auth_method))) {
            return NOXMQTT_RC_ERROR_BAD_PACKET;
        }
        if (effective_props.auth_method == NULL || effective_props.auth_method[0] == '\0') {
            effective_props.auth_method = c->active_auth_method;
        }
    } else {
        if (props->auth_method == NULL || props->auth_method[0] == '\0') {
            return NOXMQTT_RC_ERROR_BAD_PACKET;
        }
        rc = noxmqtt_set_active_auth_method(c, props->auth_method, (uint16_t)strlen(props->auth_method));
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
        effective_props.auth_method = c->active_auth_method;
    }

    reason_code = effective_props.reason_code;
    rc = noxmqtt_mqtt5_encode_auth_properties(props_buf, sizeof(props_buf), &effective_props, &props_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    MEMZERO_S(hdr);
    noxmqtt_clear_tx_buf(c);
    hdr.type = NOXMQTT_CTRL_PKT_TYPE_AUTH;
    rc = noxmqtt_write_bytes(c->tx_buf, c->tx_buf_size, &offset, &reason_code, 1U);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }
    rc = noxmqtt_write_varint(c->tx_buf, c->tx_buf_size, &offset, props_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }
    rc = noxmqtt_write_bytes(c->tx_buf, c->tx_buf_size, &offset, props_buf, props_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    body_len = (uint16_t)(offset - NOXMQTT_FIXED_HEADER_MAX_LEN);
    rc = noxmqtt_finalize_packet(c, hdr, body_len, &send_ptr, &send_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    c->status.auth_in_progress = noxmqtt_is_auth_exchange_active(reason_code);
    return noxmqtt_transport_write_packet(c, send_ptr, send_len);
}

/**
 * @brief Runs periodic keepalive, reconnect, and queued-send processing.
 *
 * @param[in] c Client instance.
 *
 * @return NoxMQTT status code.
 */
noxmqtt_rc_t noxmqtt_process(noxmqtt_client_t* c)
{
    uint32_t now = 0;
    uint32_t keepalive_ms = 0;
    uint32_t last_activity = 0;

    if (noxmqtt_validate_client(c) != NOXMQTT_SUCCESS) {
        return NOXMQTT_RC_ERROR_NOT_INIT;
    }

    now = noxmqtt_tal_time_ms();

    if (!c->status.connected) {
        if (c->status.reconnect_pending &&
            !c->last_conf.server.disable_auto_reconnect &&
            c->last_conf.server.addr != NULL &&
            now >= c->next_reconnect_ms) {
            c->status.reconnect_pending = 0;
            return noxmqtt_connect(c, &c->last_conf, c->keepalive);
        }
        return NOXMQTT_SUCCESS;
    }

    {
        noxmqtt_rc_t pump_rc = noxmqtt_outbox_pump(c);
        if (pump_rc != NOXMQTT_SUCCESS) {
            return pump_rc;
        }
    }

    if (c->keepalive == 0U) {
        return NOXMQTT_SUCCESS;
    }

    keepalive_ms = (uint32_t)c->keepalive * 1000U;
    last_activity = noxmqtt_last_activity_ms(c);

    if (c->status.ping_outstanding) {
        if ((now - c->last_pingreq_ms) >= keepalive_ms) {
            (void)noxmqtt_transport_disconnect(c);
            noxmqtt_transport_notify_disconnected(c, NOXMQTT_RC_ERROR_TIMEOUT);
            return NOXMQTT_RC_ERROR_TIMEOUT;
        }
        return NOXMQTT_SUCCESS;
    }

    if ((now - last_activity) >= keepalive_ms) {
        noxmqtt_rc_t rc = noxmqtt_pingreq(c);
        if (rc == NOXMQTT_SUCCESS) {
            c->status.ping_outstanding = 1;
            c->last_pingreq_ms = now;
        }
        return rc;
    }

    return NOXMQTT_SUCCESS;
}

/**
 * @brief Notifies the client that the transport disconnected.
 *
 * @param[in] c Client instance.
 * @param[in] reason Disconnect reason reported by the transport.
 */
void noxmqtt_transport_notify_disconnected(noxmqtt_client_t* c, noxmqtt_rc_t reason)
{
    noxmqtt_evt_data_t evt_data;

    if (noxmqtt_validate_client(c) != NOXMQTT_SUCCESS) {
        return;
    }

    MEMZERO_S(evt_data);

    c->status.connected = 0;
    c->status.ping_outstanding = 0;
    c->status.auth_in_progress = 0;
    noxmqtt_reset_broker_capabilities(c);
    noxmqtt_topic_alias_cache_clear(c);
    noxmqtt_publish_topic_alias_cache_clear(c);
    noxmqtt_clear_active_auth_method(c);

    if (!c->last_conf.server.disable_auto_reconnect && c->last_conf.server.addr != NULL) {
        c->status.reconnect_pending = 1;
        c->next_reconnect_ms = noxmqtt_tal_time_ms() +
            (c->last_conf.server.reconnect_timeout_ms ? c->last_conf.server.reconnect_timeout_ms
                                                      : NOXMQTT_DEFAULT_RECONNECT_TIMEOUT_MS);
    } else {
        c->status.reconnect_pending = 0;
    }

    evt_data.evt_id = NOXMQTT_EVT_DISCONNECT;
    noxmqtt_send_event(c, &evt_data);

    if (reason != NOXMQTT_SUCCESS) {
        noxmqtt_send_error(c, reason);
    }
}

/**
 * @brief Sends encoded MQTT bytes through the active transport.
 *
 * @param[in] c Client instance.
 * @param[in] data Buffer containing bytes to send.
 * @param[in] len Number of bytes to send.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_transport_write_packet(noxmqtt_client_t* c, const uint8_t* data, uint16_t len)
{
    if (c == NULL || data == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0 &&
        c->status.connected &&
        c->broker_maximum_packet_size > 0U &&
        (uint32_t)len > c->broker_maximum_packet_size) {
        return NOXMQTT_RC_ERROR_OVERFLOW;
    }

    if (noxmqtt_transport_send(c, data, len) != 0) {
        return NOXMQTT_RC_ERROR_TRANSPORT;
    }

    c->last_tx_ms = noxmqtt_tal_time_ms();
    return NOXMQTT_SUCCESS;
}

static noxmqtt_rc_t noxmqtt_write_bytes(uint8_t* buffer,
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

    if (data == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if ((uint32_t)*offset + data_len > buffer_len) {
        return NOXMQTT_RC_ERROR_OVERFLOW;
    }

    memcpy(&buffer[*offset], data, data_len);
    *offset = (uint16_t)(*offset + data_len);

    return NOXMQTT_SUCCESS;
}

static noxmqtt_rc_t noxmqtt_write_utf8_string(uint8_t* buffer,
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
    rc = noxmqtt_write_u16(buffer, buffer_len, offset, str_len);
    if (rc != NOXMQTT_SUCCESS) {
        return rc;
    }

    return noxmqtt_write_bytes(buffer, buffer_len, offset, (const uint8_t*)str, str_len);
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
static noxmqtt_rc_t noxmqtt_write_u16(uint8_t* buffer, uint16_t buffer_len, uint16_t* offset, uint16_t value)
{
    uint8_t bytes[2];

    bytes[0] = MSB(value);
    bytes[1] = LSB(value);
    return noxmqtt_write_bytes(buffer, buffer_len, offset, bytes, sizeof(bytes));
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
static noxmqtt_rc_t noxmqtt_write_varint(uint8_t* buffer, uint16_t buffer_len, uint16_t* offset, uint32_t value)
{
    uint8_t encoded[4];
    uint8_t count = 0;

    if (buffer == NULL || offset == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    do {
        uint8_t digit = (uint8_t)(value % 128U);
        value /= 128U;
        if (value > 0U) {
            digit |= 0x80U;
        }
        encoded[count++] = digit;
    } while (value > 0U && count < 4U);

    return noxmqtt_write_bytes(buffer, buffer_len, offset, encoded, count);
}

/**
 * @brief Computes the packet start offset for an encoded remaining length.
 *
 * @param[in] remain_len_bytes Number of encoded remaining-length bytes.
 *
 * @return Packet start offset within the transmit buffer.
 */
static uint16_t noxmqtt_packet_start_offset(uint8_t remain_len_bytes)
{
    return (uint16_t)(NOXMQTT_FIXED_HEADER_MAX_LEN - (sizeof(noxmqtt_hdr_t) + remain_len_bytes));
}

/**
 * @brief Finalizes a packet in the transmit buffer.
 *
 * @param[in] c Client instance.
 * @param[in] hdr Fixed header to write.
 * @param[in] body_len Encoded body length in bytes.
 * @param[out] out_start Pointer to the packet start in the transmit buffer.
 * @param[out] out_len Final packet length in bytes.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_finalize_packet(noxmqtt_client_t* c,
                                            noxmqtt_hdr_t hdr,
                                            uint16_t body_len,
                                            uint8_t** out_start,
                                            uint16_t* out_len)
{
    uint8_t remain_len_bytes = 0;
    uint16_t start_offset = 0;

    if (noxmqtt_validate_client(c) != NOXMQTT_SUCCESS || out_start == NULL || out_len == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    remain_len_bytes = (uint8_t)noxmqtt_set_remain_len(&c->tx_buf[sizeof(hdr)], body_len);
    if (remain_len_bytes == 0U) {
        return NOXMQTT_RC_ERROR_BAD_PACKET;
    }

    start_offset = noxmqtt_packet_start_offset(remain_len_bytes);
    memcpy(&c->tx_buf[start_offset], &hdr, sizeof(hdr));
    *out_start = &c->tx_buf[start_offset];
    *out_len = (uint16_t)(sizeof(hdr) + remain_len_bytes + body_len);

    return NOXMQTT_SUCCESS;
}

/**
 * @brief Validates that a client instance is initialized.
 *
 * @param[in] c Client instance.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_validate_client(const noxmqtt_client_t* c)
{
    if (c == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (c->flag_initialized != NOXMQTT_INIT_FLAG || c->tx_buf == NULL || c->rcv_buf == NULL) {
        return NOXMQTT_RC_ERROR_NOT_INIT;
    }

    return NOXMQTT_SUCCESS;
}

/**
 * @brief Validates the fixed-header flags for a received control packet.
 *
 * @param[in] hdr Received fixed header.
 *
 * @return Non-zero when the flags are valid, otherwise zero.
 */
static uint8_t noxmqtt_validate_received_header_flags(const noxmqtt_hdr_t* hdr)
{
    if (hdr == NULL) {
        return 0U;
    }

    switch (hdr->type) {
        case NOXMQTT_CTRL_PKT_TYPE_PUBLISH:
            return (hdr->qos != 3U) ? 1U : 0U;
        case NOXMQTT_CTRL_PKT_TYPE_PUBREL:
        case NOXMQTT_CTRL_PKT_TYPE_SUBSCRIBE:
        case NOXMQTT_CTRL_PKT_TYPE_UNSUBSCRIBE:
            return (hdr->dup == 0U && hdr->qos == 1U && hdr->retain == 0U) ? 1U : 0U;
        case NOXMQTT_CTRL_PKT_TYPE_CONNACK:
        case NOXMQTT_CTRL_PKT_TYPE_PUBACK:
        case NOXMQTT_CTRL_PKT_TYPE_PUBREC:
        case NOXMQTT_CTRL_PKT_TYPE_PUBCOMP:
        case NOXMQTT_CTRL_PKT_TYPE_SUBACK:
        case NOXMQTT_CTRL_PKT_TYPE_UNSUBACK:
        case NOXMQTT_CTRL_PKT_TYPE_PINGRESP:
        case NOXMQTT_CTRL_PKT_TYPE_DISCONNECT:
        case NOXMQTT_CTRL_PKT_TYPE_AUTH:
            return (hdr->dup == 0U && hdr->qos == 0U && hdr->retain == 0U) ? 1U : 0U;
        default:
            return 0U;
    }
}

/**
 * @brief Gets the most recent client I/O activity timestamp.
 *
 * @param[in] c Client instance.
 *
 * @return Timestamp in milliseconds.
 */
static uint32_t noxmqtt_last_activity_ms(noxmqtt_client_t* c)
{
    if (c->last_tx_ms > c->last_rx_ms) {
        return c->last_tx_ms;
    }

    return c->last_rx_ms;
}

/**
 * @brief Clears the cached subscription list.
 *
 * @param[in] c Client instance.
 */
static void noxmqtt_subscription_cache_clear(noxmqtt_client_t* c)
{
    uint16_t i = 0;

    if (c == NULL || c->subscriptions == NULL) {
        return;
    }

    for (i = 0; i < NOXMQTT_MAX_SUBSCRIPTIONS; i++) {
        if (c->subscriptions[i].topic != NULL) {
            free(c->subscriptions[i].topic);
            c->subscriptions[i].topic = NULL;
        }
        c->subscriptions[i].active = 0;
    }

    c->subscriptions_count = 0;
}

/**
 * @brief Clears the QoS message outbox.
 *
 * @param[in] c Client instance.
 */
static void noxmqtt_outbox_clear(noxmqtt_client_t* c)
{
    uint16_t i = 0;

    if (c == NULL || c->outbox == NULL) {
        return;
    }

    for (i = 0; i < NOXMQTT_MAX_OUTBOX_MESSAGES; i++) {
        noxmqtt_outbox_item_reset(&c->outbox[i]);
    }

    c->outbox_count = 0;
}

/**
 * @brief Adds or updates a cached subscription entry.
 *
 * @param[in] c Client instance.
 * @param[in] topic Subscription description to cache.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_subscription_cache_add(noxmqtt_client_t* c, const noxmqtt_topic_sub_t* topic)
{
    uint16_t i = 0;
    char* topic_copy = NULL;

    if (c == NULL || c->subscriptions == NULL || topic == NULL || topic->topic == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    for (i = 0; i < NOXMQTT_MAX_SUBSCRIPTIONS; i++) {
        if (c->subscriptions[i].active && strcmp(c->subscriptions[i].topic, topic->topic) == 0) {
            c->subscriptions[i].qos = topic->qos;
            c->subscriptions[i].mqtt5 = topic->mqtt5;
            return NOXMQTT_SUCCESS;
        }
    }

    topic_copy = noxmqtt_strdup_local(topic->topic);
    if (topic_copy == NULL) {
        return NOXMQTT_RC_ERROR_NO_MEMORY;
    }

    for (i = 0; i < NOXMQTT_MAX_SUBSCRIPTIONS; i++) {
        if (!c->subscriptions[i].active) {
            c->subscriptions[i].topic = topic_copy;
            c->subscriptions[i].qos = topic->qos;
            c->subscriptions[i].mqtt5 = topic->mqtt5;
            c->subscriptions[i].active = 1;
            c->subscriptions_count++;
            return NOXMQTT_SUCCESS;
        }
    }

    free(topic_copy);
    return NOXMQTT_RC_ERROR_OVERFLOW;
}

/**
 * @brief Removes a cached subscription entry by topic.
 *
 * @param[in] c Client instance.
 * @param[in] topic Topic name to remove.
 */
static void noxmqtt_subscription_cache_remove(noxmqtt_client_t* c, const char* topic)
{
    uint16_t i = 0;

    if (c == NULL || c->subscriptions == NULL || topic == NULL) {
        return;
    }

    for (i = 0; i < NOXMQTT_MAX_SUBSCRIPTIONS; i++) {
        if (c->subscriptions[i].active && c->subscriptions[i].topic != NULL &&
            strcmp(c->subscriptions[i].topic, topic) == 0) {
            free(c->subscriptions[i].topic);
            c->subscriptions[i].topic = NULL;
            c->subscriptions[i].active = 0;
            if (c->subscriptions_count > 0U) {
                c->subscriptions_count--;
            }
            break;
        }
    }
}

/**
 * @brief Stores a QoS publish in the outbox.
 *
 * @param[in] c Client instance.
 * @param[in] packet_identifier Packet identifier for the publish.
 * @param[in] topic Topic name.
 * @param[in] payload Payload buffer.
 * @param[in] payload_len Payload length in bytes.
 * @param[in] qos Requested QoS level.
 * @param[in] retain Retain flag.
 * @param[in] props Optional MQTT 5 publish properties.
 * @param[in] initial_state Initial outbox state to assign.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_outbox_store(noxmqtt_client_t* c,
                                         uint16_t packet_identifier,
                                         const char* topic,
                                         const uint8_t* payload,
                                         uint16_t payload_len,
                                         noxmqtt_qos_t qos,
                                         uint8_t retain,
                                         const noxmqtt_mqtt5_publish_props_t* props,
                                         noxmqtt_outbox_state_t initial_state)
{
    uint16_t i = 0;
    char* topic_copy = NULL;
    uint8_t* payload_copy = NULL;
    noxmqtt_rc_t rc;

    if (c == NULL || c->outbox == NULL || topic == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    if (noxmqtt_outbox_find(c, packet_identifier) != NULL) {
        return NOXMQTT_SUCCESS;
    }

    topic_copy = noxmqtt_strdup_local(topic);
    if (topic_copy == NULL) {
        return NOXMQTT_RC_ERROR_NO_MEMORY;
    }

    payload_copy = noxmqtt_memdup_local(payload, payload_len);
    if (payload_len > 0U && payload_copy == NULL) {
        free(topic_copy);
        return NOXMQTT_RC_ERROR_NO_MEMORY;
    }

    for (i = 0; i < NOXMQTT_MAX_OUTBOX_MESSAGES; i++) {
        if (c->outbox[i].state == NOXMQTT_OUTBOX_STATE_FREE) {
            c->outbox[i].packet_identifier = packet_identifier;
            c->outbox[i].topic = topic_copy;
            c->outbox[i].payload = payload_copy;
            c->outbox[i].payload_len = payload_len;
            c->outbox[i].qos = qos;
            c->outbox[i].retain = retain;
            c->outbox[i].dup = 0;
            c->outbox[i].state = initial_state;
            rc = noxmqtt_outbox_clone_publish_props(&c->outbox[i], props);
            if (rc != NOXMQTT_SUCCESS) {
                noxmqtt_outbox_item_reset(&c->outbox[i]);
                return rc;
            }
            c->outbox_count++;
            return NOXMQTT_SUCCESS;
        }
    }

    free(topic_copy);
    free(payload_copy);
    return NOXMQTT_RC_ERROR_OVERFLOW;
}

/**
 * @brief Finds an outbox entry by packet identifier.
 *
 * @param[in] c Client instance.
 * @param[in] packet_identifier Packet identifier to search for.
 *
 * @return Matching outbox item, or `NULL` if none is found.
 */
static noxmqtt_outbox_item_t* noxmqtt_outbox_find(noxmqtt_client_t* c, uint16_t packet_identifier)
{
    uint16_t i = 0;

    if (c == NULL || c->outbox == NULL) {
        return NULL;
    }

    for (i = 0; i < NOXMQTT_MAX_OUTBOX_MESSAGES; i++) {
        if (c->outbox[i].state != NOXMQTT_OUTBOX_STATE_FREE &&
            c->outbox[i].packet_identifier == packet_identifier) {
            return &c->outbox[i];
        }
    }

    return NULL;
}

/**
 * @brief Removes an outbox entry by packet identifier.
 *
 * @param[in] c Client instance.
 * @param[in] packet_identifier Packet identifier to remove.
 */
static void noxmqtt_outbox_remove(noxmqtt_client_t* c, uint16_t packet_identifier)
{
    noxmqtt_outbox_item_t* item = noxmqtt_outbox_find(c, packet_identifier);

    if (item == NULL) {
        return;
    }

    noxmqtt_outbox_item_reset(item);
    if (c->outbox_count > 0U) {
        c->outbox_count--;
    }
}

/**
 * @brief Frees memory owned by an outbox item and resets it.
 *
 * @param[in] item Outbox item to reset.
 */
static void noxmqtt_outbox_item_reset(noxmqtt_outbox_item_t* item)
{
    uint16_t i = 0;

    if (item == NULL) {
        return;
    }

    free(item->topic);
    free(item->payload);
    free(item->mqtt5_response_topic);
    free(item->mqtt5_correlation_data);
    free(item->mqtt5_content_type);
    for (i = 0; i < NOXMQTT_MAX_USER_PROPERTIES; i++) {
        free(item->mqtt5_user_property_names[i]);
        free(item->mqtt5_user_property_values[i]);
    }

    memset(item, 0, sizeof(*item));
    item->state = NOXMQTT_OUTBOX_STATE_FREE;
}

/**
 * @brief Clones MQTT 5 publish properties into an outbox item.
 *
 * @param[in] item Outbox item to populate.
 * @param[in] props Publish properties to clone.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_outbox_clone_publish_props(noxmqtt_outbox_item_t* item,
                                                       const noxmqtt_mqtt5_publish_props_t* props)
{
    if (item == NULL) {
        return NOXMQTT_RC_ERROR_NULL;
    }
    if (props == NULL) {
        return NOXMQTT_SUCCESS;
    }

    item->mqtt5 = *props;
    if (props->response_topic != NULL) {
        item->mqtt5_response_topic = noxmqtt_strdup_local(props->response_topic);
        if (item->mqtt5_response_topic == NULL) {
            return NOXMQTT_RC_ERROR_NO_MEMORY;
        }
        item->mqtt5.response_topic = item->mqtt5_response_topic;
    }
    if (props->correlation_data != NULL && props->correlation_data_len > 0U) {
        item->mqtt5_correlation_data = noxmqtt_memdup_local(props->correlation_data, props->correlation_data_len);
        if (item->mqtt5_correlation_data == NULL) {
            return NOXMQTT_RC_ERROR_NO_MEMORY;
        }
        item->mqtt5.correlation_data = item->mqtt5_correlation_data;
    }
    if (props->content_type != NULL) {
        item->mqtt5_content_type = noxmqtt_strdup_local(props->content_type);
        if (item->mqtt5_content_type == NULL) {
            return NOXMQTT_RC_ERROR_NO_MEMORY;
        }
        item->mqtt5.content_type = item->mqtt5_content_type;
    }

    if (props->user_properties != NULL && props->user_property_count > 0U) {
        if (props->user_property_count > NOXMQTT_MAX_USER_PROPERTIES) {
            return NOXMQTT_RC_ERROR_OVERFLOW;
        }

        item->mqtt5.user_properties = item->mqtt5_user_properties;
        item->mqtt5.user_property_count = props->user_property_count;
        for (uint16_t i = 0; i < props->user_property_count; i++) {
            item->mqtt5_user_property_names[i] = (props->user_properties[i].name != NULL)
                ? noxmqtt_strdup_local(props->user_properties[i].name) : NULL;
            item->mqtt5_user_property_values[i] = (props->user_properties[i].value != NULL)
                ? noxmqtt_strdup_local(props->user_properties[i].value) : NULL;
            if ((props->user_properties[i].name != NULL && item->mqtt5_user_property_names[i] == NULL) ||
                (props->user_properties[i].value != NULL && item->mqtt5_user_property_values[i] == NULL)) {
                return NOXMQTT_RC_ERROR_NO_MEMORY;
            }

            item->mqtt5_user_properties[i].name = item->mqtt5_user_property_names[i];
            item->mqtt5_user_properties[i].name_len = props->user_properties[i].name_len;
            item->mqtt5_user_properties[i].value = item->mqtt5_user_property_values[i];
            item->mqtt5_user_properties[i].value_len = props->user_properties[i].value_len;
        }
    }

    return NOXMQTT_SUCCESS;
}

/**
 * @brief Counts queued messages that are currently in flight.
 *
 * @param[in] c Client instance.
 *
 * @return Number of in-flight outbox entries.
 */
static uint16_t noxmqtt_outbox_inflight_count(const noxmqtt_client_t* c)
{
    uint16_t i = 0;
    uint16_t count = 0;

    if (c == NULL || c->outbox == NULL) {
        return 0U;
    }

    for (i = 0; i < NOXMQTT_MAX_OUTBOX_MESSAGES; i++) {
        if (c->outbox[i].state == NOXMQTT_OUTBOX_STATE_PUBLISH_SENT ||
            c->outbox[i].state == NOXMQTT_OUTBOX_STATE_PUBREL_SENT) {
            count++;
        }
    }

    return count;
}

/**
 * @brief Sends queued outbox messages when capacity is available.
 *
 * @param[in] c Client instance.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_outbox_pump(noxmqtt_client_t* c)
{
    uint16_t i = 0;
    uint16_t inflight_count = 0;
    uint16_t inflight_limit = 0;

    if (c == NULL || c->outbox == NULL || !c->status.connected) {
        return NOXMQTT_SUCCESS;
    }

    inflight_count = noxmqtt_outbox_inflight_count(c);
    inflight_limit = c->broker_receive_maximum;
    if (inflight_limit == 0U) {
        inflight_limit = NOXMQTT_MAX_OUTBOX_MESSAGES;
    }

    for (i = 0; i < NOXMQTT_MAX_OUTBOX_MESSAGES; i++) {
        noxmqtt_outbox_item_t* item = &c->outbox[i];
        const noxmqtt_mqtt5_publish_props_t* props = NULL;
        noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

        if (item->state != NOXMQTT_OUTBOX_STATE_QUEUED) {
            continue;
        }
        if (inflight_count >= inflight_limit) {
            break;
        }

        if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
            props = &item->mqtt5;
        }

        rc = noxmqtt_publish_internal(c,
                                      item->qos,
                                      item->retain,
                                      item->dup,
                                      item->topic,
                                      item->payload,
                                      item->payload_len,
                                      item->packet_identifier,
                                      props);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }

        item->state = NOXMQTT_OUTBOX_STATE_PUBLISH_SENT;
        inflight_count++;
    }

    return NOXMQTT_SUCCESS;
}

/**
 * @brief Re-subscribes all cached subscriptions after reconnect.
 *
 * @param[in] c Client instance.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_resubscribe_all(noxmqtt_client_t* c)
{
    uint16_t i = 0;

    if (c == NULL || c->subscriptions == NULL || c->subscriptions_count == 0U) {
        return NOXMQTT_SUCCESS;
    }

    for (i = 0; i < NOXMQTT_MAX_SUBSCRIPTIONS; i++) {
        noxmqtt_topic_sub_t topic;
        noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

        if (!c->subscriptions[i].active) {
            continue;
        }

        topic.topic = c->subscriptions[i].topic;
        topic.qos = c->subscriptions[i].qos;
        topic.mqtt5 = c->subscriptions[i].mqtt5;
        rc = noxmqtt_subscribe(c, &topic, 1U);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
    }

    return NOXMQTT_SUCCESS;
}

/**
 * @brief Replays queued QoS messages after reconnect.
 *
 * @param[in] c Client instance.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_replay_outbox(noxmqtt_client_t* c)
{
    uint16_t i = 0;

    if (c == NULL || c->outbox == NULL || c->outbox_count == 0U) {
        return NOXMQTT_SUCCESS;
    }

    for (i = 0; i < NOXMQTT_MAX_OUTBOX_MESSAGES; i++) {
        noxmqtt_outbox_item_t* item = &c->outbox[i];
        const noxmqtt_mqtt5_publish_props_t* props = NULL;
        noxmqtt_rc_t rc = NOXMQTT_SUCCESS;

        if (item->state == NOXMQTT_OUTBOX_STATE_FREE) {
            continue;
        }

        if (c->last_conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
            props = &item->mqtt5;
        }

        if (item->state == NOXMQTT_OUTBOX_STATE_PUBREL_SENT) {
            rc = noxmqtt_pubrel(c, item->packet_identifier);
            if (rc != NOXMQTT_SUCCESS) {
                return rc;
            }
            continue;
        }

        if (item->state == NOXMQTT_OUTBOX_STATE_PUBLISH_SENT) {
            item->dup = 1;
        }
        rc = noxmqtt_publish_internal(c,
                                      item->qos,
                                      item->retain,
                                      item->dup,
                                      item->topic,
                                      item->payload,
                                      item->payload_len,
                                      item->packet_identifier,
                                      props);
        if (rc != NOXMQTT_SUCCESS) {
            return rc;
        }
        item->state = NOXMQTT_OUTBOX_STATE_PUBLISH_SENT;
    }

    return NOXMQTT_SUCCESS;
}

/**
 * @brief Duplicates a null-terminated string.
 *
 * @param[in] src String to duplicate.
 *
 * @return Newly allocated string copy, or `NULL` on failure.
 */
static char* noxmqtt_strdup_local(const char* src)
{
    size_t len = 0;
    char* copy = NULL;

    if (src == NULL) {
        return NULL;
    }

    len = strlen(src) + 1U;
    copy = (char*)malloc(len);
    if (copy != NULL) {
        memcpy(copy, src, len);
    }

    return copy;
}

/**
 * @brief Duplicates a counted string and appends a null terminator.
 *
 * @param[in] src String bytes to duplicate.
 * @param[in] len Number of bytes to copy.
 *
 * @return Newly allocated string copy, or `NULL` on failure.
 */
static char* noxmqtt_strndup_local(const char* src, uint16_t len)
{
    char* copy = NULL;

    if (src == NULL) {
        return NULL;
    }

    copy = (char*)malloc((size_t)len + 1U);
    if (copy != NULL) {
        memcpy(copy, src, len);
        copy[len] = '\0';
    }

    return copy;
}

/**
 * @brief Duplicates a binary buffer.
 *
 * @param[in] src Buffer to duplicate.
 * @param[in] len Number of bytes to copy.
 *
 * @return Newly allocated buffer copy, or `NULL` on failure.
 */
static uint8_t* noxmqtt_memdup_local(const uint8_t* src, uint16_t len)
{
    uint8_t* copy = NULL;

    if (len == 0U) {
        return NULL;
    }

    if (src == NULL) {
        return NULL;
    }

    copy = (uint8_t*)malloc(len);
    if (copy != NULL) {
        memcpy(copy, src, len);
    }

    return copy;
}

/**
 * @brief Releases the stored broker-assigned client identifier.
 *
 * @param[in] c Client instance.
 */
static void noxmqtt_clear_assigned_client_identifier(noxmqtt_client_t* c)
{
    if (c == NULL) {
        return;
    }

    if (c->assigned_client_identifier != NULL) {
        if (c->last_conf.client_identifier == c->assigned_client_identifier) {
            c->last_conf.client_identifier = NULL;
        }
        free(c->assigned_client_identifier);
        c->assigned_client_identifier = NULL;
    }
}

/**
 * @brief Stores a broker-assigned client identifier for reconnect use.
 *
 * @param[in] c Client instance.
 * @param[in] identifier Assigned client identifier bytes.
 * @param[in] len Identifier length in bytes.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_set_assigned_client_identifier(noxmqtt_client_t* c, const char* identifier, uint16_t len)
{
    char* copy = NULL;

    if (c == NULL || identifier == NULL || len == 0U) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    copy = noxmqtt_strndup_local(identifier, len);
    if (copy == NULL) {
        return NOXMQTT_RC_ERROR_NO_MEMORY;
    }

    noxmqtt_clear_assigned_client_identifier(c);
    c->assigned_client_identifier = copy;
    c->last_conf.client_identifier = c->assigned_client_identifier;
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Clears the stored MQTT 5 authentication method.
 *
 * @param[in] c Client instance.
 */
static void noxmqtt_clear_active_auth_method(noxmqtt_client_t* c)
{
    if (c == NULL) {
        return;
    }

    if (c->active_auth_method != NULL) {
        free(c->active_auth_method);
        c->active_auth_method = NULL;
    }
}

/**
 * @brief Stores the active MQTT 5 authentication method.
 *
 * @param[in] c Client instance.
 * @param[in] method Authentication method string.
 * @param[in] len Authentication method length in bytes.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_set_active_auth_method(noxmqtt_client_t* c, const char* method, uint16_t len)
{
    char* copy = NULL;

    if (c == NULL || method == NULL || len == 0U) {
        return NOXMQTT_RC_ERROR_NULL;
    }

    copy = noxmqtt_strndup_local(method, len);
    if (copy == NULL) {
        return NOXMQTT_RC_ERROR_NO_MEMORY;
    }

    noxmqtt_clear_active_auth_method(c);
    c->active_auth_method = copy;
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Checks whether the stored auth method matches a parsed value.
 *
 * @param[in] c Client instance.
 * @param[in] method Authentication method string.
 * @param[in] len Authentication method length in bytes.
 *
 * @return Non-zero when the methods match, otherwise zero.
 */
static uint8_t noxmqtt_active_auth_method_matches(const noxmqtt_client_t* c, const char* method, uint16_t len)
{
    size_t active_len = 0;

    if (c == NULL || c->active_auth_method == NULL || method == NULL) {
        return 0U;
    }

    active_len = strlen(c->active_auth_method);
    if (active_len != len) {
        return 0U;
    }

    return (memcmp(c->active_auth_method, method, len) == 0) ? 1U : 0U;
}

/**
 * @brief Reports whether an MQTT 5 AUTH reason starts or continues auth exchange state.
 *
 * @param[in] reason_code MQTT 5 AUTH reason code.
 *
 * @return Non-zero when the AUTH exchange remains active, otherwise zero.
 */
static uint8_t noxmqtt_is_auth_exchange_active(uint8_t reason_code)
{
    return (reason_code == MQTT5_REASON_CONTINUE_AUTHENTICATION ||
            reason_code == MQTT5_REASON_REAUTHENTICATE) ? 1U : 0U;
}

/**
 * @brief Resets broker capability state to protocol defaults.
 *
 * @param[in] c Client instance.
 */
static void noxmqtt_reset_broker_capabilities(noxmqtt_client_t* c)
{
    if (c == NULL) {
        return;
    }

    c->broker_receive_maximum = 0;
    c->broker_topic_alias_maximum = 0;
    c->broker_maximum_qos = (uint8_t)NOXMQTT_QOS2_EXACTLY_ONCE_DELIV;
    c->broker_retain_available = 1U;
    c->broker_wildcard_subscriptions_available = 1U;
    c->broker_subscription_identifiers_available = 1U;
    c->broker_shared_subscriptions_available = 1U;
    c->broker_maximum_packet_size = 0;
    c->broker_session_expiry_interval = 0;
}

/**
 * @brief Checks whether a topic filter contains wildcard characters.
 *
 * @param[in] topic Topic filter string.
 *
 * @return Non-zero when a wildcard is present, otherwise zero.
 */
static uint8_t noxmqtt_topic_has_wildcard(const char* topic)
{
    if (topic == NULL) {
        return 0U;
    }

    return (strchr(topic, '+') != NULL || strchr(topic, '#') != NULL) ? 1U : 0U;
}

/**
 * @brief Checks whether a topic filter is a shared subscription.
 *
 * @param[in] topic Topic filter string.
 *
 * @return Non-zero when the topic uses the shared-subscription prefix.
 */
static uint8_t noxmqtt_topic_is_shared_subscription(const char* topic)
{
    if (topic == NULL) {
        return 0U;
    }

    return (strncmp(topic, "$share/", 7U) == 0) ? 1U : 0U;
}

/**
 * @brief Clears the inbound topic alias cache.
 *
 * @param[in] c Client instance.
 */
static void noxmqtt_topic_alias_cache_clear(noxmqtt_client_t* c)
{
    uint16_t i = 0;

    if (c == NULL || c->topic_aliases == NULL) {
        return;
    }

    for (i = 1U; i <= c->topic_alias_capacity; i++) {
        if (c->topic_aliases[i] != NULL) {
            free(c->topic_aliases[i]);
            c->topic_aliases[i] = NULL;
        }
    }
}

/**
 * @brief Stores an inbound MQTT 5 topic alias mapping.
 *
 * @param[in] c Client instance.
 * @param[in] alias Topic alias value.
 * @param[in] topic Topic string bytes.
 * @param[in] topic_len Topic string length in bytes.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_topic_alias_store(noxmqtt_client_t* c, uint16_t alias, const char* topic, uint16_t topic_len)
{
    char* topic_copy = NULL;

    if (c == NULL || c->topic_aliases == NULL || topic == NULL || topic_len == 0U) {
        return NOXMQTT_RC_ERROR_NULL;
    }
    if (alias == 0U || alias > c->topic_alias_capacity) {
        return NOXMQTT_RC_ERROR_OVERFLOW;
    }

    topic_copy = (char*)malloc((size_t)topic_len + 1U);
    if (topic_copy == NULL) {
        return NOXMQTT_RC_ERROR_NO_MEMORY;
    }
    memcpy(topic_copy, topic, topic_len);
    topic_copy[topic_len] = '\0';

    if (c->topic_aliases[alias] != NULL) {
        free(c->topic_aliases[alias]);
    }
    c->topic_aliases[alias] = topic_copy;
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Looks up a topic string by inbound alias.
 *
 * @param[in] c Client instance.
 * @param[in] alias Topic alias value.
 * @param[out] topic_len Length of the resolved topic string.
 *
 * @return Topic string for the alias, or `NULL` when not found.
 */
static const char* noxmqtt_topic_alias_lookup(noxmqtt_client_t* c, uint16_t alias, uint16_t* topic_len)
{
    size_t len = 0;

    if (topic_len != NULL) {
        *topic_len = 0;
    }
    if (c == NULL || c->topic_aliases == NULL || alias == 0U || alias > c->topic_alias_capacity) {
        return NULL;
    }
    if (c->topic_aliases[alias] == NULL) {
        return NULL;
    }

    len = strlen(c->topic_aliases[alias]);
    if (topic_len != NULL) {
        *topic_len = (uint16_t)len;
    }

    return c->topic_aliases[alias];
}

/**
 * @brief Clears the outbound topic alias cache.
 *
 * @param[in] c Client instance.
 */
static void noxmqtt_publish_topic_alias_cache_clear(noxmqtt_client_t* c)
{
    uint16_t i = 0;

    if (c == NULL || c->publish_topic_aliases == NULL) {
        return;
    }

    for (i = 1U; i <= c->topic_alias_capacity; i++) {
        if (c->publish_topic_aliases[i] != NULL) {
            free(c->publish_topic_aliases[i]);
            c->publish_topic_aliases[i] = NULL;
        }
    }

    c->next_publish_topic_alias = 1U;
}

/**
 * @brief Finds an outbound topic alias for a topic.
 *
 * @param[in] c Client instance.
 * @param[in] topic Topic string to search for.
 *
 * @return Alias value, or zero when none is assigned.
 */
static uint16_t noxmqtt_publish_topic_alias_find(const noxmqtt_client_t* c, const char* topic)
{
    uint16_t i = 0;
    uint16_t alias_limit = 0;

    if (c == NULL || c->publish_topic_aliases == NULL || topic == NULL || topic[0] == '\0') {
        return 0U;
    }

    alias_limit = c->broker_topic_alias_maximum;
    if (alias_limit == 0U || alias_limit > c->topic_alias_capacity) {
        alias_limit = c->topic_alias_capacity;
    }

    for (i = 1U; i <= alias_limit; i++) {
        if (c->publish_topic_aliases[i] != NULL && strcmp(c->publish_topic_aliases[i], topic) == 0) {
            return i;
        }
    }

    return 0U;
}

/**
 * @brief Looks up the topic currently assigned to an outbound alias.
 *
 * @param[in] c Client instance.
 * @param[in] alias Topic alias value.
 *
 * @return Topic string for the alias, or `NULL` when none is assigned.
 */
static const char* noxmqtt_publish_topic_alias_lookup(noxmqtt_client_t* c, uint16_t alias)
{
    uint16_t alias_limit = 0;

    if (c == NULL || c->publish_topic_aliases == NULL) {
        return NULL;
    }

    alias_limit = c->broker_topic_alias_maximum;
    if (alias_limit == 0U || alias_limit > c->topic_alias_capacity) {
        alias_limit = c->topic_alias_capacity;
    }
    if (alias == 0U || alias > alias_limit) {
        return NULL;
    }

    return c->publish_topic_aliases[alias];
}

/**
 * @brief Stores an outbound MQTT 5 topic alias mapping.
 *
 * @param[in] c Client instance.
 * @param[in] alias Topic alias value.
 * @param[in] topic Topic string to map.
 *
 * @return NoxMQTT status code.
 */
static noxmqtt_rc_t noxmqtt_publish_topic_alias_store(noxmqtt_client_t* c, uint16_t alias, const char* topic)
{
    uint16_t i = 0;
    char* topic_copy = NULL;
    uint16_t alias_limit = 0;

    if (c == NULL || c->publish_topic_aliases == NULL || topic == NULL || topic[0] == '\0') {
        return NOXMQTT_RC_ERROR_NULL;
    }

    alias_limit = c->broker_topic_alias_maximum;
    if (alias_limit == 0U || alias_limit > c->topic_alias_capacity) {
        alias_limit = c->topic_alias_capacity;
    }
    if (alias == 0U || alias > alias_limit) {
        return NOXMQTT_RC_ERROR_OVERFLOW;
    }

    topic_copy = noxmqtt_strdup_local(topic);
    if (topic_copy == NULL) {
        return NOXMQTT_RC_ERROR_NO_MEMORY;
    }

    for (i = 1U; i <= alias_limit; i++) {
        if (i != alias && c->publish_topic_aliases[i] != NULL && strcmp(c->publish_topic_aliases[i], topic) == 0) {
            free(c->publish_topic_aliases[i]);
            c->publish_topic_aliases[i] = NULL;
        }
    }

    if (c->publish_topic_aliases[alias] != NULL) {
        free(c->publish_topic_aliases[alias]);
    }
    c->publish_topic_aliases[alias] = topic_copy;
    return NOXMQTT_SUCCESS;
}

/**
 * @brief Assigns the next outbound topic alias to a topic.
 *
 * @param[in] c Client instance.
 * @param[in] topic Topic string to map.
 *
 * @return Assigned alias value, or zero on failure.
 */
static uint16_t noxmqtt_publish_topic_alias_assign(noxmqtt_client_t* c, const char* topic)
{
    uint16_t alias = 0;
    uint16_t assigned_alias = 0;
    uint16_t alias_limit = 0;

    if (c == NULL || topic == NULL || topic[0] == '\0') {
        return 0U;
    }

    alias_limit = c->broker_topic_alias_maximum;
    if (alias_limit == 0U || alias_limit > c->topic_alias_capacity) {
        alias_limit = c->topic_alias_capacity;
    }
    if (alias_limit == 0U) {
        return 0U;
    }

    alias = c->next_publish_topic_alias;
    if (alias == 0U || alias > alias_limit) {
        alias = 1U;
    }
    assigned_alias = alias;

    if (noxmqtt_publish_topic_alias_store(c, assigned_alias, topic) != NOXMQTT_SUCCESS) {
        return 0U;
    }

    alias++;
    if (alias > alias_limit) {
        alias = 1U;
    }
    c->next_publish_topic_alias = alias;

    return assigned_alias;
}

/**
 * @brief Encodes an MQTT remaining-length field.
 *
 * @param[out] buffer Output buffer that receives the encoded bytes.
 * @param[in] len Remaining length value to encode.
 *
 * @return Number of encoded bytes, or 0 on failure.
 */
int noxmqtt_set_remain_len(uint8_t* buffer, uint32_t len)
{
    uint8_t encoded[4];
    uint8_t count = 0;

    if (buffer == NULL || len > 268435455U) {
        return 0;
    }

    do {
        uint8_t digit = (uint8_t)(len % 128U);
        len /= 128U;
        if (len > 0U) {
            digit |= 0x80U;
        }
        encoded[count++] = digit;
    } while (len > 0U && count < 4U);

    memset(buffer, 0, 4U);
    memcpy(&buffer[4U - count], encoded, count);
    return count;
}

/**
 * @brief Decodes an MQTT remaining-length field.
 *
 * @param[in] buffer Buffer containing encoded bytes.
 * @param[out] len Decoded remaining length.
 *
 * @return Number of consumed bytes, or `-1` on failure.
 */
int noxmqtt_decode_remain_len(const uint8_t* buffer, uint32_t* len)
{
    uint8_t byte = 0;
    uint32_t multiplier = 1;
    uint32_t value = 0;
    int index = 0;

    if (buffer == NULL) {
        return -1;
    }

    do {
        byte = buffer[index++];
        value += (uint32_t)(byte & 127U) * multiplier;
        multiplier *= 128U;
        if (multiplier > 128U * 128U * 128U * 128U) {
            return -1;
        }
    } while ((byte & 128U) != 0U && index < 4);

    if ((byte & 128U) != 0U) {
        return -1;
    }

    if (len != NULL) {
        *len = value;
    }

    return index;
}

/**
 * @brief Clears the per-client transmit buffer.
 *
 * @param[in] c Client instance that owns the transmit buffer.
 */
static void noxmqtt_clear_tx_buf(noxmqtt_client_t* c)
{
    if (c != NULL && c->tx_buf != NULL && c->tx_buf_size > 0U) {
        memset(c->tx_buf, 0, c->tx_buf_size);
    }
}

/**
 * @brief Reports whether the client is currently connected.
 *
 * @param[in] c Client instance.
 *
 * @return Non-zero when connected, otherwise zero.
 */
uint8_t noxmqtt_is_connected(noxmqtt_client_t* c)
{
    if (c == NULL) {
        return 0;
    }

    return c->status.connected ? 1U : 0U;
}

#ifdef __cplusplus
}
#endif
