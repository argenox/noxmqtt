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
* File: noxmqtt.h
* Summary: NoxMQTT External APIs Definitions
*
*****************************************************************************/

#ifndef _NOXMQTT_H_
#define _NOXMQTT_H_

#include <stdint.h>


#ifdef __cplusplus
extern "C" {
#endif

#include "noxmqtt_err.h"
#include "noxmqttlib.h"
#include "noxmqtt_version.h"
#include "noxmqtt_config.h"

#define NOXMQTT_PACKET_IDENT_BYTE_LEN (2)
#define NOXMQTT_LENGTH_BYTE_LEN       (2)

typedef enum
{
    NOXMQTT_PROTOCOL_V3_1_1 = 4,
    NOXMQTT_PROTOCOL_V5_0   = 5,
} noxmqtt_protocol_ver_t;

typedef enum
{
    NOXMQTT_TRANSPORT_TCP = 0,
    NOXMQTT_TRANSPORT_TLS = 1,
    NOXMQTT_TRANSPORT_WS  = 2,
    NOXMQTT_TRANSPORT_WSS = 3,
} noxmqtt_transport_mode_t;

/** NoxMQTT QoS Levels */
typedef enum
{
    NOXMQTT_QOS0_AT_MOST_ONCE_DELIV  = 0,
    NOXMQTT_QOS1_AT_LEAST_ONCE_DELIV = 1,
    NOXMQTT_QOS2_EXACTLY_ONCE_DELIV  = 2,
} noxmqtt_qos_t;

/** NoxMQTT Events  */
typedef enum {
    NOXMQTT_EVT_CONNECT,        /* Connection */
    NOXMQTT_EVT_CONNECT_ERROR,  /* Connection Error */
    NOXMQTT_EVT_PUBLISHED,      /* Published */
    NOXMQTT_EVT_RECEIVED,       /* Received Publish */
    NOXMQTT_EVT_SUBSCRIBED,
    NOXMQTT_EVT_UNSUBSCRIBED,
    NOXMQTT_EVT_PINGRESP,
    NOXMQTT_EVT_PUBREL,
    NOXMQTT_EVT_DISCONNECT,
    NOXMQTT_EVT_AUTH,
    NOXMQTT_EVT_ERROR,

} noxmqtt_evt_id_t;

/** MQTT Subscription acknowledgement return codes */
typedef enum {

    NOXMQTT_SUBACK_RETURN_SUCCESS_QOS0 = 0x00, /* Success - Maximum QoS 0 */
    NOXMQTT_SUBACK_RETURN_SUCCESS_QOS1 = 0x01, /* Success - Maximum QoS 1 */
    NOXMQTT_SUBACK_RETURN_SUCCESS_QOS2 = 0x02, /* Success - Maximum QoS 2 */
    NOXMQTT_SUBACK_RETURN_FAILURE      = 0x80, /* Failure */

} noxmqtt_suback_return_t;


/** Connection Error Reason codes */
typedef enum {
    NOXMQTT_CONN_ERR_REFUSED_UNACCP_PROT_VER = 0x01, /* Connection Refused, unacceptable protocol version */
    NOXMQTT_CONN_ERR_REFUSED_IDENT_REJECTED = 0x02,  /* Connection Refused, identifier rejected */
    NOXMQTT_CONN_ERR_REFUSED_SERVER_UNAVAIL = 0x03,  /* Connection Refused, Server Unavailable */
    NOXMQTT_CONN_ERR_REFUSED_BAD_USER_PASS = 0x04,   /* Connection Refused, Bad Username or Password */
    NOXMQTT_CONN_ERR_REFUSED_NOT_AUTH = 0x05,        /* Connection Refused, Not authorized */

} noxmqtt_conn_err_reason_t;

typedef struct
{
    const char* name;
    uint16_t name_len;
    const char* value;
    uint16_t value_len;
} noxmqtt_mqtt5_user_property_t;

typedef struct
{
    const noxmqtt_mqtt5_user_property_t* user_properties;
    uint16_t user_property_count;
    const char* reason_string;
    uint16_t reason_string_len;
} noxmqtt_mqtt5_common_props_t;

typedef struct
{
    uint8_t session_present;
    uint8_t reason_code;
    uint8_t maximum_qos;
    uint8_t retain_available;
    uint8_t wildcard_subscriptions_available;
    uint8_t subscription_identifiers_available;
    uint8_t shared_subscriptions_available;
    uint32_t session_expiry_interval;
    uint32_t maximum_packet_size;
    uint16_t receive_maximum;
    uint16_t topic_alias_maximum;
    const char* assigned_client_identifier;
    uint16_t assigned_client_identifier_len;
    const char* response_information;
    uint16_t response_information_len;
    const char* server_reference;
    uint16_t server_reference_len;
    const char* auth_method;
    uint16_t auth_method_len;
    const uint8_t* auth_data;
    uint16_t auth_data_len;
    const noxmqtt_mqtt5_user_property_t* user_properties;
    uint16_t user_property_count;

} connect_evt_t;

typedef struct
{
    noxmqtt_conn_err_reason_t reason;
    uint8_t raw_reason_code;
    const char* reason_string;
    uint16_t reason_string_len;
    const char* server_reference;
    uint16_t server_reference_len;
    const noxmqtt_mqtt5_user_property_t* user_properties;
    uint16_t user_property_count;

} connect_error_evt_t;

typedef struct
{
    noxmqtt_rc_t rc;
    uint8_t mqtt5_reason_code;

} error_evt_t;

typedef struct
{
    uint8_t packet_identified_msb;
    uint8_t packet_identified_lsb;
    uint16_t packet_identifier;
    uint8_t reason_code;
    noxmqtt_mqtt5_common_props_t mqtt5;

} published_evt_t;

typedef struct
{
    char* topic;
    uint16_t topic_len;
    uint16_t packet_identifier;
    char* payload;
    uint16_t payload_len;
    uint8_t payload_format_indicator;
    uint32_t message_expiry_interval;
    uint16_t topic_alias;
    uint32_t subscription_identifier;
    const uint32_t* subscription_identifiers;
    uint16_t subscription_identifier_count;
    const char* response_topic;
    uint16_t response_topic_len;
    const uint8_t* correlation_data;
    uint16_t correlation_data_len;
    const char* content_type;
    uint16_t content_type_len;
    const noxmqtt_mqtt5_user_property_t* user_properties;
    uint16_t user_property_count;

} received_evt_t;

typedef struct
{
    noxmqtt_suback_return_t return_code;
    uint16_t packet_identifier;
    const uint8_t* reason_codes;
    uint16_t reason_code_count;
    noxmqtt_mqtt5_common_props_t mqtt5;

} subscribed_evt_t;

typedef struct
{
    uint8_t packet_identified_msb;
    uint8_t packet_identified_lsb;
    uint16_t packet_identifier;
    const uint8_t* reason_codes;
    uint16_t reason_code_count;
    noxmqtt_mqtt5_common_props_t mqtt5;

} unsubscribed_evt_t;

typedef struct
{
    uint8_t test;

} pingresp_evt_t;

typedef struct
{
    uint8_t test;

} pubrel_evt_t;

typedef struct
{
    uint8_t reason_code;
    uint32_t session_expiry_interval;
    const char* server_reference;
    uint16_t server_reference_len;
    noxmqtt_mqtt5_common_props_t mqtt5;

} disconnect_evt_t;

typedef struct
{
    uint8_t reason_code;
    const char* auth_method;
    uint16_t auth_method_len;
    const uint8_t* auth_data;
    uint16_t auth_data_len;
    noxmqtt_mqtt5_common_props_t mqtt5;
} auth_evt_t;

/** NoxMQTT Event Data */
typedef struct
{
    noxmqtt_evt_id_t evt_id; /* Indicates which event occured */

    /* Event information */
    union {
        connect_evt_t      connect_evt;
        connect_error_evt_t conn_err_evt;
        published_evt_t    published_evt;
        received_evt_t     received_evt;
        subscribed_evt_t   subscribed_evt;
        unsubscribed_evt_t unsubscribed_evt;
        pingresp_evt_t     pingresp_evt;
        pubrel_evt_t       pubrel_evt;
        disconnect_evt_t   disconnect_evt;
        auth_evt_t         auth_evt;
        error_evt_t        error_evt;
    }evt;

} noxmqtt_evt_data_t;


/* Callback */
typedef void (*noxmqtt_callback_t)(noxmqtt_evt_data_t * evt_data);

typedef struct
{
    uint32_t session_expiry_interval;
    uint32_t maximum_packet_size;
    uint16_t receive_maximum;
    uint16_t topic_alias_maximum;
    uint8_t request_response_info;
    uint8_t request_problem_info;
    const char* auth_method;
    const uint8_t* auth_data;
    uint16_t auth_data_len;
    const noxmqtt_mqtt5_user_property_t* user_properties;
    uint16_t user_property_count;
} noxmqtt_mqtt5_connect_props_t;

typedef struct
{
    uint32_t will_delay_interval;
    uint32_t message_expiry_interval;
    const char* response_topic;
    const uint8_t* correlation_data;
    uint16_t correlation_data_len;
    const char* content_type;
    uint8_t payload_format_indicator;
    const noxmqtt_mqtt5_user_property_t* user_properties;
    uint16_t user_property_count;
} noxmqtt_mqtt5_will_props_t;

typedef struct
{
    uint32_t message_expiry_interval;
    uint16_t topic_alias;
    const char* response_topic;
    const uint8_t* correlation_data;
    uint16_t correlation_data_len;
    const char* content_type;
    uint8_t payload_format_indicator;
    const noxmqtt_mqtt5_user_property_t* user_properties;
    uint16_t user_property_count;
} noxmqtt_mqtt5_publish_props_t;

typedef struct
{
    uint16_t subscribe_identifier;
    uint8_t no_local;
    uint8_t retain_as_published;
    uint8_t retain_handling;
    const noxmqtt_mqtt5_user_property_t* user_properties;
    uint16_t user_property_count;
} noxmqtt_mqtt5_subscribe_props_t;

typedef struct
{
    uint32_t session_expiry_interval;
    uint8_t reason_code;
    const char* reason_string;
    const char* server_reference;
    const noxmqtt_mqtt5_user_property_t* user_properties;
    uint16_t user_property_count;
} noxmqtt_mqtt5_disconnect_props_t;

typedef struct
{
    uint8_t reason_code;
    const char* auth_method;
    const uint8_t* auth_data;
    uint16_t auth_data_len;
    const char* reason_string;
    const noxmqtt_mqtt5_user_property_t* user_properties;
    uint16_t user_property_count;
} noxmqtt_mqtt5_auth_props_t;

typedef struct
{
    struct {
        noxmqtt_transport_mode_t mode;
        char* addr; /* URL or IP Address*/
        uint16_t port; /* Port to use */
        uint32_t reconnect_timeout_ms;
        uint32_t network_timeout_ms;
        uint8_t disable_auto_reconnect;
        struct {
            const char* ca_cert;
            const char* client_cert;
            const char* client_key;
            const char* server_name;
            const char* const* alpn_protos;
            uint8_t verify_peer;
            uint8_t verify_hostname;
        } tls;
    } server;

    struct {
        char* username;
        char* password;
    } auth;

    struct  {
        char*   topic;
        char*   msg;
        const uint8_t* payload;
        uint16_t payload_len;
        noxmqtt_qos_t qos;
        uint8_t retain;
        noxmqtt_mqtt5_will_props_t mqtt5;
    } will_topic;

    uint8_t clean_session;
    const char* client_identifier; /* Unique Client Identifier */
    noxmqtt_callback_t callback;
    noxmqtt_protocol_ver_t protocol_version;
    noxmqtt_mqtt5_connect_props_t mqtt5;

} noxmqtt_client_conf_t;

typedef struct
{
    char* topic;
    noxmqtt_qos_t qos;
    noxmqtt_mqtt5_subscribe_props_t mqtt5;

} noxmqtt_topic_sub_t;

typedef enum
{
    NOXMQTT_OUTBOX_STATE_FREE = 0,
    NOXMQTT_OUTBOX_STATE_QUEUED = 1,
    NOXMQTT_OUTBOX_STATE_PUBLISH_SENT = 2,
    NOXMQTT_OUTBOX_STATE_PUBREL_SENT = 3,
} noxmqtt_outbox_state_t;

typedef struct
{
    char* topic;
    noxmqtt_qos_t qos;
    noxmqtt_mqtt5_subscribe_props_t mqtt5;
    uint8_t active;
} noxmqtt_subscription_state_t;

typedef struct
{
    uint16_t packet_identifier;
    char* topic;
    uint8_t* payload;
    uint16_t payload_len;
    noxmqtt_qos_t qos;
    uint8_t retain;
    uint8_t dup;
    noxmqtt_outbox_state_t state;
    noxmqtt_mqtt5_publish_props_t mqtt5;
    noxmqtt_mqtt5_user_property_t mqtt5_user_properties[NOXMQTT_MAX_USER_PROPERTIES];
    char* mqtt5_response_topic;
    uint8_t* mqtt5_correlation_data;
    char* mqtt5_content_type;
    char* mqtt5_user_property_names[NOXMQTT_MAX_USER_PROPERTIES];
    char* mqtt5_user_property_values[NOXMQTT_MAX_USER_PROPERTIES];
} noxmqtt_outbox_item_t;

typedef struct
{
    uint32_t flag_initialized; /** Indicates the client object is successfully initialized */

    struct  {
        uint8_t connected : 1;
        uint8_t ping_outstanding : 1;
        uint8_t reconnect_pending : 1;
        uint8_t auth_in_progress : 1;
    } status;

    noxmqtt_callback_t callback;
    uint16_t packet_ident;
    uint16_t keepalive;

    noxmqtt_debug_lvl_t debug_lvl;

    uint8_t* tx_buf;
    uint16_t tx_buf_size;
    uint8_t* rcv_buf;
    uint16_t rcv_offset;
    uint16_t rcv_buf_size;
    uint32_t last_rx_ms;
    uint32_t last_tx_ms;
    uint32_t last_pingreq_ms;
    uint32_t next_reconnect_ms;
    void* transport_ctx;
    noxmqtt_subscription_state_t* subscriptions;
    uint16_t subscriptions_count;
    noxmqtt_outbox_item_t* outbox;
    uint16_t outbox_count;
    char** topic_aliases;
    char** publish_topic_aliases;
    uint16_t topic_alias_capacity;
    uint16_t next_publish_topic_alias;
    uint16_t broker_receive_maximum;
    uint16_t broker_topic_alias_maximum;
    uint8_t broker_maximum_qos;
    uint8_t broker_retain_available;
    uint8_t broker_wildcard_subscriptions_available;
    uint8_t broker_subscription_identifiers_available;
    uint8_t broker_shared_subscriptions_available;
    uint32_t broker_maximum_packet_size;
    uint32_t broker_session_expiry_interval;
    char* active_auth_method;
    char* assigned_client_identifier;
    noxmqtt_client_conf_t last_conf;

} noxmqtt_client_t;

/**
 * @brief Initializes an NoxMQTT client instance.
 *
 * @param[in] c Client instance to initialize.
 * @param[in] lvl Initial debug verbosity level.
 *
 * @return NoxMQTT status code.
 */
extern noxmqtt_rc_t noxmqtt_init(noxmqtt_client_t * c, noxmqtt_debug_lvl_t lvl);

/**
 * @brief Releases resources owned by an NoxMQTT client instance.
 *
 * @param[in] c Client instance to deinitialize.
 *
 * @return NoxMQTT status code.
 */
extern noxmqtt_rc_t noxmqtt_deinit(noxmqtt_client_t* c);

/**
 * @brief Connects a client to an MQTT broker.
 *
 * @param[in] c Client instance.
 * @param[in] conf Connection configuration.
 * @param[in] keepalive Keepalive interval in seconds.
 *
 * @return NoxMQTT status code.
 */
extern noxmqtt_rc_t noxmqtt_connect(noxmqtt_client_t* c, noxmqtt_client_conf_t* conf, uint16_t keepalive);

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
extern noxmqtt_rc_t noxmqtt_publish_data(noxmqtt_client_t* c,
                                         noxmqtt_qos_t qos,
                                         uint8_t retain,
                                         uint8_t dup,
                                         char* topic,
                                         const uint8_t* payload,
                                         uint16_t payload_len,
                                         const noxmqtt_mqtt5_publish_props_t* props);

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
extern noxmqtt_rc_t noxmqtt_publish(noxmqtt_client_t* c,
                                    noxmqtt_qos_t qos,
                                    uint8_t retain,
                                    uint8_t dup,
                                    char* topic,
                                    char* msg);

/**
 * @brief Subscribes to one or more topics.
 *
 * @param[in] c Client instance.
 * @param[in] topics Topic subscription array.
 * @param[in] topic_cnt Number of topic entries.
 *
 * @return NoxMQTT status code.
 */
extern noxmqtt_rc_t noxmqtt_subscribe(noxmqtt_client_t* c,
                                const noxmqtt_topic_sub_t* topics,
                                uint8_t topic_cnt);

/**
 * @brief Unsubscribes from one or more topics.
 *
 * @param[in] c Client instance.
 * @param[in] topics Topic subscription array.
 * @param[in] topic_cnt Number of topic entries.
 *
 * @return NoxMQTT status code.
 */
extern noxmqtt_rc_t noxmqtt_unsubscribe(noxmqtt_client_t* c,
    const noxmqtt_topic_sub_t* topics,
    uint8_t topic_cnt);

/**
 * @brief Disconnects from the broker.
 *
 * @param[in] c Client instance.
 *
 * @return NoxMQTT status code.
 */
extern noxmqtt_rc_t noxmqtt_disconnect(noxmqtt_client_t * c);

/**
 * @brief Disconnects from the broker with MQTT 5 properties.
 *
 * @param[in] c Client instance.
 * @param[in] props Optional MQTT 5 disconnect properties.
 *
 * @return NoxMQTT status code.
 */
extern noxmqtt_rc_t noxmqtt_disconnect_ex(noxmqtt_client_t* c,
                                          const noxmqtt_mqtt5_disconnect_props_t* props);

/**
 * @brief Sends an MQTT 5 AUTH packet.
 *
 * @param[in] c Client instance.
 * @param[in] props MQTT 5 AUTH properties to send.
 *
 * @return NoxMQTT status code.
 */
extern noxmqtt_rc_t noxmqtt_auth(noxmqtt_client_t* c,
                                 const noxmqtt_mqtt5_auth_props_t* props);

/**
 * @brief Runs periodic keepalive, reconnect, and queued-send processing.
 *
 * @param[in] c Client instance.
 *
 * @return NoxMQTT status code.
 */
extern noxmqtt_rc_t noxmqtt_process(noxmqtt_client_t* c);

/**
 * @brief Notifies the client that the transport disconnected.
 *
 * @param[in] c Client instance.
 * @param[in] reason Disconnect reason reported by the transport.
 */
extern void noxmqtt_transport_notify_disconnected(noxmqtt_client_t* c, noxmqtt_rc_t reason);

/**
 * @brief Reports whether the client is currently connected.
 *
 * @param[in] c Client instance.
 *
 * @return Non-zero when connected, otherwise zero.
 */
extern uint8_t noxmqtt_is_connected(noxmqtt_client_t* c);

#ifdef __cplusplus
}
#endif

#endif /* _NOXMQTT_H_ */
