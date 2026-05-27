/*
 * SPDX-License-Identifier: GPL-2.0-only OR LicenseRef-Argenox-Commercial
 *
 * Copyright (c) 2024 Argenox Technologies LLC
 *
 * This file is part of NoxMQTT.
 * You may use this file under GPL-2.0-only or under a commercial license
 * from Argenox Technologies LLC. See LICENSE and
 * LICENSE.
 *
 * File: test_noxmqtt_integration.c
 * Summary: Broker-backed NoxMQTT integration tests
 */

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "noxmqtt.h"
#include "noxmqtt_tal.h"

enum
{
    NOXMQTT_TEST_POLL_SLEEP_MS = 10,
    NOXMQTT_TEST_WAIT_TIMEOUT_MS = 5000,
    NOXMQTT_TEST_SHORT_WAIT_MS = 500,
    NOXMQTT_TEST_TOPIC_LEN = 256,
    NOXMQTT_TEST_PAYLOAD_LEN = 512,
    NOXMQTT_TEST_RESPONSE_TOPIC_LEN = 256,
    NOXMQTT_TEST_CONTENT_TYPE_LEN = 128,
    NOXMQTT_TEST_CORRELATION_DATA_LEN = 64,
    NOXMQTT_TEST_CLIENT_ID_LEN = 96,
    NOXMQTT_TEST_HOST_LEN = 128,
    NOXMQTT_TEST_REASON_CODES_LEN = 8,
};

typedef struct
{
    int socket_fd;
    noxmqtt_transport_rcv_t recv_cb;
} noxmqtt_test_transport_ctx_t;

typedef struct
{
    uint32_t connect_count;
    uint32_t connect_error_count;
    uint32_t published_count;
    uint32_t received_count;
    uint32_t subscribed_count;
    uint32_t unsubscribed_count;
    uint32_t disconnect_count;
    uint32_t auth_count;
    uint32_t error_count;
    uint8_t last_connect_reason_code;
    uint8_t last_session_present;
    uint16_t last_topic_alias_maximum;
    uint16_t last_receive_maximum;
    uint8_t last_published_reason_code;
    uint16_t last_packet_identifier;
    noxmqtt_suback_return_t last_suback_return_code;
    uint8_t last_suback_reason_codes[NOXMQTT_TEST_REASON_CODES_LEN];
    uint16_t last_suback_reason_code_count;
    noxmqtt_rc_t last_error_rc;
    char last_topic[NOXMQTT_TEST_TOPIC_LEN];
    uint16_t last_topic_len;
    uint8_t last_payload[NOXMQTT_TEST_PAYLOAD_LEN];
    uint16_t last_payload_len;
    uint16_t last_received_packet_identifier;
    uint16_t last_received_topic_alias;
    uint8_t last_received_payload_format_indicator;
    uint32_t last_received_message_expiry_interval;
    uint32_t last_subscription_identifier;
    uint32_t last_subscription_identifiers[NOXMQTT_MAX_SUBSCRIPTION_IDENTIFIERS];
    uint16_t last_subscription_identifier_count;
    char last_response_topic[NOXMQTT_TEST_RESPONSE_TOPIC_LEN];
    uint16_t last_response_topic_len;
    uint8_t last_correlation_data[NOXMQTT_TEST_CORRELATION_DATA_LEN];
    uint16_t last_correlation_data_len;
    char last_content_type[NOXMQTT_TEST_CONTENT_TYPE_LEN];
    uint16_t last_content_type_len;
} noxmqtt_test_observed_t;

typedef struct
{
    noxmqtt_client_t client;
    noxmqtt_client_conf_t conf;
    noxmqtt_test_observed_t observed;
    char role_name[24];
    char client_id[NOXMQTT_TEST_CLIENT_ID_LEN];
    char broker_host[NOXMQTT_TEST_HOST_LEN];
} noxmqtt_test_client_state_t;

static noxmqtt_test_client_state_t* g_publisher_state = NULL;
static noxmqtt_test_client_state_t* g_subscriber_state = NULL;
static noxmqtt_test_client_state_t* g_session_state = NULL;
static noxmqtt_test_client_state_t* g_aux_state = NULL;
static uint32_t g_client_counter = 0U;

/**
 * @brief Copies a bounded byte sequence into a fixed-size buffer.
 *
 * @param[out] dst Destination buffer.
 * @param[in] dst_len Destination capacity in bytes.
 * @param[in] src Source bytes to copy.
 * @param[in] src_len Number of bytes requested from the source.
 *
 * @return Number of bytes copied into the destination buffer.
 */
static uint16_t noxmqtt_test_copy_bytes(uint8_t* dst, uint16_t dst_len, const uint8_t* src, uint16_t src_len)
{
    uint16_t copy_len = 0U;

    if (dst == NULL || dst_len == 0U) {
        return 0U;
    }

    if (src == NULL || src_len == 0U) {
        memset(dst, 0, dst_len);
        return 0U;
    }

    copy_len = (src_len < dst_len) ? src_len : dst_len;
    memcpy(dst, src, copy_len);
    if (copy_len < dst_len) {
        memset(&dst[copy_len], 0, (size_t)(dst_len - copy_len));
    }

    return copy_len;
}

/**
 * @brief Copies a bounded string view into a null-terminated buffer.
 *
 * @param[out] dst Destination character buffer.
 * @param[in] dst_len Destination capacity including the null terminator.
 * @param[in] src Source character bytes.
 * @param[in] src_len Number of bytes requested from the source.
 *
 * @return Number of characters copied, excluding the null terminator.
 */
static uint16_t noxmqtt_test_copy_string(char* dst, uint16_t dst_len, const char* src, uint16_t src_len)
{
    uint16_t copy_len = 0U;

    if (dst == NULL || dst_len == 0U) {
        return 0U;
    }

    dst[0] = '\0';
    if (src == NULL || src_len == 0U) {
        return 0U;
    }

    copy_len = (src_len < (uint16_t)(dst_len - 1U)) ? src_len : (uint16_t)(dst_len - 1U);
    memcpy(dst, src, copy_len);
    dst[copy_len] = '\0';
    return copy_len;
}

/**
 * @brief Reports an assertion failure when a condition is false.
 *
 * @param[in] condition Condition under test.
 * @param[in] message Failure message to print when the condition is false.
 *
 * @return Non-zero when the assertion passes, otherwise zero.
 */
static int noxmqtt_test_assert_true(int condition, const char* message)
{
    if (!condition) {
        fprintf(stderr, "Assertion failed: %s\n", message);
        return 0;
    }

    return 1;
}

/**
 * @brief Returns the broker host used by integration tests.
 *
 * @return Null-terminated broker host string.
 */
static const char* noxmqtt_test_broker_host(void)
{
    const char* env = getenv("NOXMQTT_TEST_BROKER_HOST");

    if (env != NULL && env[0] != '\0') {
        return env;
    }

    return "127.0.0.1";
}

/**
 * @brief Returns the broker port used by integration tests.
 *
 * @return Broker TCP port number.
 */
static uint16_t noxmqtt_test_broker_port(void)
{
    const char* env = getenv("NOXMQTT_TEST_BROKER_PORT");
    char* endptr = NULL;
    long value = 1883L;

    if (env != NULL && env[0] != '\0') {
        value = strtol(env, &endptr, 10);
        if (endptr == NULL || *endptr != '\0' || value <= 0L || value > 65535L) {
            return 1883U;
        }
    }

    return (uint16_t)value;
}

/**
 * @brief Sleeps for the requested number of milliseconds.
 *
 * @param[in] delay_ms Delay duration in milliseconds.
 */
static void noxmqtt_test_sleep_ms(uint32_t delay_ms)
{
    struct timespec req;

    req.tv_sec = (time_t)(delay_ms / 1000U);
    req.tv_nsec = (long)((delay_ms % 1000U) * 1000000UL);
    while (nanosleep(&req, &req) != 0 && errno == EINTR) {
    }
}

/**
 * @brief Pumps incoming transport bytes into a single client.
 *
 * @param[in] state Client state whose transport should be polled.
 *
 * @return Non-zero when the transport remains usable, otherwise zero.
 */
static int noxmqtt_test_poll_client(noxmqtt_test_client_state_t* state)
{
    noxmqtt_test_transport_ctx_t* transport = NULL;
    uint8_t buffer[NOXMQTT_RX_BUF_SIZE];

    if (state == NULL || state->client.transport_ctx == NULL) {
        return 1;
    }

    transport = (noxmqtt_test_transport_ctx_t*)state->client.transport_ctx;
    if (transport->socket_fd < 0 || transport->recv_cb == NULL) {
        return 1;
    }

    for (;;) {
        ssize_t recv_len = recv(transport->socket_fd, buffer, sizeof(buffer), 0);
        if (recv_len > 0) {
            transport->recv_cb(&state->client, buffer, (uint16_t)recv_len);
            continue;
        }

        if (recv_len == 0) {
            (void)noxmqtt_transport_disconnect(&state->client);
            noxmqtt_transport_notify_disconnected(&state->client, NOXMQTT_RC_ERROR_TRANSPORT);
            return 0;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 1;
        }
        if (errno == EINTR) {
            continue;
        }

        (void)noxmqtt_transport_disconnect(&state->client);
        noxmqtt_transport_notify_disconnected(&state->client, NOXMQTT_RC_ERROR_TRANSPORT);
        return 0;
    }
}

/**
 * @brief Pumps all clients through transport receive and periodic processing.
 *
 * @param[in] states Array of client-state pointers to poll.
 * @param[in] state_count Number of entries in `states`.
 */
static void noxmqtt_test_pump_all(noxmqtt_test_client_state_t** states, size_t state_count)
{
    size_t i = 0U;

    for (i = 0U; i < state_count; ++i) {
        if (states[i] == NULL) {
            continue;
        }
        (void)noxmqtt_test_poll_client(states[i]);
        (void)noxmqtt_process(&states[i]->client);
    }
}

/**
 * @brief Waits until a client connects or reports an error.
 *
 * @param[in] states Array of client-state pointers to poll.
 * @param[in] state_count Number of entries in `states`.
 * @param[in] target Client expected to connect.
 * @param[in] timeout_ms Maximum wait duration in milliseconds.
 *
 * @return Non-zero when the client connects before timeout, otherwise zero.
 */
static int noxmqtt_test_wait_for_connect(noxmqtt_test_client_state_t** states,
                                         size_t state_count,
                                         noxmqtt_test_client_state_t* target,
                                         uint32_t timeout_ms)
{
    uint32_t start_ms = noxmqtt_tal_time_ms();

    while ((noxmqtt_tal_time_ms() - start_ms) < timeout_ms) {
        noxmqtt_test_pump_all(states, state_count);
        if (target->observed.connect_count > 0U) {
            return 1;
        }
        if (target->observed.connect_error_count > 0U || target->observed.error_count > 0U) {
            return 0;
        }
        noxmqtt_test_sleep_ms(NOXMQTT_TEST_POLL_SLEEP_MS);
    }

    noxmqtt_test_pump_all(states, state_count);
    return (target->observed.connect_count > 0U) ? 1 : 0;
}

/**
 * @brief Waits until a client receives a subscription acknowledgement.
 *
 * @param[in] states Array of client-state pointers to poll.
 * @param[in] state_count Number of entries in `states`.
 * @param[in] target Client expected to receive `SUBACK`.
 * @param[in] timeout_ms Maximum wait duration in milliseconds.
 *
 * @return Non-zero when the acknowledgement arrives before timeout, otherwise zero.
 */
static int noxmqtt_test_wait_for_subscribed(noxmqtt_test_client_state_t** states,
                                            size_t state_count,
                                            noxmqtt_test_client_state_t* target,
                                            uint32_t timeout_ms)
{
    uint32_t start_ms = noxmqtt_tal_time_ms();

    while ((noxmqtt_tal_time_ms() - start_ms) < timeout_ms) {
        noxmqtt_test_pump_all(states, state_count);
        if (target->observed.subscribed_count > 0U) {
            return 1;
        }
        if (target->observed.error_count > 0U) {
            return 0;
        }
        noxmqtt_test_sleep_ms(NOXMQTT_TEST_POLL_SLEEP_MS);
    }

    noxmqtt_test_pump_all(states, state_count);
    return (target->observed.subscribed_count > 0U) ? 1 : 0;
}

/**
 * @brief Waits until a client publishes at least a target number of QoS 1 messages.
 *
 * @param[in] states Array of client-state pointers to poll.
 * @param[in] state_count Number of entries in `states`.
 * @param[in] target Client expected to receive publish acknowledgements.
 * @param[in] expected_count Minimum number of published events to observe.
 * @param[in] timeout_ms Maximum wait duration in milliseconds.
 *
 * @return Non-zero when the expected count is reached before timeout, otherwise zero.
 */
static int noxmqtt_test_wait_for_published(noxmqtt_test_client_state_t** states,
                                           size_t state_count,
                                           noxmqtt_test_client_state_t* target,
                                           uint32_t expected_count,
                                           uint32_t timeout_ms)
{
    uint32_t start_ms = noxmqtt_tal_time_ms();

    while ((noxmqtt_tal_time_ms() - start_ms) < timeout_ms) {
        noxmqtt_test_pump_all(states, state_count);
        if (target->observed.published_count >= expected_count) {
            return 1;
        }
        if (target->observed.error_count > 0U) {
            return 0;
        }
        noxmqtt_test_sleep_ms(NOXMQTT_TEST_POLL_SLEEP_MS);
    }

    noxmqtt_test_pump_all(states, state_count);
    return (target->observed.published_count >= expected_count) ? 1 : 0;
}

/**
 * @brief Waits until a client receives at least a target number of publish events.
 *
 * @param[in] states Array of client-state pointers to poll.
 * @param[in] state_count Number of entries in `states`.
 * @param[in] target Client expected to receive messages.
 * @param[in] expected_count Minimum number of receive events to observe.
 * @param[in] timeout_ms Maximum wait duration in milliseconds.
 *
 * @return Non-zero when the expected count is reached before timeout, otherwise zero.
 */
static int noxmqtt_test_wait_for_received(noxmqtt_test_client_state_t** states,
                                          size_t state_count,
                                          noxmqtt_test_client_state_t* target,
                                          uint32_t expected_count,
                                          uint32_t timeout_ms)
{
    uint32_t start_ms = noxmqtt_tal_time_ms();

    while ((noxmqtt_tal_time_ms() - start_ms) < timeout_ms) {
        noxmqtt_test_pump_all(states, state_count);
        if (target->observed.received_count >= expected_count) {
            return 1;
        }
        if (target->observed.error_count > 0U) {
            return 0;
        }
        noxmqtt_test_sleep_ms(NOXMQTT_TEST_POLL_SLEEP_MS);
    }

    noxmqtt_test_pump_all(states, state_count);
    return (target->observed.received_count >= expected_count) ? 1 : 0;
}

/**
 * @brief Pumps clients for a fixed period without waiting on a specific event.
 *
 * @param[in] states Array of client-state pointers to poll.
 * @param[in] state_count Number of entries in `states`.
 * @param[in] duration_ms Total pump duration in milliseconds.
 */
static void noxmqtt_test_run_for(noxmqtt_test_client_state_t** states, size_t state_count, uint32_t duration_ms)
{
    uint32_t start_ms = noxmqtt_tal_time_ms();

    while ((noxmqtt_tal_time_ms() - start_ms) < duration_ms) {
        noxmqtt_test_pump_all(states, state_count);
        noxmqtt_test_sleep_ms(NOXMQTT_TEST_POLL_SLEEP_MS);
    }
}

/**
 * @brief Clears the observed-event snapshot for a client.
 *
 * @param[in,out] state Client state to reset.
 */
static void noxmqtt_test_reset_observed(noxmqtt_test_client_state_t* state)
{
    if (state == NULL) {
        return;
    }

    memset(&state->observed, 0, sizeof(state->observed));
}

/**
 * @brief Builds a unique client identifier or topic suffix.
 *
 * @param[out] dst Destination string buffer.
 * @param[in] dst_len Destination buffer capacity.
 * @param[in] prefix Prefix to place at the beginning of the string.
 */
static void noxmqtt_test_make_unique_string(char* dst, size_t dst_len, const char* prefix)
{
    ++g_client_counter;
    (void)snprintf(dst, dst_len, "%s-%lu-%" PRIu32, prefix, (unsigned long)getpid(), g_client_counter);
}

/**
 * @brief Builds a unique MQTT topic string for a test case.
 *
 * @param[out] dst Destination topic buffer.
 * @param[in] dst_len Destination buffer capacity.
 * @param[in] group Topic group used to keep tests distinct.
 */
static void noxmqtt_test_make_topic(char* dst, size_t dst_len, const char* group)
{
    char suffix[NOXMQTT_TEST_CLIENT_ID_LEN];

    noxmqtt_test_make_unique_string(suffix, sizeof(suffix), group);
    (void)snprintf(dst, dst_len, "noxmqtt/ci/%s", suffix);
}

/**
 * @brief Prepares a client state for a new connection.
 *
 * @param[out] state Client state to populate.
 * @param[in] role_name Human-readable role used in diagnostics.
 * @param[in] callback Callback wrapper assigned to this client.
 * @param[in] protocol_version MQTT protocol version to use.
 * @param[in] clean_session Clean-session / clean-start flag.
 * @param[in] client_id_prefix Prefix used to generate a unique client identifier.
 */
static void noxmqtt_test_prepare_client(noxmqtt_test_client_state_t* state,
                                        const char* role_name,
                                        noxmqtt_callback_t callback,
                                        noxmqtt_protocol_ver_t protocol_version,
                                        uint8_t clean_session,
                                        const char* client_id_prefix)
{
    memset(state, 0, sizeof(*state));
    noxmqtt_test_copy_string(state->role_name, sizeof(state->role_name), role_name, (uint16_t)strlen(role_name));
    noxmqtt_test_copy_string(state->broker_host,
                             sizeof(state->broker_host),
                             noxmqtt_test_broker_host(),
                             (uint16_t)strlen(noxmqtt_test_broker_host()));
    noxmqtt_test_make_unique_string(state->client_id, sizeof(state->client_id), client_id_prefix);

    state->conf.server.addr = state->broker_host;
    state->conf.server.port = noxmqtt_test_broker_port();
    state->conf.server.mode = NOXMQTT_TRANSPORT_TCP;
    state->conf.server.disable_auto_reconnect = 1U;
    state->conf.server.network_timeout_ms = 1000U;
    state->conf.clean_session = clean_session;
    state->conf.client_identifier = state->client_id;
    state->conf.callback = callback;
    state->conf.protocol_version = protocol_version;
}

/**
 * @brief Initializes and connects a client, then waits for `CONNACK`.
 *
 * @param[in,out] state Client state to start.
 * @param[in] states Array of client-state pointers to poll.
 * @param[in] state_count Number of entries in `states`.
 *
 * @return Non-zero when the client connects successfully, otherwise zero.
 */
static int noxmqtt_test_start_client(noxmqtt_test_client_state_t* state,
                                     noxmqtt_test_client_state_t** states,
                                     size_t state_count)
{
    noxmqtt_rc_t rc = NOXMQTT_RC_ERROR;

    rc = noxmqtt_init(&state->client, NOXMQTT_DEBUG_LVL_NONE);
    if (!noxmqtt_test_assert_true(rc == NOXMQTT_SUCCESS, "noxmqtt_init should succeed")) {
        return 0;
    }

    rc = noxmqtt_connect(&state->client, &state->conf, 20U);
    if (!noxmqtt_test_assert_true(rc == NOXMQTT_SUCCESS, "noxmqtt_connect should succeed")) {
        return 0;
    }

    if (!noxmqtt_test_wait_for_connect(states, state_count, state, NOXMQTT_TEST_WAIT_TIMEOUT_MS)) {
        fprintf(stderr, "%s did not connect in time\n", state->role_name);
        return 0;
    }

    if (!noxmqtt_test_assert_true(state->observed.error_count == 0U, "client should connect without MQTT error events")) {
        return 0;
    }

    return 1;
}

/**
 * @brief Disconnects and deinitializes a client if it was initialized.
 *
 * @param[in,out] state Client state to tear down.
 */
static void noxmqtt_test_shutdown_client(noxmqtt_test_client_state_t* state)
{
    if (state == NULL || state->client.flag_initialized == 0U) {
        return;
    }

    (void)noxmqtt_disconnect(&state->client);
    (void)noxmqtt_deinit(&state->client);
}

/**
 * @brief Copies relevant event fields into a client-observation snapshot.
 *
 * @param[in,out] state Destination client state.
 * @param[in] evt_data Event data supplied by NoxMQTT.
 */
static void noxmqtt_test_record_event(noxmqtt_test_client_state_t* state, noxmqtt_evt_data_t* evt_data)
{
    if (state == NULL || evt_data == NULL) {
        return;
    }

    switch (evt_data->evt_id) {
        case NOXMQTT_EVT_CONNECT:
            state->observed.connect_count++;
            state->observed.last_connect_reason_code = evt_data->evt.connect_evt.reason_code;
            state->observed.last_session_present = evt_data->evt.connect_evt.session_present;
            state->observed.last_topic_alias_maximum = evt_data->evt.connect_evt.topic_alias_maximum;
            state->observed.last_receive_maximum = evt_data->evt.connect_evt.receive_maximum;
            break;
        case NOXMQTT_EVT_CONNECT_ERROR:
            state->observed.connect_error_count++;
            break;
        case NOXMQTT_EVT_PUBLISHED:
            state->observed.published_count++;
            state->observed.last_published_reason_code = evt_data->evt.published_evt.reason_code;
            state->observed.last_packet_identifier = evt_data->evt.published_evt.packet_identifier;
            break;
        case NOXMQTT_EVT_RECEIVED:
            state->observed.received_count++;
            state->observed.last_received_packet_identifier = evt_data->evt.received_evt.packet_identifier;
            state->observed.last_received_topic_alias = evt_data->evt.received_evt.topic_alias;
            state->observed.last_received_payload_format_indicator =
                evt_data->evt.received_evt.payload_format_indicator;
            state->observed.last_received_message_expiry_interval =
                evt_data->evt.received_evt.message_expiry_interval;
            state->observed.last_subscription_identifier = evt_data->evt.received_evt.subscription_identifier;
            state->observed.last_subscription_identifier_count =
                evt_data->evt.received_evt.subscription_identifier_count;
            if (evt_data->evt.received_evt.subscription_identifiers != NULL &&
                evt_data->evt.received_evt.subscription_identifier_count > 0U) {
                uint16_t copy_count = evt_data->evt.received_evt.subscription_identifier_count;
                if (copy_count > NOXMQTT_MAX_SUBSCRIPTION_IDENTIFIERS) {
                    copy_count = NOXMQTT_MAX_SUBSCRIPTION_IDENTIFIERS;
                }
                memcpy(state->observed.last_subscription_identifiers,
                       evt_data->evt.received_evt.subscription_identifiers,
                       copy_count * sizeof(uint32_t));
            }
            state->observed.last_topic_len =
                noxmqtt_test_copy_string(state->observed.last_topic,
                                         sizeof(state->observed.last_topic),
                                         evt_data->evt.received_evt.topic,
                                         evt_data->evt.received_evt.topic_len);
            state->observed.last_payload_len =
                noxmqtt_test_copy_bytes(state->observed.last_payload,
                                        sizeof(state->observed.last_payload),
                                        (const uint8_t*)evt_data->evt.received_evt.payload,
                                        evt_data->evt.received_evt.payload_len);
            state->observed.last_response_topic_len =
                noxmqtt_test_copy_string(state->observed.last_response_topic,
                                         sizeof(state->observed.last_response_topic),
                                         evt_data->evt.received_evt.response_topic,
                                         evt_data->evt.received_evt.response_topic_len);
            state->observed.last_correlation_data_len =
                noxmqtt_test_copy_bytes(state->observed.last_correlation_data,
                                        sizeof(state->observed.last_correlation_data),
                                        evt_data->evt.received_evt.correlation_data,
                                        evt_data->evt.received_evt.correlation_data_len);
            state->observed.last_content_type_len =
                noxmqtt_test_copy_string(state->observed.last_content_type,
                                         sizeof(state->observed.last_content_type),
                                         evt_data->evt.received_evt.content_type,
                                         evt_data->evt.received_evt.content_type_len);
            break;
        case NOXMQTT_EVT_SUBSCRIBED:
            state->observed.subscribed_count++;
            state->observed.last_packet_identifier = evt_data->evt.subscribed_evt.packet_identifier;
            state->observed.last_suback_return_code = evt_data->evt.subscribed_evt.return_code;
            state->observed.last_suback_reason_code_count = evt_data->evt.subscribed_evt.reason_code_count;
            if (evt_data->evt.subscribed_evt.reason_codes != NULL &&
                evt_data->evt.subscribed_evt.reason_code_count > 0U) {
                uint16_t copy_count = evt_data->evt.subscribed_evt.reason_code_count;
                if (copy_count > NOXMQTT_TEST_REASON_CODES_LEN) {
                    copy_count = NOXMQTT_TEST_REASON_CODES_LEN;
                }
                memcpy(state->observed.last_suback_reason_codes,
                       evt_data->evt.subscribed_evt.reason_codes,
                       copy_count);
            }
            break;
        case NOXMQTT_EVT_UNSUBSCRIBED:
            state->observed.unsubscribed_count++;
            break;
        case NOXMQTT_EVT_DISCONNECT:
            state->observed.disconnect_count++;
            break;
        case NOXMQTT_EVT_AUTH:
            state->observed.auth_count++;
            break;
        case NOXMQTT_EVT_ERROR:
            state->observed.error_count++;
            state->observed.last_error_rc = evt_data->evt.error_evt.rc;
            break;
        case NOXMQTT_EVT_PINGRESP:
        case NOXMQTT_EVT_PUBREL:
        default:
            break;
    }
}

/**
 * @brief Forwards publisher callback events into the shared recorder.
 *
 * @param[in] evt_data Event data supplied by NoxMQTT.
 */
static void noxmqtt_test_publisher_callback(noxmqtt_evt_data_t* evt_data)
{
    noxmqtt_test_record_event(g_publisher_state, evt_data);
}

/**
 * @brief Forwards subscriber callback events into the shared recorder.
 *
 * @param[in] evt_data Event data supplied by NoxMQTT.
 */
static void noxmqtt_test_subscriber_callback(noxmqtt_evt_data_t* evt_data)
{
    noxmqtt_test_record_event(g_subscriber_state, evt_data);
}

/**
 * @brief Forwards session-test callback events into the shared recorder.
 *
 * @param[in] evt_data Event data supplied by NoxMQTT.
 */
static void noxmqtt_test_session_callback(noxmqtt_evt_data_t* evt_data)
{
    noxmqtt_test_record_event(g_session_state, evt_data);
}

/**
 * @brief Forwards auxiliary callback events into the shared recorder.
 *
 * @param[in] evt_data Event data supplied by NoxMQTT.
 */
static void noxmqtt_test_aux_callback(noxmqtt_evt_data_t* evt_data)
{
    noxmqtt_test_record_event(g_aux_state, evt_data);
}

/**
 * @brief Exercises MQTT 3.1.1 end-to-end publish flows at QoS 0, 1, and 2.
 *
 * @return Non-zero when the test passes, otherwise zero.
 */
static int noxmqtt_test_mqtt311_roundtrip(void)
{
    noxmqtt_test_client_state_t publisher;
    noxmqtt_test_client_state_t subscriber;
    noxmqtt_test_client_state_t* states[] = {&subscriber, &publisher};
    noxmqtt_topic_sub_t subscription;
    char topic[NOXMQTT_TEST_TOPIC_LEN];
    uint8_t qos0_payload[] = "mqtt311-qos0";
    uint8_t qos1_payload[] = "mqtt311-qos1";
    uint8_t qos2_payload[] = "mqtt311-qos2";
    noxmqtt_rc_t rc = NOXMQTT_RC_ERROR;
    int ok = 0;

    noxmqtt_test_make_topic(topic, sizeof(topic), "mqtt311-roundtrip");
    noxmqtt_test_prepare_client(&subscriber,
                                "subscriber311",
                                noxmqtt_test_subscriber_callback,
                                NOXMQTT_PROTOCOL_V3_1_1,
                                1U,
                                "sub311");
    noxmqtt_test_prepare_client(&publisher,
                                "publisher311",
                                noxmqtt_test_publisher_callback,
                                NOXMQTT_PROTOCOL_V3_1_1,
                                1U,
                                "pub311");
    g_subscriber_state = &subscriber;
    g_publisher_state = &publisher;

    if (!noxmqtt_test_start_client(&subscriber, states, 2U)) {
        goto cleanup;
    }
    if (!noxmqtt_test_start_client(&publisher, states, 2U)) {
        goto cleanup;
    }

    memset(&subscription, 0, sizeof(subscription));
    subscription.topic = topic;
    subscription.qos = NOXMQTT_QOS2_EXACTLY_ONCE_DELIV;
    rc = noxmqtt_subscribe(&subscriber.client, &subscription, 1U);
    if (!noxmqtt_test_assert_true(rc == NOXMQTT_SUCCESS, "MQTT 3.1.1 subscribe should succeed")) {
        goto cleanup;
    }
    if (!noxmqtt_test_wait_for_subscribed(states, 2U, &subscriber, NOXMQTT_TEST_WAIT_TIMEOUT_MS)) {
        fprintf(stderr, "MQTT 3.1.1 subscriber did not receive SUBACK\n");
        goto cleanup;
    }

    noxmqtt_test_reset_observed(&subscriber);
    rc = noxmqtt_publish_data(&publisher.client,
                              NOXMQTT_QOS0_AT_MOST_ONCE_DELIV,
                              0U,
                              0U,
                              topic,
                              qos0_payload,
                              (uint16_t)(sizeof(qos0_payload) - 1U),
                              NULL);
    if (!noxmqtt_test_assert_true(rc == NOXMQTT_SUCCESS, "MQTT 3.1.1 QoS0 publish should succeed")) {
        goto cleanup;
    }
    if (!noxmqtt_test_wait_for_received(states, 2U, &subscriber, 1U, NOXMQTT_TEST_WAIT_TIMEOUT_MS)) {
        fprintf(stderr, "MQTT 3.1.1 subscriber did not receive QoS0 publish\n");
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(strcmp(subscriber.observed.last_topic, topic) == 0,
                                  "MQTT 3.1.1 QoS0 topic should round-trip")) {
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(subscriber.observed.last_payload_len == (uint16_t)(sizeof(qos0_payload) - 1U) &&
                                  memcmp(subscriber.observed.last_payload,
                                         qos0_payload,
                                         sizeof(qos0_payload) - 1U) == 0,
                                  "MQTT 3.1.1 QoS0 payload should round-trip")) {
        goto cleanup;
    }

    noxmqtt_test_reset_observed(&subscriber);
    noxmqtt_test_reset_observed(&publisher);
    rc = noxmqtt_publish_data(&publisher.client,
                              NOXMQTT_QOS1_AT_LEAST_ONCE_DELIV,
                              0U,
                              0U,
                              topic,
                              qos1_payload,
                              (uint16_t)(sizeof(qos1_payload) - 1U),
                              NULL);
    if (!noxmqtt_test_assert_true(rc == NOXMQTT_SUCCESS, "MQTT 3.1.1 QoS1 publish should succeed")) {
        goto cleanup;
    }
    if (!noxmqtt_test_wait_for_published(states, 2U, &publisher, 1U, NOXMQTT_TEST_WAIT_TIMEOUT_MS)) {
        fprintf(stderr, "MQTT 3.1.1 publisher did not receive PUBACK\n");
        goto cleanup;
    }
    if (!noxmqtt_test_wait_for_received(states, 2U, &subscriber, 1U, NOXMQTT_TEST_WAIT_TIMEOUT_MS)) {
        fprintf(stderr, "MQTT 3.1.1 subscriber did not receive QoS1 publish\n");
        goto cleanup;
    }

    noxmqtt_test_reset_observed(&subscriber);
    noxmqtt_test_reset_observed(&publisher);
    rc = noxmqtt_publish_data(&publisher.client,
                              NOXMQTT_QOS2_EXACTLY_ONCE_DELIV,
                              0U,
                              0U,
                              topic,
                              qos2_payload,
                              (uint16_t)(sizeof(qos2_payload) - 1U),
                              NULL);
    if (!noxmqtt_test_assert_true(rc == NOXMQTT_SUCCESS, "MQTT 3.1.1 QoS2 publish should succeed")) {
        goto cleanup;
    }
    if (!noxmqtt_test_wait_for_received(states, 2U, &subscriber, 1U, NOXMQTT_TEST_WAIT_TIMEOUT_MS)) {
        fprintf(stderr, "MQTT 3.1.1 subscriber did not receive QoS2 publish\n");
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(subscriber.observed.last_payload_len == (uint16_t)(sizeof(qos2_payload) - 1U) &&
                                  memcmp(subscriber.observed.last_payload,
                                         qos2_payload,
                                         sizeof(qos2_payload) - 1U) == 0,
                                  "MQTT 3.1.1 QoS2 payload should round-trip")) {
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(subscriber.observed.error_count == 0U && publisher.observed.error_count == 0U,
                                  "MQTT 3.1.1 roundtrip should not emit error events")) {
        goto cleanup;
    }

    ok = 1;

cleanup:
    noxmqtt_test_shutdown_client(&publisher);
    noxmqtt_test_shutdown_client(&subscriber);
    return ok;
}

/**
 * @brief Exercises MQTT 5 publish properties and client topic-alias reuse.
 *
 * @return Non-zero when the test passes, otherwise zero.
 */
static int noxmqtt_test_mqtt5_properties_and_aliases(void)
{
    noxmqtt_test_client_state_t publisher;
    noxmqtt_test_client_state_t subscriber;
    noxmqtt_test_client_state_t* states[] = {&subscriber, &publisher};
    noxmqtt_topic_sub_t subscription;
    noxmqtt_mqtt5_user_property_t user_property;
    noxmqtt_mqtt5_publish_props_t publish_props;
    noxmqtt_mqtt5_publish_props_t alias_only_props;
    char topic[NOXMQTT_TEST_TOPIC_LEN];
    char response_topic[NOXMQTT_TEST_RESPONSE_TOPIC_LEN];
    char empty_topic[] = "";
    uint8_t first_payload[] = "mqtt5-first";
    uint8_t second_payload[] = "mqtt5-second";
    uint8_t correlation_data[] = {0x41U, 0x42U, 0x43U, 0x44U};
    noxmqtt_rc_t rc = NOXMQTT_RC_ERROR;
    int ok = 0;

    noxmqtt_test_make_topic(topic, sizeof(topic), "mqtt5-props");
    noxmqtt_test_make_topic(response_topic, sizeof(response_topic), "mqtt5-response");
    noxmqtt_test_prepare_client(&subscriber,
                                "subscriber5",
                                noxmqtt_test_subscriber_callback,
                                NOXMQTT_PROTOCOL_V5_0,
                                1U,
                                "sub5");
    noxmqtt_test_prepare_client(&publisher,
                                "publisher5",
                                noxmqtt_test_publisher_callback,
                                NOXMQTT_PROTOCOL_V5_0,
                                1U,
                                "pub5");
    g_subscriber_state = &subscriber;
    g_publisher_state = &publisher;

    if (!noxmqtt_test_start_client(&subscriber, states, 2U)) {
        goto cleanup;
    }
    if (!noxmqtt_test_start_client(&publisher, states, 2U)) {
        goto cleanup;
    }

    memset(&subscription, 0, sizeof(subscription));
    subscription.topic = topic;
    subscription.qos = NOXMQTT_QOS1_AT_LEAST_ONCE_DELIV;
    subscription.mqtt5.subscribe_identifier = 7U;
    rc = noxmqtt_subscribe(&subscriber.client, &subscription, 1U);
    if (!noxmqtt_test_assert_true(rc == NOXMQTT_SUCCESS, "MQTT 5 subscribe should succeed")) {
        goto cleanup;
    }
    if (!noxmqtt_test_wait_for_subscribed(states, 2U, &subscriber, NOXMQTT_TEST_WAIT_TIMEOUT_MS)) {
        fprintf(stderr, "MQTT 5 subscriber did not receive SUBACK\n");
        goto cleanup;
    }

    memset(&user_property, 0, sizeof(user_property));
    user_property.name = "suite";
    user_property.name_len = 5U;
    user_property.value = "integration";
    user_property.value_len = 11U;

    memset(&publish_props, 0, sizeof(publish_props));
    publish_props.message_expiry_interval = 60U;
    publish_props.topic_alias = 1U;
    publish_props.response_topic = response_topic;
    publish_props.correlation_data = correlation_data;
    publish_props.correlation_data_len = (uint16_t)sizeof(correlation_data);
    publish_props.content_type = "application/octet-stream";
    publish_props.payload_format_indicator = 1U;
    publish_props.user_properties = &user_property;
    publish_props.user_property_count = 1U;

    noxmqtt_test_reset_observed(&publisher);
    noxmqtt_test_reset_observed(&subscriber);
    rc = noxmqtt_publish_data(&publisher.client,
                              NOXMQTT_QOS1_AT_LEAST_ONCE_DELIV,
                              0U,
                              0U,
                              topic,
                              first_payload,
                              (uint16_t)(sizeof(first_payload) - 1U),
                              &publish_props);
    if (!noxmqtt_test_assert_true(rc == NOXMQTT_SUCCESS, "MQTT 5 publish with properties should succeed")) {
        goto cleanup;
    }
    if (!noxmqtt_test_wait_for_published(states, 2U, &publisher, 1U, NOXMQTT_TEST_WAIT_TIMEOUT_MS)) {
        fprintf(stderr, "MQTT 5 publisher did not receive PUBACK\n");
        goto cleanup;
    }
    if (!noxmqtt_test_wait_for_received(states, 2U, &subscriber, 1U, NOXMQTT_TEST_WAIT_TIMEOUT_MS)) {
        fprintf(stderr, "MQTT 5 subscriber did not receive property-bearing publish\n");
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(strcmp(subscriber.observed.last_topic, topic) == 0,
                                  "MQTT 5 publish topic should round-trip")) {
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(subscriber.observed.last_payload_len == (uint16_t)(sizeof(first_payload) - 1U) &&
                                  memcmp(subscriber.observed.last_payload,
                                         first_payload,
                                         sizeof(first_payload) - 1U) == 0,
                                  "MQTT 5 publish payload should round-trip")) {
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(subscriber.observed.last_received_payload_format_indicator == 1U,
                                  "MQTT 5 payload format indicator should be preserved")) {
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(subscriber.observed.last_received_message_expiry_interval == 60U,
                                  "MQTT 5 message expiry interval should be preserved")) {
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(subscriber.observed.last_subscription_identifier == 7U &&
                                  subscriber.observed.last_subscription_identifier_count >= 1U &&
                                  subscriber.observed.last_subscription_identifiers[0] == 7U,
                                  "MQTT 5 subscription identifier should be preserved")) {
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(strcmp(subscriber.observed.last_response_topic, response_topic) == 0,
                                  "MQTT 5 response topic should be preserved")) {
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(subscriber.observed.last_correlation_data_len == (uint16_t)sizeof(correlation_data) &&
                                  memcmp(subscriber.observed.last_correlation_data,
                                         correlation_data,
                                         sizeof(correlation_data)) == 0,
                                  "MQTT 5 correlation data should be preserved")) {
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(strcmp(subscriber.observed.last_content_type, "application/octet-stream") == 0,
                                  "MQTT 5 content type should be preserved")) {
        goto cleanup;
    }

    if (!noxmqtt_test_assert_true(publisher.client.broker_topic_alias_maximum >= 1U,
                                  "broker should advertise MQTT 5 topic-alias support for CI tests")) {
        goto cleanup;
    }

    memset(&alias_only_props, 0, sizeof(alias_only_props));
    alias_only_props.topic_alias = 1U;

    noxmqtt_test_reset_observed(&subscriber);
    rc = noxmqtt_publish_data(&publisher.client,
                              NOXMQTT_QOS0_AT_MOST_ONCE_DELIV,
                              0U,
                              0U,
                              empty_topic,
                              second_payload,
                              (uint16_t)(sizeof(second_payload) - 1U),
                              &alias_only_props);
    if (!noxmqtt_test_assert_true(rc == NOXMQTT_SUCCESS, "MQTT 5 alias-only publish should succeed")) {
        goto cleanup;
    }
    if (!noxmqtt_test_wait_for_received(states, 2U, &subscriber, 1U, NOXMQTT_TEST_WAIT_TIMEOUT_MS)) {
        fprintf(stderr, "MQTT 5 subscriber did not receive alias-only publish\n");
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(strcmp(subscriber.observed.last_topic, topic) == 0,
                                  "MQTT 5 alias-only publish should resolve to the original topic")) {
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(subscriber.observed.last_payload_len == (uint16_t)(sizeof(second_payload) - 1U) &&
                                  memcmp(subscriber.observed.last_payload,
                                         second_payload,
                                         sizeof(second_payload) - 1U) == 0,
                                  "MQTT 5 alias-only payload should round-trip")) {
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(subscriber.observed.error_count == 0U && publisher.observed.error_count == 0U,
                                  "MQTT 5 publish flow should not emit error events")) {
        goto cleanup;
    }

    ok = 1;

cleanup:
    noxmqtt_test_shutdown_client(&publisher);
    noxmqtt_test_shutdown_client(&subscriber);
    return ok;
}

/**
 * @brief Exercises MQTT 5 persistent-session reconnect behavior against a broker.
 *
 * @return Non-zero when the test passes, otherwise zero.
 */
static int noxmqtt_test_mqtt5_session_resume(void)
{
    noxmqtt_test_client_state_t session_client;
    noxmqtt_test_client_state_t aux_publisher;
    noxmqtt_test_client_state_t* states[] = {&session_client, &aux_publisher};
    noxmqtt_topic_sub_t subscription;
    char topic[NOXMQTT_TEST_TOPIC_LEN];
    uint8_t payload[] = "mqtt5-resume";
    noxmqtt_rc_t rc = NOXMQTT_RC_ERROR;
    int ok = 0;

    noxmqtt_test_make_topic(topic, sizeof(topic), "mqtt5-session");
    noxmqtt_test_prepare_client(&session_client,
                                "session5",
                                noxmqtt_test_session_callback,
                                NOXMQTT_PROTOCOL_V5_0,
                                0U,
                                "session5");
    noxmqtt_test_prepare_client(&aux_publisher,
                                "aux5",
                                noxmqtt_test_aux_callback,
                                NOXMQTT_PROTOCOL_V5_0,
                                1U,
                                "aux5");
    session_client.conf.mqtt5.session_expiry_interval = 300U;
    g_session_state = &session_client;
    g_aux_state = &aux_publisher;

    if (!noxmqtt_test_start_client(&session_client, states, 2U)) {
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(session_client.observed.last_session_present == 0U,
                                  "first MQTT 5 session connect should not report a stored session")) {
        goto cleanup;
    }

    memset(&subscription, 0, sizeof(subscription));
    subscription.topic = topic;
    subscription.qos = NOXMQTT_QOS1_AT_LEAST_ONCE_DELIV;
    subscription.mqtt5.subscribe_identifier = 19U;
    rc = noxmqtt_subscribe(&session_client.client, &subscription, 1U);
    if (!noxmqtt_test_assert_true(rc == NOXMQTT_SUCCESS, "MQTT 5 persistent subscribe should succeed")) {
        goto cleanup;
    }
    if (!noxmqtt_test_wait_for_subscribed(states, 2U, &session_client, NOXMQTT_TEST_WAIT_TIMEOUT_MS)) {
        fprintf(stderr, "MQTT 5 session client did not receive SUBACK\n");
        goto cleanup;
    }

    rc = noxmqtt_disconnect(&session_client.client);
    if (!noxmqtt_test_assert_true(rc == NOXMQTT_SUCCESS, "MQTT 5 session disconnect should succeed")) {
        goto cleanup;
    }

    noxmqtt_test_reset_observed(&session_client);
    rc = noxmqtt_connect(&session_client.client, &session_client.conf, 20U);
    if (!noxmqtt_test_assert_true(rc == NOXMQTT_SUCCESS, "MQTT 5 reconnect should succeed")) {
        goto cleanup;
    }
    if (!noxmqtt_test_wait_for_connect(states, 2U, &session_client, NOXMQTT_TEST_WAIT_TIMEOUT_MS)) {
        fprintf(stderr, "MQTT 5 session client did not reconnect\n");
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(session_client.observed.last_session_present == 1U,
                                  "reconnect should resume the stored MQTT 5 session")) {
        goto cleanup;
    }
    noxmqtt_test_run_for(states, 2U, NOXMQTT_TEST_SHORT_WAIT_MS);
    if (!noxmqtt_test_assert_true(session_client.observed.subscribed_count == 0U,
                                  "session-resume reconnect should not trigger an extra resubscribe")) {
        goto cleanup;
    }

    if (!noxmqtt_test_start_client(&aux_publisher, states, 2U)) {
        goto cleanup;
    }
    noxmqtt_test_reset_observed(&session_client);
    rc = noxmqtt_publish_data(&aux_publisher.client,
                              NOXMQTT_QOS1_AT_LEAST_ONCE_DELIV,
                              0U,
                              0U,
                              topic,
                              payload,
                              (uint16_t)(sizeof(payload) - 1U),
                              NULL);
    if (!noxmqtt_test_assert_true(rc == NOXMQTT_SUCCESS, "MQTT 5 resumed-session publish should succeed")) {
        goto cleanup;
    }
    if (!noxmqtt_test_wait_for_received(states, 2U, &session_client, 1U, NOXMQTT_TEST_WAIT_TIMEOUT_MS)) {
        fprintf(stderr, "MQTT 5 resumed session did not receive a publish after reconnect\n");
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(strcmp(session_client.observed.last_topic, topic) == 0,
                                  "MQTT 5 resumed session should receive messages on the stored subscription")) {
        goto cleanup;
    }
    if (!noxmqtt_test_assert_true(session_client.observed.last_subscription_identifier == 19U,
                                  "MQTT 5 resumed session should preserve subscription identifiers")) {
        goto cleanup;
    }

    ok = 1;

cleanup:
    noxmqtt_test_shutdown_client(&aux_publisher);
    noxmqtt_test_shutdown_client(&session_client);
    return ok;
}

/**
 * @brief Implements the platform transport initialization hook for tests.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] rcv_cback Receive callback to use for incoming bytes.
 *
 * @return 0 on success, otherwise `-1`.
 */
int noxmqtt_transport_init(noxmqtt_client_t* c, noxmqtt_transport_rcv_t rcv_cback)
{
    noxmqtt_test_transport_ctx_t* transport = NULL;

    if (c == NULL || rcv_cback == NULL) {
        return -1;
    }

    transport = (noxmqtt_test_transport_ctx_t*)c->transport_ctx;
    if (transport == NULL) {
        transport = (noxmqtt_test_transport_ctx_t*)calloc(1U, sizeof(*transport));
        if (transport == NULL) {
            return -1;
        }
        transport->socket_fd = -1;
        c->transport_ctx = transport;
    }

    transport->recv_cb = rcv_cback;
    return 0;
}

/**
 * @brief Connects a test client transport to the configured broker.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] conf Client connection configuration.
 *
 * @return 0 on success, otherwise `-1`.
 */
int noxmqtt_transport_connect(noxmqtt_client_t* c, const noxmqtt_client_conf_t* conf)
{
    noxmqtt_test_transport_ctx_t* transport = NULL;
    struct addrinfo hints;
    struct addrinfo* result = NULL;
    struct addrinfo* current = NULL;
    char port_str[8];
    int fd = -1;
    int rc = -1;

    if (c == NULL || conf == NULL || conf->server.addr == NULL || c->transport_ctx == NULL) {
        return -1;
    }

    transport = (noxmqtt_test_transport_ctx_t*)c->transport_ctx;
    if (transport->socket_fd >= 0) {
        (void)close(transport->socket_fd);
        transport->socket_fd = -1;
    }

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    (void)snprintf(port_str, sizeof(port_str), "%u", conf->server.port);

    if (getaddrinfo(conf->server.addr, port_str, &hints, &result) != 0) {
        return -1;
    }

    for (current = result; current != NULL; current = current->ai_next) {
        fd = socket(current->ai_family, current->ai_socktype, current->ai_protocol);
        if (fd < 0) {
            continue;
        }

        if (connect(fd, current->ai_addr, current->ai_addrlen) == 0) {
            int flags = fcntl(fd, F_GETFL, 0);
            if (flags >= 0) {
                (void)fcntl(fd, F_SETFL, flags | O_NONBLOCK);
            }
            transport->socket_fd = fd;
            rc = 0;
            break;
        }

        (void)close(fd);
        fd = -1;
    }

    freeaddrinfo(result);
    return rc;
}

/**
 * @brief Sends raw bytes over the test transport.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] data Bytes to send.
 * @param[in] len Number of bytes to send.
 *
 * @return 0 on success, otherwise `-1`.
 */
int noxmqtt_transport_send(noxmqtt_client_t* c, const uint8_t* data, uint16_t len)
{
    noxmqtt_test_transport_ctx_t* transport = NULL;
    size_t sent = 0U;

    if (c == NULL || data == NULL || c->transport_ctx == NULL) {
        return -1;
    }

    transport = (noxmqtt_test_transport_ctx_t*)c->transport_ctx;
    if (transport->socket_fd < 0) {
        return -1;
    }

    while (sent < len) {
        ssize_t rc = send(transport->socket_fd, data + sent, (size_t)(len - sent), 0);
        if (rc > 0) {
            sent += (size_t)rc;
            continue;
        }
        if (rc < 0 && errno == EINTR) {
            continue;
        }
        if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            noxmqtt_test_sleep_ms(NOXMQTT_TEST_POLL_SLEEP_MS);
            continue;
        }
        return -1;
    }

    return 0;
}

/**
 * @brief Stub receive-thread entry point for tests without background threads.
 *
 * @param[in] ptr Unused transport context pointer.
 *
 * @return Always returns 0.
 */
int noxmqtt_transport_receive_thread(void* ptr)
{
    (void)ptr;
    return 0;
}

/**
 * @brief Disconnects the test transport socket.
 *
 * @param[in] c NoxMQTT client instance.
 *
 * @return 0 on success, otherwise `-1`.
 */
int noxmqtt_transport_disconnect(noxmqtt_client_t* c)
{
    noxmqtt_test_transport_ctx_t* transport = NULL;

    if (c == NULL || c->transport_ctx == NULL) {
        return -1;
    }

    transport = (noxmqtt_test_transport_ctx_t*)c->transport_ctx;
    if (transport->socket_fd >= 0) {
        (void)shutdown(transport->socket_fd, SHUT_RDWR);
        (void)close(transport->socket_fd);
        transport->socket_fd = -1;
    }

    return 0;
}

/**
 * @brief Stub wait hook for tests without background threads.
 */
void noxmqtt_transport_wait(void)
{
}

/**
 * @brief Returns monotonic time in milliseconds for test polling.
 *
 * @return Current monotonic time in milliseconds.
 */
uint32_t noxmqtt_tal_time_ms(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0U;
    }

    return (uint32_t)((uint64_t)ts.tv_sec * 1000ULL + (uint64_t)(ts.tv_nsec / 1000000L));
}

/**
 * @brief Routes NoxMQTT debug strings to standard error during tests.
 *
 * @param[in] str Null-terminated debug string.
 */
void noxmqtt_hal_debug_printf(const char* str)
{
    if (str != NULL) {
        fputs(str, stderr);
    }
}

/**
 * @brief Runs all broker-backed integration tests.
 *
 * @return Process exit status.
 */
int main(void)
{
    int ok = 1;

    ok &= noxmqtt_test_mqtt311_roundtrip();
    ok &= noxmqtt_test_mqtt5_properties_and_aliases();
    ok &= noxmqtt_test_mqtt5_session_resume();

    if (!ok) {
        return 1;
    }

    printf("noxmqtt_integration_tests passed\n");
    return 0;
}
