#include <stdio.h>
#include <string.h>

#include "noxmqtt.h"
#include "noxmqtt_mqtt5.h"
#include "noxmqtt_tal.h"

extern void noxmqtt_transport_rcv_func(noxmqtt_client_t* c, uint8_t* data, uint16_t len);

static noxmqtt_transport_rcv_t g_recv_cb = NULL;
static noxmqtt_client_t* g_recv_client = NULL;
static uint8_t g_last_send[512];
static uint16_t g_last_send_len = 0;
static uint32_t g_fake_time_ms = 1000U;
static noxmqtt_evt_data_t g_last_event;
static uint32_t g_event_count = 0;

static void reset_test_state(void)
{
    memset(g_last_send, 0, sizeof(g_last_send));
    g_last_send_len = 0U;
    g_fake_time_ms = 1000U;
    memset(&g_last_event, 0, sizeof(g_last_event));
    g_event_count = 0U;
}

static void test_callback(noxmqtt_evt_data_t* evt_data)
{
    if (evt_data != NULL) {
        g_last_event = *evt_data;
        g_event_count++;
    }
}

static int assert_true(int condition, const char* message)
{
    if (!condition) {
        fprintf(stderr, "Assertion failed: %s\n", message);
        return 0;
    }

    return 1;
}

static int feed_packet(noxmqtt_client_t* client, uint8_t* packet, uint16_t packet_len)
{
    if (g_recv_cb == NULL || client == NULL) {
        fprintf(stderr, "Receive callback not initialized\n");
        return 0;
    }

    g_recv_cb(client, packet, packet_len);
    return 1;
}

static int perform_connect(noxmqtt_client_t* client)
{
    noxmqtt_client_conf_t conf;
    noxmqtt_rc_t rc = NOXMQTT_RC_ERROR;

    memset(&conf, 0, sizeof(conf));
    conf.callback = test_callback;
    conf.client_identifier = "test-client";
    conf.clean_session = 1U;
    conf.protocol_version = NOXMQTT_PROTOCOL_V5_0;
    conf.server.addr = "127.0.0.1";
    conf.server.port = 1883U;
    conf.mqtt5.auth_method = "SCRAM";

    rc = noxmqtt_connect(client, &conf, 30U);
    if (!assert_true(rc == NOXMQTT_SUCCESS, "noxmqtt_connect should succeed")) {
        return 0;
    }

    return 1;
}

static int test_incoming_reauth_uses_active_method(void)
{
    noxmqtt_client_t client;
    noxmqtt_rc_t rc = NOXMQTT_RC_ERROR;
    uint8_t connack[] = {
        0x20U, 0x0BU,
        0x00U, 0x00U,
        0x08U,
        MQTT5_PROPERTY_AUTH_METHOD, 0x00U, 0x05U, 'S', 'C', 'R', 'A', 'M'
    };
    uint8_t auth_reauth[] = {0xF0U, 0x02U, MQTT5_REASON_REAUTHENTICATE, 0x00U};
    uint8_t auth_success[] = {0xF0U, 0x02U, MQTT5_REASON_SUCCESS, 0x00U};

    reset_test_state();
    memset(&client, 0, sizeof(client));

    rc = noxmqtt_init(&client, NOXMQTT_DEBUG_LVL_NONE);
    if (!assert_true(rc == NOXMQTT_SUCCESS, "noxmqtt_init should succeed")) {
        return 0;
    }
    if (!perform_connect(&client)) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }
    if (!feed_packet(&client, connack, (uint16_t)sizeof(connack))) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }
    if (!assert_true(client.status.connected == 1U, "client should be connected after CONNACK")) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }
    if (!assert_true(client.status.auth_in_progress == 0U, "auth exchange should be idle after CONNACK")) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }

    if (!feed_packet(&client, auth_reauth, (uint16_t)sizeof(auth_reauth))) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }
    if (!assert_true(g_last_event.evt_id == NOXMQTT_EVT_AUTH, "AUTH event should be emitted")) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }
    if (!assert_true(client.status.auth_in_progress == 1U, "REAUTHENTICATE should mark auth as active")) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }
    if (!assert_true(g_last_event.evt.auth_evt.auth_method != NULL, "AUTH event should surface active method")) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }
    if (!assert_true(strncmp(g_last_event.evt.auth_evt.auth_method, "SCRAM", 5U) == 0,
                     "AUTH event should reuse established auth method")) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }

    if (!feed_packet(&client, auth_success, (uint16_t)sizeof(auth_success))) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }
    if (!assert_true(client.status.auth_in_progress == 0U, "AUTH success should end auth exchange")) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }
    if (!assert_true(g_last_event.evt.auth_evt.auth_method != NULL, "AUTH success should still expose active method")) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }

    (void)noxmqtt_deinit(&client);
    return 1;
}

static int test_outgoing_reauth_starts_auth_exchange(void)
{
    noxmqtt_client_t client;
    noxmqtt_rc_t rc = NOXMQTT_RC_ERROR;
    uint8_t connack[] = {
        0x20U, 0x0BU,
        0x00U, 0x00U,
        0x08U,
        MQTT5_PROPERTY_AUTH_METHOD, 0x00U, 0x05U, 'S', 'C', 'R', 'A', 'M'
    };
    noxmqtt_mqtt5_auth_props_t auth_props;

    reset_test_state();
    memset(&client, 0, sizeof(client));
    memset(&auth_props, 0, sizeof(auth_props));

    rc = noxmqtt_init(&client, NOXMQTT_DEBUG_LVL_NONE);
    if (!assert_true(rc == NOXMQTT_SUCCESS, "noxmqtt_init should succeed")) {
        return 0;
    }
    if (!perform_connect(&client)) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }
    if (!feed_packet(&client, connack, (uint16_t)sizeof(connack))) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }

    auth_props.reason_code = MQTT5_REASON_REAUTHENTICATE;
    rc = noxmqtt_auth(&client, &auth_props);
    if (!assert_true(rc == NOXMQTT_SUCCESS, "noxmqtt_auth reauthenticate should succeed")) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }
    if (!assert_true(client.status.auth_in_progress == 1U, "outgoing REAUTHENTICATE should start auth exchange")) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }
    if (!assert_true(g_last_send_len >= 4U, "AUTH packet should be sent")) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }
    if (!assert_true(g_last_send[0] == 0xF0U, "sent packet should be AUTH")) {
        (void)noxmqtt_deinit(&client);
        return 0;
    }

    (void)noxmqtt_deinit(&client);
    return 1;
}

static int test_duplicate_singleton_property_rejected(void)
{
    uint8_t props_buf[] = {
        0x04U,
        MQTT5_PROPERTY_PAYLOAD_FORMAT_INDICATOR, 0x00U,
        MQTT5_PROPERTY_PAYLOAD_FORMAT_INDICATOR, 0x01U
    };
    noxmqtt_mqtt5_property_view_t props;
    uint16_t offset = 0U;
    noxmqtt_rc_t rc = noxmqtt_mqtt5_parse_properties(props_buf,
                                                     (uint16_t)sizeof(props_buf),
                                                     &offset,
                                                     &props);

    return assert_true(rc == NOXMQTT_RC_ERROR_BAD_PACKET,
                       "duplicate singleton MQTT 5 property should be rejected");
}

static int test_multiple_subscription_identifiers_preserved(void)
{
    uint8_t props_buf[] = {
        0x04U,
        MQTT5_PROPERTY_SUBSCRIPTION_IDENTIFIER, 0x01U,
        MQTT5_PROPERTY_SUBSCRIPTION_IDENTIFIER, 0x02U
    };
    noxmqtt_mqtt5_property_view_t props;
    uint16_t offset = 0U;
    noxmqtt_rc_t rc = noxmqtt_mqtt5_parse_properties(props_buf,
                                                     (uint16_t)sizeof(props_buf),
                                                     &offset,
                                                     &props);

    if (!assert_true(rc == NOXMQTT_SUCCESS, "subscription identifier parse should succeed")) {
        return 0;
    }
    if (!assert_true(props.subscription_identifier_count == 2U, "all subscription identifiers should be preserved")) {
        return 0;
    }
    if (!assert_true(props.subscription_identifiers[0] == 1U && props.subscription_identifiers[1] == 2U,
                     "subscription identifiers should keep order")) {
        return 0;
    }

    return 1;
}

int noxmqtt_transport_init(noxmqtt_client_t* c, noxmqtt_transport_rcv_t rcv_cback)
{
    g_recv_client = c;
    g_recv_cb = rcv_cback;
    return 0;
}

int noxmqtt_transport_connect(noxmqtt_client_t* c, const noxmqtt_client_conf_t* conf)
{
    (void)c;
    (void)conf;
    return 0;
}

int noxmqtt_transport_send(noxmqtt_client_t* c, const uint8_t* data, uint16_t len)
{
    (void)c;

    if (len > sizeof(g_last_send)) {
        return -1;
    }

    memcpy(g_last_send, data, len);
    g_last_send_len = len;
    return 0;
}

int noxmqtt_transport_receive_thread(void* ptr)
{
    (void)ptr;
    return 0;
}

int noxmqtt_transport_disconnect(noxmqtt_client_t* c)
{
    (void)c;
    return 0;
}

void noxmqtt_transport_wait(void)
{
}

uint32_t noxmqtt_tal_time_ms(void)
{
    return g_fake_time_ms;
}

void noxmqtt_hal_debug_printf(const char* str)
{
    (void)str;
}

int main(void)
{
    int ok = 1;

    ok &= test_incoming_reauth_uses_active_method();
    ok &= test_outgoing_reauth_starts_auth_exchange();
    ok &= test_duplicate_singleton_property_rejected();
    ok &= test_multiple_subscription_identifiers_preserved();

    if (!ok) {
        return 1;
    }

    printf("noxmqtt_tests passed\n");
    return 0;
}
