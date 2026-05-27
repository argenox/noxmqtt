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
* File:    main.c
* Summary: Cross-platform noxmqtt command-line client
*
*****************************************************************************/

#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <process.h>
#include <windows.h>
#define NOXMQTT_STRICMP _stricmp
#else
#include <pthread.h>
#include <strings.h>
#include <unistd.h>
#define NOXMQTT_STRICMP strcasecmp
#endif

#include "noxmqtt.h"
#include "noxmqtt_tal.h"

#if defined(NOXMQTT_APP_HAS_NOXTLS) && (NOXMQTT_APP_HAS_NOXTLS != 0)
#include "noxtls_version.h"
#endif

#define NOXMQTT_APP_HOST_LEN     256
#define NOXMQTT_APP_TOPIC_LEN    256
#define NOXMQTT_APP_ID_LEN       256
#define NOXMQTT_APP_USER_LEN     256
#define NOXMQTT_APP_PASS_LEN     256
#define NOXMQTT_APP_PATH_LEN     512
#define NOXMQTT_APP_MESSAGE_LEN  1024
#define NOXMQTT_APP_INPUT_LEN    2048

typedef enum
{
    NOXMQTT_APP_ACTION_NONE = 0,
    NOXMQTT_APP_ACTION_PUBLISH,
    NOXMQTT_APP_ACTION_SUBSCRIBE,
    NOXMQTT_APP_ACTION_UNSUBSCRIBE,
    NOXMQTT_APP_ACTION_INTERACTIVE
} noxmqtt_app_action_t;

typedef struct
{
    noxmqtt_client_t client;
    noxmqtt_client_conf_t conf;
    noxmqtt_debug_lvl_t debug_level;
    noxmqtt_app_action_t action;
    volatile int running;
    volatile int connected;
    volatile int command_complete;
    volatile int action_started;
    int exit_code;
    uint16_t keepalive_seconds;
    uint32_t wait_timeout_ms;
    uint32_t expected_messages;
    uint32_t received_messages;
    noxmqtt_qos_t action_qos;
    uint8_t retain;
    char host[NOXMQTT_APP_HOST_LEN];
    char client_id[NOXMQTT_APP_ID_LEN];
    char username[NOXMQTT_APP_USER_LEN];
    char password[NOXMQTT_APP_PASS_LEN];
    char topic[NOXMQTT_APP_TOPIC_LEN];
    char message[NOXMQTT_APP_MESSAGE_LEN];
    char ca_file[NOXMQTT_APP_PATH_LEN];
    char cert_file[NOXMQTT_APP_PATH_LEN];
    char key_file[NOXMQTT_APP_PATH_LEN];
    char server_name[NOXMQTT_APP_HOST_LEN];
#ifdef _WIN32
    HANDLE worker_thread;
#else
    pthread_t worker_thread;
#endif
    int worker_started;
} noxmqtt_app_t;

static noxmqtt_app_t* g_app = NULL;

static void noxmqtt_app_usage(void);
static void noxmqtt_app_defaults(noxmqtt_app_t* app);
static int noxmqtt_app_parse_args(noxmqtt_app_t* app, int argc, char** argv);
static int noxmqtt_app_run_interactive(noxmqtt_app_t* app);
static int noxmqtt_app_run_command(noxmqtt_app_t* app);
static void noxmqtt_app_callback(noxmqtt_evt_data_t* data);
static int noxmqtt_app_start_worker(noxmqtt_app_t* app);
static void noxmqtt_app_stop_worker(noxmqtt_app_t* app);
static noxmqtt_rc_t noxmqtt_app_connect(noxmqtt_app_t* app);
static void noxmqtt_app_disconnect(noxmqtt_app_t* app);
static void noxmqtt_app_print_show(const noxmqtt_app_t* app);
static int noxmqtt_app_process_line(noxmqtt_app_t* app, char* line);
static char* noxmqtt_app_trim(char* str);
static char* noxmqtt_app_next_token(char** input);
static int noxmqtt_app_parse_u16(const char* str, uint16_t* out);
static int noxmqtt_app_parse_u32(const char* str, uint32_t* out);
static int noxmqtt_app_parse_u8(const char* str, uint8_t* out);
static int noxmqtt_app_parse_bool(const char* str, uint8_t* out);
static int noxmqtt_app_set_field(noxmqtt_app_t* app, const char* field, const char* value);
static void noxmqtt_app_assign_mutable_string(char* storage, size_t storage_len, char** field, const char* value);
static void noxmqtt_app_assign_const_string(char* storage, size_t storage_len, const char** field, const char* value);
static const char* noxmqtt_app_mode_name(noxmqtt_transport_mode_t mode);
static const char* noxmqtt_app_protocol_name(noxmqtt_protocol_ver_t version);
static const char* noxmqtt_app_noxtls_version(void);
static void noxmqtt_app_print_versions(void);
static void noxmqtt_app_request_stop(noxmqtt_app_t* app);
static void noxmqtt_app_print_message(const received_evt_t* evt);

#ifdef _WIN32
static unsigned __stdcall noxmqtt_app_worker_thread(void* arg);
static BOOL WINAPI noxmqtt_app_console_handler(DWORD type);
#else
static void* noxmqtt_app_worker_thread(void* arg);
static void noxmqtt_app_signal_handler(int signum);
#endif

/**
 * @brief Sleeps for a small number of milliseconds.
 *
 * @param[in] ms Number of milliseconds to sleep.
 */
static void noxmqtt_app_sleep_ms(uint32_t ms)
{
#ifdef _WIN32
    Sleep(ms);
#else
    usleep(ms * 1000U);
#endif
}

/**
 * @brief Requests the application to stop.
 *
 * @param[in] app Application state object.
 */
static void noxmqtt_app_request_stop(noxmqtt_app_t* app)
{
    if (app != NULL) {
        app->running = 0;
    }
}

#ifdef _WIN32
/**
 * @brief Handles console close and Ctrl+C events on Windows.
 *
 * @param[in] type Console control event type.
 *
 * @return `TRUE` when handled, otherwise `FALSE`.
 */
static BOOL WINAPI noxmqtt_app_console_handler(DWORD type)
{
    if (type == CTRL_C_EVENT || type == CTRL_BREAK_EVENT || type == CTRL_CLOSE_EVENT) {
        noxmqtt_app_request_stop(g_app);
        return TRUE;
    }

    return FALSE;
}
#else
/**
 * @brief Handles process signals on POSIX hosts.
 *
 * @param[in] signum Signal number.
 */
static void noxmqtt_app_signal_handler(int signum)
{
    (void)signum;
    noxmqtt_app_request_stop(g_app);
}
#endif

/**
 * @brief Prints command-line usage information.
 */
static void noxmqtt_app_usage(void)
{
    printf("noxmqtt - cross-platform MQTT client\n\n");
    noxmqtt_app_print_versions();
    printf("\n");
    printf("Usage:\n");
    printf("  noxmqtt pub [options] -t <topic> -m <message>\n");
    printf("  noxmqtt sub [options] -t <topic>\n");
    printf("  noxmqtt unsub [options] -t <topic>\n");
    printf("  noxmqtt interactive [options]\n");
    printf("  noxmqtt --interactive [options]\n\n");
    printf("General options:\n");
    printf("  -h, --host <host>                Broker host\n");
    printf("  -p, --port <port>                Broker port\n");
    printf("  -t, --topic <topic>              Topic for pub/sub/unsub\n");
    printf("  -m, --message <payload>          Publish payload\n");
    printf("  -q, --qos <0|1|2>                MQTT QoS\n");
    printf("  -k, --keepalive <seconds>        Keepalive interval\n");
    printf("  -i, --id <client-id>             MQTT client identifier\n");
    printf("  -u, --username <username>        Username\n");
    printf("  -P, --password <password>        Password\n");
    printf("  -V, --protocol-version <311|5>   MQTT protocol version\n");
    printf("  -d, --debug <none|error|warning|info|debug|all>\n");
    printf("      --retain                     Set publish retain flag\n");
    printf("      --count <n>                  Exit after n received messages when subscribing\n");
    printf("      --wait-ms <ms>               Stop after timeout in command mode\n");
    printf("      --clean-session <0|1>        Clean session flag\n");
    printf("      --disable-auto-reconnect <0|1>\n");
    printf("      --reconnect-ms <ms>          Reconnect delay\n");
    printf("      --timeout-ms <ms>            Network timeout\n");
    printf("      --interactive                Run interactive shell\n");
    printf("      --version                    Show build versions\n");
    printf("      --help                       Show this help text\n\n");
    printf("TLS options:\n");
    printf("      --tls                        Use secure MQTTS transport\n");
    printf("      --tcp                        Use plain TCP transport\n");
    printf("      --cafile <path>              CA certificate path\n");
    printf("      --cert <path>                Client certificate path\n");
    printf("      --key <path>                 Client private key path\n");
    printf("      --sni <name>                 TLS SNI / server name\n");
    printf("      --verify-peer <0|1>          Enable peer certificate validation\n");
    printf("      --verify-hostname <0|1>      Enable hostname validation\n");
    printf("      --insecure                   Disable peer and hostname verification\n\n");
    printf("Interactive commands:\n");
    printf("  help\n");
    printf("  version\n");
    printf("  show\n");
    printf("  set host|port|mode|proto|clientid|keepalive|clean|username|password\n");
    printf("  set reconnect_ms|timeout_ms|disable_auto_reconnect|verify_peer|verify_hostname\n");
    printf("  set server_name|ca|client_cert|client_key\n");
    printf("  connect\n");
    printf("  disconnect\n");
    printf("  pub <topic> <qos> <payload...>\n");
    printf("  sub <topic> [qos]\n");
    printf("  unsub <topic>\n");
    printf("  exit\n");
}

/**
 * @brief Copies a mutable string into application-owned storage.
 *
 * @param[in,out] storage Destination storage buffer.
 * @param[in] storage_len Destination storage size.
 * @param[out] field Configuration field to update.
 * @param[in] value New string value, or `NULL` to clear.
 */
static void noxmqtt_app_assign_mutable_string(char* storage, size_t storage_len, char** field, const char* value)
{
    if (storage == NULL || storage_len == 0U || field == NULL) {
        return;
    }

    storage[0] = '\0';
    *field = NULL;

    if (value == NULL || value[0] == '\0') {
        return;
    }

    strncpy(storage, value, storage_len - 1U);
    storage[storage_len - 1U] = '\0';
    *field = storage;
}

/**
 * @brief Copies a const string into application-owned storage.
 *
 * @param[in,out] storage Destination storage buffer.
 * @param[in] storage_len Destination storage size.
 * @param[out] field Configuration field to update.
 * @param[in] value New string value, or `NULL` to clear.
 */
static void noxmqtt_app_assign_const_string(char* storage, size_t storage_len, const char** field, const char* value)
{
    if (storage == NULL || storage_len == 0U || field == NULL) {
        return;
    }

    storage[0] = '\0';
    *field = NULL;

    if (value == NULL || value[0] == '\0') {
        return;
    }

    strncpy(storage, value, storage_len - 1U);
    storage[storage_len - 1U] = '\0';
    *field = storage;
}

/**
 * @brief Initializes default application settings.
 *
 * @param[in,out] app Application state object to initialize.
 */
static void noxmqtt_app_defaults(noxmqtt_app_t* app)
{
    if (app == NULL) {
        return;
    }

    memset(app, 0, sizeof(*app));
    app->action = NOXMQTT_APP_ACTION_NONE;
    app->debug_level = NOXMQTT_DEBUG_LVL_INFO;
    app->keepalive_seconds = 60U;
    app->wait_timeout_ms = 0U;
    app->expected_messages = 0U;
    app->action_qos = NOXMQTT_QOS0_AT_MOST_ONCE_DELIV;
    app->retain = 0U;
    app->exit_code = 0;
    app->running = 1;

    app->conf.server.mode = NOXMQTT_TRANSPORT_TCP;
    app->conf.server.port = 1883U;
    app->conf.server.reconnect_timeout_ms = NOXMQTT_DEFAULT_RECONNECT_TIMEOUT_MS;
    app->conf.server.network_timeout_ms = 1000U;
    app->conf.server.disable_auto_reconnect = 0U;
    app->conf.server.tls.verify_peer = 1U;
    app->conf.server.tls.verify_hostname = 1U;
    app->conf.clean_session = 1U;
    app->conf.protocol_version = NOXMQTT_PROTOCOL_V3_1_1;
    app->conf.callback = noxmqtt_app_callback;

    noxmqtt_app_assign_mutable_string(app->host, sizeof(app->host), &app->conf.server.addr, "test.mosquitto.org");
    noxmqtt_app_assign_const_string(app->client_id, sizeof(app->client_id), &app->conf.client_identifier, "noxmqtt");
    noxmqtt_app_assign_mutable_string(app->username, sizeof(app->username), &app->conf.auth.username, NULL);
    noxmqtt_app_assign_mutable_string(app->password, sizeof(app->password), &app->conf.auth.password, NULL);
    noxmqtt_app_assign_const_string(app->ca_file, sizeof(app->ca_file), &app->conf.server.tls.ca_cert, NULL);
    noxmqtt_app_assign_const_string(app->cert_file, sizeof(app->cert_file), &app->conf.server.tls.client_cert, NULL);
    noxmqtt_app_assign_const_string(app->key_file, sizeof(app->key_file), &app->conf.server.tls.client_key, NULL);
    noxmqtt_app_assign_const_string(app->server_name, sizeof(app->server_name), &app->conf.server.tls.server_name, NULL);
}

/**
 * @brief Parses a `uint16_t` value from text.
 *
 * @param[in] str Text to parse.
 * @param[out] out Parsed value.
 *
 * @return 1 on success, otherwise 0.
 */
static int noxmqtt_app_parse_u16(const char* str, uint16_t* out)
{
    char* end = NULL;
    unsigned long value = 0;

    if (str == NULL || out == NULL || str[0] == '\0') {
        return 0;
    }

    value = strtoul(str, &end, 10);
    if (*end != '\0' || value > 65535UL) {
        return 0;
    }

    *out = (uint16_t)value;
    return 1;
}

/**
 * @brief Parses a `uint32_t` value from text.
 *
 * @param[in] str Text to parse.
 * @param[out] out Parsed value.
 *
 * @return 1 on success, otherwise 0.
 */
static int noxmqtt_app_parse_u32(const char* str, uint32_t* out)
{
    char* end = NULL;
    unsigned long value = 0;

    if (str == NULL || out == NULL || str[0] == '\0') {
        return 0;
    }

    value = strtoul(str, &end, 10);
    if (*end != '\0') {
        return 0;
    }

    *out = (uint32_t)value;
    return 1;
}

/**
 * @brief Parses a `uint8_t` value from text.
 *
 * @param[in] str Text to parse.
 * @param[out] out Parsed value.
 *
 * @return 1 on success, otherwise 0.
 */
static int noxmqtt_app_parse_u8(const char* str, uint8_t* out)
{
    uint16_t value = 0U;

    if (!noxmqtt_app_parse_u16(str, &value) || value > 255U) {
        return 0;
    }

    *out = (uint8_t)value;
    return 1;
}

/**
 * @brief Parses a boolean-style value from text.
 *
 * @param[in] str Text to parse.
 * @param[out] out Parsed boolean value.
 *
 * @return 1 on success, otherwise 0.
 */
static int noxmqtt_app_parse_bool(const char* str, uint8_t* out)
{
    if (str == NULL || out == NULL) {
        return 0;
    }

    if ((strcmp(str, "1") == 0) || (NOXMQTT_STRICMP(str, "true") == 0) || (NOXMQTT_STRICMP(str, "on") == 0) || (NOXMQTT_STRICMP(str, "yes") == 0)) {
        *out = 1U;
        return 1;
    }

    if ((strcmp(str, "0") == 0) || (NOXMQTT_STRICMP(str, "false") == 0) || (NOXMQTT_STRICMP(str, "off") == 0) || (NOXMQTT_STRICMP(str, "no") == 0)) {
        *out = 0U;
        return 1;
    }

    return 0;
}

/**
 * @brief Converts a transport mode to a printable string.
 *
 * @param[in] mode Transport mode value.
 *
 * @return Constant mode string.
 */
static const char* noxmqtt_app_mode_name(noxmqtt_transport_mode_t mode)
{
    switch (mode) {
        case NOXMQTT_TRANSPORT_TCP:
            return "tcp";
        case NOXMQTT_TRANSPORT_TLS:
            return "tls";
        case NOXMQTT_TRANSPORT_WS:
            return "ws";
        case NOXMQTT_TRANSPORT_WSS:
            return "wss";
        default:
            return "unknown";
    }
}

/**
 * @brief Converts a protocol version to a printable string.
 *
 * @param[in] version MQTT protocol version value.
 *
 * @return Constant protocol string.
 */
static const char* noxmqtt_app_protocol_name(noxmqtt_protocol_ver_t version)
{
    switch (version) {
        case NOXMQTT_PROTOCOL_V3_1_1:
            return "3.1.1";
        case NOXMQTT_PROTOCOL_V5_0:
            return "5.0";
        default:
            return "unknown";
    }
}

/**
 * @brief Returns the NoxTLS version compiled into this CLI build.
 *
 * @return NoxTLS version string, or `"disabled"` when NoxTLS is not linked.
 */
static const char* noxmqtt_app_noxtls_version(void)
{
#if defined(NOXMQTT_APP_HAS_NOXTLS) && (NOXMQTT_APP_HAS_NOXTLS != 0)
    return NOXTLS_VERSION_STRING;
#else
    return "disabled";
#endif
}

/**
 * @brief Prints the NoxMQTT and compiled NoxTLS versions.
 */
static void noxmqtt_app_print_versions(void)
{
    printf("NoxMQTT %s\n", NOXMQTT_VERSION);
    printf("NoxTLS %s\n", noxmqtt_app_noxtls_version());
}

/**
 * @brief Prints the current application configuration.
 *
 * @param[in] app Application state object.
 */
static void noxmqtt_app_print_show(const noxmqtt_app_t* app)
{
    if (app == NULL) {
        return;
    }

    printf("noxmqtt_version=%s\n", NOXMQTT_VERSION);
    printf("noxtls_version=%s\n", noxmqtt_app_noxtls_version());
    printf("connected=%d\n", app->connected);
    printf("action=%d\n", (int)app->action);
    printf("host=%s\n", (app->conf.server.addr != NULL) ? app->conf.server.addr : "");
    printf("port=%u\n", app->conf.server.port);
    printf("mode=%s\n", noxmqtt_app_mode_name(app->conf.server.mode));
    printf("protocol=%s\n", noxmqtt_app_protocol_name(app->conf.protocol_version));
    printf("clientid=%s\n", (app->conf.client_identifier != NULL) ? app->conf.client_identifier : "");
    printf("keepalive=%u\n", app->keepalive_seconds);
    printf("clean_session=%u\n", app->conf.clean_session);
    printf("username=%s\n", (app->conf.auth.username != NULL) ? app->conf.auth.username : "");
    printf("password=%s\n", (app->conf.auth.password != NULL && app->conf.auth.password[0] != '\0') ? "<set>" : "");
    printf("topic=%s\n", app->topic);
    printf("qos=%u\n", (unsigned int)app->action_qos);
    printf("retain=%u\n", app->retain);
    printf("count=%lu\n", (unsigned long)app->expected_messages);
    printf("wait_ms=%lu\n", (unsigned long)app->wait_timeout_ms);
    printf("reconnect_ms=%lu\n", (unsigned long)app->conf.server.reconnect_timeout_ms);
    printf("timeout_ms=%lu\n", (unsigned long)app->conf.server.network_timeout_ms);
    printf("disable_auto_reconnect=%u\n", app->conf.server.disable_auto_reconnect);
    printf("verify_peer=%u\n", app->conf.server.tls.verify_peer);
    printf("verify_hostname=%u\n", app->conf.server.tls.verify_hostname);
    printf("server_name=%s\n", (app->conf.server.tls.server_name != NULL) ? app->conf.server.tls.server_name : "");
    printf("ca=%s\n", (app->conf.server.tls.ca_cert != NULL) ? app->conf.server.tls.ca_cert : "");
    printf("client_cert=%s\n", (app->conf.server.tls.client_cert != NULL) ? app->conf.server.tls.client_cert : "");
    printf("client_key=%s\n", (app->conf.server.tls.client_key != NULL) ? app->conf.server.tls.client_key : "");
}

/**
 * @brief Applies one mutable configuration field from a command or option.
 *
 * @param[in,out] app Application state object.
 * @param[in] field Field name.
 * @param[in] value Field value string.
 *
 * @return 0 on success, otherwise `-1`.
 */
static int noxmqtt_app_set_field(noxmqtt_app_t* app, const char* field, const char* value)
{
    uint16_t u16_value = 0U;
    uint32_t u32_value = 0U;
    uint8_t u8_value = 0U;

    if (app == NULL || field == NULL || value == NULL) {
        return -1;
    }

    if (NOXMQTT_STRICMP(field, "host") == 0 || NOXMQTT_STRICMP(field, "addr") == 0) {
        noxmqtt_app_assign_mutable_string(app->host, sizeof(app->host), &app->conf.server.addr, value);
    } else if (NOXMQTT_STRICMP(field, "port") == 0) {
        if (!noxmqtt_app_parse_u16(value, &u16_value)) {
            return -1;
        }
        app->conf.server.port = u16_value;
    } else if (NOXMQTT_STRICMP(field, "mode") == 0) {
        if (NOXMQTT_STRICMP(value, "tcp") == 0) {
            app->conf.server.mode = NOXMQTT_TRANSPORT_TCP;
        } else if (NOXMQTT_STRICMP(value, "tls") == 0) {
            app->conf.server.mode = NOXMQTT_TRANSPORT_TLS;
        } else {
            return -1;
        }
    } else if (NOXMQTT_STRICMP(field, "proto") == 0 || NOXMQTT_STRICMP(field, "protocol") == 0) {
        if ((strcmp(value, "311") == 0) || (NOXMQTT_STRICMP(value, "3.1.1") == 0)) {
            app->conf.protocol_version = NOXMQTT_PROTOCOL_V3_1_1;
        } else if ((strcmp(value, "5") == 0) || (NOXMQTT_STRICMP(value, "5.0") == 0)) {
            app->conf.protocol_version = NOXMQTT_PROTOCOL_V5_0;
        } else {
            return -1;
        }
    } else if (NOXMQTT_STRICMP(field, "clientid") == 0 || NOXMQTT_STRICMP(field, "client_id") == 0) {
        noxmqtt_app_assign_const_string(app->client_id, sizeof(app->client_id), &app->conf.client_identifier, value);
    } else if (NOXMQTT_STRICMP(field, "keepalive") == 0) {
        if (!noxmqtt_app_parse_u16(value, &u16_value)) {
            return -1;
        }
        app->keepalive_seconds = u16_value;
    } else if (NOXMQTT_STRICMP(field, "clean") == 0 || NOXMQTT_STRICMP(field, "clean_session") == 0) {
        if (!noxmqtt_app_parse_bool(value, &u8_value)) {
            return -1;
        }
        app->conf.clean_session = u8_value;
    } else if (NOXMQTT_STRICMP(field, "username") == 0) {
        noxmqtt_app_assign_mutable_string(app->username, sizeof(app->username), &app->conf.auth.username, value);
    } else if (NOXMQTT_STRICMP(field, "password") == 0) {
        noxmqtt_app_assign_mutable_string(app->password, sizeof(app->password), &app->conf.auth.password, value);
    } else if (NOXMQTT_STRICMP(field, "reconnect_ms") == 0) {
        if (!noxmqtt_app_parse_u32(value, &u32_value)) {
            return -1;
        }
        app->conf.server.reconnect_timeout_ms = u32_value;
    } else if (NOXMQTT_STRICMP(field, "timeout_ms") == 0) {
        if (!noxmqtt_app_parse_u32(value, &u32_value)) {
            return -1;
        }
        app->conf.server.network_timeout_ms = u32_value;
    } else if (NOXMQTT_STRICMP(field, "disable_auto_reconnect") == 0) {
        if (!noxmqtt_app_parse_bool(value, &u8_value)) {
            return -1;
        }
        app->conf.server.disable_auto_reconnect = u8_value;
    } else if (NOXMQTT_STRICMP(field, "verify_peer") == 0) {
        if (!noxmqtt_app_parse_bool(value, &u8_value)) {
            return -1;
        }
        app->conf.server.tls.verify_peer = u8_value;
    } else if (NOXMQTT_STRICMP(field, "verify_hostname") == 0) {
        if (!noxmqtt_app_parse_bool(value, &u8_value)) {
            return -1;
        }
        app->conf.server.tls.verify_hostname = u8_value;
    } else if (NOXMQTT_STRICMP(field, "server_name") == 0 || NOXMQTT_STRICMP(field, "sni") == 0) {
        noxmqtt_app_assign_const_string(app->server_name, sizeof(app->server_name), &app->conf.server.tls.server_name, value);
    } else if (NOXMQTT_STRICMP(field, "ca") == 0 || NOXMQTT_STRICMP(field, "cafile") == 0) {
        noxmqtt_app_assign_const_string(app->ca_file, sizeof(app->ca_file), &app->conf.server.tls.ca_cert, value);
    } else if (NOXMQTT_STRICMP(field, "client_cert") == 0 || NOXMQTT_STRICMP(field, "cert") == 0) {
        noxmqtt_app_assign_const_string(app->cert_file, sizeof(app->cert_file), &app->conf.server.tls.client_cert, value);
    } else if (NOXMQTT_STRICMP(field, "client_key") == 0 || NOXMQTT_STRICMP(field, "key") == 0) {
        noxmqtt_app_assign_const_string(app->key_file, sizeof(app->key_file), &app->conf.server.tls.client_key, value);
    } else {
        return -1;
    }

    return 0;
}

/**
 * @brief Trims leading and trailing whitespace from a string.
 *
 * @param[in] str String to trim.
 *
 * @return Pointer to the trimmed string.
 */
static char* noxmqtt_app_trim(char* str)
{
    char* end = NULL;

    if (str == NULL) {
        return NULL;
    }

    while (*str == ' ' || *str == '\t' || *str == '\r' || *str == '\n') {
        str++;
    }

    end = str + strlen(str);
    while (end > str) {
        char ch = *(end - 1);
        if (ch != ' ' && ch != '\t' && ch != '\r' && ch != '\n') {
            break;
        }
        end--;
    }

    *end = '\0';
    return str;
}

/**
 * @brief Extracts the next whitespace-delimited token from a string.
 *
 * @param[in,out] input Pointer to the parse cursor.
 *
 * @return Pointer to the next token, or `NULL` when none remains.
 */
static char* noxmqtt_app_next_token(char** input)
{
    char* token = NULL;
    char* cursor = NULL;

    if (input == NULL || *input == NULL) {
        return NULL;
    }

    cursor = noxmqtt_app_trim(*input);
    if (*cursor == '\0') {
        *input = cursor;
        return NULL;
    }

    token = cursor;
    while (*cursor != '\0' && *cursor != ' ' && *cursor != '\t') {
        cursor++;
    }

    if (*cursor != '\0') {
        *cursor = '\0';
        cursor++;
    }

    *input = cursor;
    return token;
}

/**
 * @brief Connects using the current application configuration.
 *
 * @param[in,out] app Application state object.
 *
 * @return MQTT result code from `noxmqtt_connect`.
 */
static noxmqtt_rc_t noxmqtt_app_connect(noxmqtt_app_t* app)
{
    noxmqtt_rc_t rc = NOXMQTT_RC_ERROR;

    if (app == NULL) {
        return rc;
    }

    app->command_complete = 0;
    app->action_started = 0;
    app->connected = 0;
    rc = noxmqtt_connect(&app->client, &app->conf, app->keepalive_seconds);
    if (rc != NOXMQTT_SUCCESS) {
        app->exit_code = 1;
        fprintf(stderr, "connect failed: rc=%d\n", (int)rc);
    }

    return rc;
}

/**
 * @brief Disconnects the current MQTT session.
 *
 * @param[in,out] app Application state object.
 */
static void noxmqtt_app_disconnect(noxmqtt_app_t* app)
{
    if (app == NULL) {
        return;
    }

    (void)noxmqtt_disconnect(&app->client);
    app->connected = 0;
}

/**
 * @brief Starts the background worker thread.
 *
 * @param[in,out] app Application state object.
 *
 * @return 0 on success, otherwise `-1`.
 */
static int noxmqtt_app_start_worker(noxmqtt_app_t* app)
{
    if (app == NULL) {
        return -1;
    }

    if (app->worker_started) {
        return 0;
    }

#ifdef _WIN32
    app->worker_thread = (HANDLE)_beginthreadex(NULL, 0, noxmqtt_app_worker_thread, app, 0, NULL);
    if (app->worker_thread == NULL) {
        return -1;
    }
#else
    if (pthread_create(&app->worker_thread, NULL, noxmqtt_app_worker_thread, app) != 0) {
        return -1;
    }
#endif

    app->worker_started = 1;
    return 0;
}

/**
 * @brief Stops the background worker thread.
 *
 * @param[in,out] app Application state object.
 */
static void noxmqtt_app_stop_worker(noxmqtt_app_t* app)
{
    if (app == NULL || !app->worker_started) {
        return;
    }

    app->running = 0;

#ifdef _WIN32
    WaitForSingleObject(app->worker_thread, INFINITE);
    CloseHandle(app->worker_thread);
    app->worker_thread = NULL;
#else
    (void)pthread_join(app->worker_thread, NULL);
#endif

    app->worker_started = 0;
}

#ifdef _WIN32
/**
 * @brief Runs periodic MQTT processing in the background on Windows.
 *
 * @param[in] arg Application state pointer.
 *
 * @return Thread exit code.
 */
static unsigned __stdcall noxmqtt_app_worker_thread(void* arg)
{
    noxmqtt_app_t* app = (noxmqtt_app_t*)arg;

    while (app != NULL && app->running) {
        (void)noxmqtt_process(&app->client);
        noxmqtt_app_sleep_ms(100U);
    }

    return 0U;
}
#else
/**
 * @brief Runs periodic MQTT processing in the background on POSIX hosts.
 *
 * @param[in] arg Application state pointer.
 *
 * @return `NULL`.
 */
static void* noxmqtt_app_worker_thread(void* arg)
{
    noxmqtt_app_t* app = (noxmqtt_app_t*)arg;

    while (app != NULL && app->running) {
        (void)noxmqtt_process(&app->client);
        noxmqtt_app_sleep_ms(100U);
    }

    return NULL;
}
#endif

/**
 * @brief Prints a received publish message.
 *
 * @param[in] evt Received event payload.
 */
static void noxmqtt_app_print_message(const received_evt_t* evt)
{
    uint16_t topic_len = 0U;
    uint16_t payload_len = 0U;

    if (evt == NULL) {
        return;
    }

    topic_len = evt->topic_len;
    payload_len = evt->payload_len;

    printf("message topic=");
    if (evt->topic != NULL && topic_len > 0U) {
        fwrite(evt->topic, 1U, topic_len, stdout);
    }
    printf(" payload=");
    if (evt->payload != NULL && payload_len > 0U) {
        fwrite(evt->payload, 1U, payload_len, stdout);
    }
    printf("\n");
}

/**
 * @brief Handles MQTT events for the CLI application.
 *
 * @param[in] data Event data provided by the client library.
 */
static void noxmqtt_app_callback(noxmqtt_evt_data_t* data)
{
    noxmqtt_topic_sub_t topic_sub = { 0 };
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;
    noxmqtt_app_t* app = g_app;

    if (app == NULL || data == NULL) {
        return;
    }

    switch (data->evt_id) {
        case NOXMQTT_EVT_CONNECT:
            app->connected = 1;
            printf("connected");
            if (app->conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
                printf(" reason=%u session_present=%u", data->evt.connect_evt.reason_code, data->evt.connect_evt.session_present);
            }
            printf("\n");

            if (app->action == NOXMQTT_APP_ACTION_PUBLISH && !app->action_started) {
                app->action_started = 1;
                rc = noxmqtt_publish(&app->client, app->action_qos, app->retain, 0U, app->topic, app->message);
                if (rc != NOXMQTT_SUCCESS) {
                    fprintf(stderr, "publish failed: rc=%d\n", (int)rc);
                    app->exit_code = 1;
                    app->command_complete = 1;
                    noxmqtt_app_request_stop(app);
                } else if (app->action_qos == NOXMQTT_QOS0_AT_MOST_ONCE_DELIV) {
                    app->command_complete = 1;
                    noxmqtt_app_request_stop(app);
                }
            } else if (app->action == NOXMQTT_APP_ACTION_SUBSCRIBE && !app->action_started) {
                app->action_started = 1;
                topic_sub.topic = app->topic;
                topic_sub.qos = app->action_qos;
                rc = noxmqtt_subscribe(&app->client, &topic_sub, 1U);
                if (rc != NOXMQTT_SUCCESS) {
                    fprintf(stderr, "subscribe failed: rc=%d\n", (int)rc);
                    app->exit_code = 1;
                    app->command_complete = 1;
                    noxmqtt_app_request_stop(app);
                }
            } else if (app->action == NOXMQTT_APP_ACTION_UNSUBSCRIBE && !app->action_started) {
                app->action_started = 1;
                topic_sub.topic = app->topic;
                topic_sub.qos = NOXMQTT_QOS0_AT_MOST_ONCE_DELIV;
                rc = noxmqtt_unsubscribe(&app->client, &topic_sub, 1U);
                if (rc != NOXMQTT_SUCCESS) {
                    fprintf(stderr, "unsubscribe failed: rc=%d\n", (int)rc);
                    app->exit_code = 1;
                    app->command_complete = 1;
                    noxmqtt_app_request_stop(app);
                }
            }
            break;

        case NOXMQTT_EVT_CONNECT_ERROR:
            fprintf(stderr, "connect_error=%u", data->evt.conn_err_evt.reason);
            if (app->conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
                fprintf(stderr, " raw=%u", data->evt.conn_err_evt.raw_reason_code);
            }
            fprintf(stderr, "\n");
            app->connected = 0;
            if (app->action != NOXMQTT_APP_ACTION_INTERACTIVE) {
                app->exit_code = 1;
                app->command_complete = 1;
                noxmqtt_app_request_stop(app);
            }
            break;

        case NOXMQTT_EVT_PUBLISHED:
            printf("published packet_id=%u\n", data->evt.published_evt.packet_identifier);
            if (app->action == NOXMQTT_APP_ACTION_PUBLISH) {
                app->command_complete = 1;
                noxmqtt_app_request_stop(app);
            }
            break;

        case NOXMQTT_EVT_RECEIVED:
            app->received_messages++;
            noxmqtt_app_print_message(&data->evt.received_evt);
            if (app->action == NOXMQTT_APP_ACTION_SUBSCRIBE &&
                app->expected_messages > 0U &&
                app->received_messages >= app->expected_messages) {
                app->command_complete = 1;
                noxmqtt_app_request_stop(app);
            }
            break;

        case NOXMQTT_EVT_SUBSCRIBED:
            printf("subscribed packet_id=%u\n", data->evt.subscribed_evt.packet_identifier);
            if (app->action == NOXMQTT_APP_ACTION_SUBSCRIBE &&
                app->expected_messages == 0U &&
                app->wait_timeout_ms == 0U &&
                app->action != NOXMQTT_APP_ACTION_INTERACTIVE) {
                printf("subscription active; press Ctrl+C to stop\n");
            }
            break;

        case NOXMQTT_EVT_UNSUBSCRIBED:
            printf("unsubscribed packet_id=%u\n", data->evt.unsubscribed_evt.packet_identifier);
            if (app->action == NOXMQTT_APP_ACTION_UNSUBSCRIBE) {
                app->command_complete = 1;
                noxmqtt_app_request_stop(app);
            }
            break;

        case NOXMQTT_EVT_DISCONNECT:
            printf("disconnected");
            if (app->conf.protocol_version == NOXMQTT_PROTOCOL_V5_0) {
                printf(" reason=%u", data->evt.disconnect_evt.reason_code);
            }
            printf("\n");
            app->connected = 0;
            break;

        case NOXMQTT_EVT_AUTH:
            printf("auth reason=%u\n", data->evt.auth_evt.reason_code);
            break;

        case NOXMQTT_EVT_ERROR:
            fprintf(stderr, "mqtt_error rc=%u\n", data->evt.error_evt.rc);
            if (app->action != NOXMQTT_APP_ACTION_INTERACTIVE) {
                app->exit_code = 1;
            }
            break;

        case NOXMQTT_EVT_PINGRESP:
        case NOXMQTT_EVT_PUBREL:
        default:
            break;
    }
}

/**
 * @brief Parses top-level CLI arguments.
 *
 * @param[in,out] app Application state object.
 * @param[in] argc Argument count.
 * @param[in] argv Argument vector.
 *
 * @return 0 on success, 1 when help/usage was already handled, otherwise `-1`.
 */
static int noxmqtt_app_parse_args(noxmqtt_app_t* app, int argc, char** argv)
{
    int i = 1;

    if (app == NULL) {
        return -1;
    }

    if (argc <= 1) {
        noxmqtt_app_usage();
        app->exit_code = 0;
        return 1;
    }

    if (argv[i][0] != '-') {
        if (NOXMQTT_STRICMP(argv[i], "pub") == 0) {
            app->action = NOXMQTT_APP_ACTION_PUBLISH;
            i++;
        } else if (NOXMQTT_STRICMP(argv[i], "sub") == 0) {
            app->action = NOXMQTT_APP_ACTION_SUBSCRIBE;
            i++;
        } else if (NOXMQTT_STRICMP(argv[i], "unsub") == 0) {
            app->action = NOXMQTT_APP_ACTION_UNSUBSCRIBE;
            i++;
        } else if (NOXMQTT_STRICMP(argv[i], "interactive") == 0) {
            app->action = NOXMQTT_APP_ACTION_INTERACTIVE;
            i++;
        } else if (NOXMQTT_STRICMP(argv[i], "help") == 0) {
            noxmqtt_app_usage();
            app->exit_code = 0;
            return 1;
        }
    }

    for (; i < argc; i++) {
        const char* arg = argv[i];

        if (strcmp(arg, "--help") == 0) {
            noxmqtt_app_usage();
            app->exit_code = 0;
            return 1;
        } else if (strcmp(arg, "--version") == 0) {
            noxmqtt_app_print_versions();
            app->exit_code = 0;
            return 1;
        } else if ((strcmp(arg, "-h") == 0) || (strcmp(arg, "--host") == 0)) {
            if (++i >= argc) {
                return -1;
            }
            noxmqtt_app_assign_mutable_string(app->host, sizeof(app->host), &app->conf.server.addr, argv[i]);
        } else if ((strcmp(arg, "-p") == 0) || (strcmp(arg, "--port") == 0)) {
            if (++i >= argc || !noxmqtt_app_parse_u16(argv[i], &app->conf.server.port)) {
                return -1;
            }
        } else if ((strcmp(arg, "-t") == 0) || (strcmp(arg, "--topic") == 0)) {
            if (++i >= argc) {
                return -1;
            }
            strncpy(app->topic, argv[i], sizeof(app->topic) - 1U);
            app->topic[sizeof(app->topic) - 1U] = '\0';
        } else if ((strcmp(arg, "-m") == 0) || (strcmp(arg, "--message") == 0)) {
            if (++i >= argc) {
                return -1;
            }
            strncpy(app->message, argv[i], sizeof(app->message) - 1U);
            app->message[sizeof(app->message) - 1U] = '\0';
        } else if ((strcmp(arg, "-q") == 0) || (strcmp(arg, "--qos") == 0)) {
            uint8_t qos = 0U;
            if (++i >= argc || !noxmqtt_app_parse_u8(argv[i], &qos) || qos > 2U) {
                return -1;
            }
            app->action_qos = (noxmqtt_qos_t)qos;
        } else if ((strcmp(arg, "-k") == 0) || (strcmp(arg, "--keepalive") == 0)) {
            if (++i >= argc || !noxmqtt_app_parse_u16(argv[i], &app->keepalive_seconds)) {
                return -1;
            }
        } else if ((strcmp(arg, "-i") == 0) || (strcmp(arg, "--id") == 0)) {
            if (++i >= argc) {
                return -1;
            }
            noxmqtt_app_assign_const_string(app->client_id, sizeof(app->client_id), &app->conf.client_identifier, argv[i]);
        } else if ((strcmp(arg, "-u") == 0) || (strcmp(arg, "--username") == 0)) {
            if (++i >= argc) {
                return -1;
            }
            noxmqtt_app_assign_mutable_string(app->username, sizeof(app->username), &app->conf.auth.username, argv[i]);
        } else if ((strcmp(arg, "-P") == 0) || (strcmp(arg, "--password") == 0)) {
            if (++i >= argc) {
                return -1;
            }
            noxmqtt_app_assign_mutable_string(app->password, sizeof(app->password), &app->conf.auth.password, argv[i]);
        } else if ((strcmp(arg, "-V") == 0) || (strcmp(arg, "--protocol-version") == 0)) {
            if (++i >= argc || noxmqtt_app_set_field(app, "proto", argv[i]) != 0) {
                return -1;
            }
        } else if ((strcmp(arg, "-d") == 0) || (strcmp(arg, "--debug") == 0)) {
            if (++i >= argc) {
                return -1;
            }

            if (NOXMQTT_STRICMP(argv[i], "none") == 0) {
                app->debug_level = NOXMQTT_DEBUG_LVL_NONE;
            } else if (NOXMQTT_STRICMP(argv[i], "error") == 0) {
                app->debug_level = NOXMQTT_DEBUG_LVL_ERROR;
            } else if (NOXMQTT_STRICMP(argv[i], "warning") == 0) {
                app->debug_level = NOXMQTT_DEBUG_LVL_WARNING;
            } else if (NOXMQTT_STRICMP(argv[i], "info") == 0) {
                app->debug_level = NOXMQTT_DEBUG_LVL_INFO;
            } else if (NOXMQTT_STRICMP(argv[i], "debug") == 0) {
                app->debug_level = NOXMQTT_DEBUG_LVL_DEBUG;
            } else if (NOXMQTT_STRICMP(argv[i], "all") == 0) {
                app->debug_level = NOXMQTT_DEBUG_LVL_ALL;
            } else {
                return -1;
            }
        } else if (strcmp(arg, "--retain") == 0) {
            app->retain = 1U;
        } else if (strcmp(arg, "--tls") == 0) {
            app->conf.server.mode = NOXMQTT_TRANSPORT_TLS;
            if (app->conf.server.port == 1883U) {
                app->conf.server.port = 8883U;
            }
        } else if (strcmp(arg, "--tcp") == 0) {
            app->conf.server.mode = NOXMQTT_TRANSPORT_TCP;
        } else if ((strcmp(arg, "--cafile") == 0) || (strcmp(arg, "--ca") == 0)) {
            if (++i >= argc) {
                return -1;
            }
            noxmqtt_app_assign_const_string(app->ca_file, sizeof(app->ca_file), &app->conf.server.tls.ca_cert, argv[i]);
        } else if (strcmp(arg, "--cert") == 0) {
            if (++i >= argc) {
                return -1;
            }
            noxmqtt_app_assign_const_string(app->cert_file, sizeof(app->cert_file), &app->conf.server.tls.client_cert, argv[i]);
        } else if (strcmp(arg, "--key") == 0) {
            if (++i >= argc) {
                return -1;
            }
            noxmqtt_app_assign_const_string(app->key_file, sizeof(app->key_file), &app->conf.server.tls.client_key, argv[i]);
        } else if ((strcmp(arg, "--sni") == 0) || (strcmp(arg, "--server-name") == 0)) {
            if (++i >= argc) {
                return -1;
            }
            noxmqtt_app_assign_const_string(app->server_name, sizeof(app->server_name), &app->conf.server.tls.server_name, argv[i]);
        } else if (strcmp(arg, "--verify-peer") == 0) {
            if (++i >= argc || !noxmqtt_app_parse_bool(argv[i], &app->conf.server.tls.verify_peer)) {
                return -1;
            }
        } else if (strcmp(arg, "--verify-hostname") == 0) {
            if (++i >= argc || !noxmqtt_app_parse_bool(argv[i], &app->conf.server.tls.verify_hostname)) {
                return -1;
            }
        } else if (strcmp(arg, "--insecure") == 0) {
            app->conf.server.tls.verify_peer = 0U;
            app->conf.server.tls.verify_hostname = 0U;
        } else if (strcmp(arg, "--count") == 0) {
            if (++i >= argc || !noxmqtt_app_parse_u32(argv[i], &app->expected_messages)) {
                return -1;
            }
        } else if (strcmp(arg, "--wait-ms") == 0) {
            if (++i >= argc || !noxmqtt_app_parse_u32(argv[i], &app->wait_timeout_ms)) {
                return -1;
            }
        } else if (strcmp(arg, "--clean-session") == 0) {
            if (++i >= argc || !noxmqtt_app_parse_bool(argv[i], &app->conf.clean_session)) {
                return -1;
            }
        } else if (strcmp(arg, "--disable-auto-reconnect") == 0) {
            if (++i >= argc || !noxmqtt_app_parse_bool(argv[i], &app->conf.server.disable_auto_reconnect)) {
                return -1;
            }
        } else if (strcmp(arg, "--reconnect-ms") == 0) {
            if (++i >= argc || !noxmqtt_app_parse_u32(argv[i], &app->conf.server.reconnect_timeout_ms)) {
                return -1;
            }
        } else if (strcmp(arg, "--timeout-ms") == 0) {
            if (++i >= argc || !noxmqtt_app_parse_u32(argv[i], &app->conf.server.network_timeout_ms)) {
                return -1;
            }
        } else if (strcmp(arg, "--interactive") == 0) {
            app->action = NOXMQTT_APP_ACTION_INTERACTIVE;
        } else {
            fprintf(stderr, "Unknown argument: %s\n", arg);
            return -1;
        }
    }

    if (app->action == NOXMQTT_APP_ACTION_NONE) {
        fprintf(stderr, "Missing command. Use pub, sub, unsub, or interactive.\n");
        return -1;
    }

    if ((app->action == NOXMQTT_APP_ACTION_PUBLISH ||
         app->action == NOXMQTT_APP_ACTION_SUBSCRIBE ||
         app->action == NOXMQTT_APP_ACTION_UNSUBSCRIBE) && app->topic[0] == '\0') {
        fprintf(stderr, "A topic is required for this command.\n");
        return -1;
    }

    if (app->action == NOXMQTT_APP_ACTION_PUBLISH && app->message[0] == '\0') {
        fprintf(stderr, "A message payload is required for publish.\n");
        return -1;
    }

    return 0;
}

/**
 * @brief Executes a single interactive command line.
 *
 * @param[in,out] app Application state object.
 * @param[in,out] line Mutable command line buffer.
 *
 * @return 0 to continue, 1 to exit the interactive shell.
 */
static int noxmqtt_app_process_line(noxmqtt_app_t* app, char* line)
{
    char* args = NULL;
    char* cmd = NULL;
    char* token = NULL;
    noxmqtt_topic_sub_t topic_sub = { 0 };
    noxmqtt_rc_t rc = NOXMQTT_SUCCESS;
    uint8_t qos = 0U;

    if (app == NULL || line == NULL) {
        return 0;
    }

    args = line;
    cmd = noxmqtt_app_next_token(&args);
    if (cmd == NULL) {
        return 0;
    }

    if (NOXMQTT_STRICMP(cmd, "help") == 0) {
        noxmqtt_app_usage();
        return 0;
    }

    if (NOXMQTT_STRICMP(cmd, "version") == 0) {
        noxmqtt_app_print_versions();
        return 0;
    }

    if (NOXMQTT_STRICMP(cmd, "show") == 0 || NOXMQTT_STRICMP(cmd, "status") == 0) {
        noxmqtt_app_print_show(app);
        return 0;
    }

    if (NOXMQTT_STRICMP(cmd, "set") == 0) {
        char* field = noxmqtt_app_next_token(&args);
        char* value = noxmqtt_app_trim(args);
        if (field == NULL || value == NULL || value[0] == '\0' || noxmqtt_app_set_field(app, field, value) != 0) {
            fprintf(stderr, "Usage: set <field> <value>\n");
        } else {
            noxmqtt_app_print_show(app);
        }
        return 0;
    }

    if (NOXMQTT_STRICMP(cmd, "connect") == 0) {
        (void)noxmqtt_app_connect(app);
        return 0;
    }

    if (NOXMQTT_STRICMP(cmd, "disconnect") == 0) {
        noxmqtt_app_disconnect(app);
        return 0;
    }

    if (NOXMQTT_STRICMP(cmd, "pub") == 0) {
        char* topic = noxmqtt_app_next_token(&args);
        char* qos_str = noxmqtt_app_next_token(&args);
        char* payload = noxmqtt_app_trim(args);

        if (topic == NULL || qos_str == NULL || payload == NULL || payload[0] == '\0' ||
            !noxmqtt_app_parse_u8(qos_str, &qos) || qos > 2U) {
            fprintf(stderr, "Usage: pub <topic> <qos> <payload...>\n");
            return 0;
        }

        rc = noxmqtt_publish(&app->client, (noxmqtt_qos_t)qos, app->retain, 0U, topic, payload);
        printf("publish rc=%d\n", (int)rc);
        return 0;
    }

    if (NOXMQTT_STRICMP(cmd, "sub") == 0) {
        char* topic = noxmqtt_app_next_token(&args);
        char* qos_str = noxmqtt_app_next_token(&args);

        if (topic == NULL) {
            fprintf(stderr, "Usage: sub <topic> [qos]\n");
            return 0;
        }

        if (qos_str != NULL && (!noxmqtt_app_parse_u8(qos_str, &qos) || qos > 2U)) {
            fprintf(stderr, "Invalid qos\n");
            return 0;
        }

        topic_sub.topic = topic;
        topic_sub.qos = (noxmqtt_qos_t)qos;
        rc = noxmqtt_subscribe(&app->client, &topic_sub, 1U);
        printf("subscribe rc=%d\n", (int)rc);
        return 0;
    }

    if (NOXMQTT_STRICMP(cmd, "unsub") == 0) {
        token = noxmqtt_app_next_token(&args);
        if (token == NULL) {
            fprintf(stderr, "Usage: unsub <topic>\n");
            return 0;
        }

        topic_sub.topic = token;
        topic_sub.qos = NOXMQTT_QOS0_AT_MOST_ONCE_DELIV;
        rc = noxmqtt_unsubscribe(&app->client, &topic_sub, 1U);
        printf("unsubscribe rc=%d\n", (int)rc);
        return 0;
    }

    if (NOXMQTT_STRICMP(cmd, "exit") == 0 || NOXMQTT_STRICMP(cmd, "quit") == 0) {
        noxmqtt_app_request_stop(app);
        return 1;
    }

    fprintf(stderr, "Unknown command: %s\n", cmd);
    return 0;
}

/**
 * @brief Runs the interactive console mode.
 *
 * @param[in,out] app Application state object.
 *
 * @return Process exit status code.
 */
static int noxmqtt_app_run_interactive(noxmqtt_app_t* app)
{
    char input[NOXMQTT_APP_INPUT_LEN];

    if (app == NULL) {
        return 1;
    }

    printf("NoxMQTT interactive mode. Type 'help' for commands.\n");
    noxmqtt_app_print_versions();

    while (app->running) {
        printf("> ");
        fflush(stdout);

        if (fgets(input, sizeof(input), stdin) == NULL) {
            break;
        }

        if (noxmqtt_app_process_line(app, noxmqtt_app_trim(input)) != 0) {
            break;
        }
    }

    return app->exit_code;
}

/**
 * @brief Runs a one-shot publish/subscribe/unsubscribe command.
 *
 * @param[in,out] app Application state object.
 *
 * @return Process exit status code.
 */
static int noxmqtt_app_run_command(noxmqtt_app_t* app)
{
    uint32_t start_ms = noxmqtt_tal_time_ms();

    if (app == NULL) {
        return 1;
    }

    if (noxmqtt_app_connect(app) != NOXMQTT_SUCCESS) {
        return 1;
    }

    while (app->running) {
        uint32_t elapsed_ms = noxmqtt_tal_time_ms() - start_ms;

        if (app->command_complete) {
            break;
        }

        if (app->wait_timeout_ms > 0U && elapsed_ms >= app->wait_timeout_ms) {
            if (app->action == NOXMQTT_APP_ACTION_SUBSCRIBE) {
                printf("subscribe wait timeout reached after %lu ms\n", (unsigned long)elapsed_ms);
            } else {
                fprintf(stderr, "command timed out after %lu ms\n", (unsigned long)elapsed_ms);
                app->exit_code = 1;
            }
            break;
        }

        noxmqtt_app_sleep_ms(100U);
    }

    return app->exit_code;
}

/**
 * @brief Entry point for the noxmqtt CLI application.
 *
 * @param[in] argc Argument count.
 * @param[in] argv Argument vector.
 *
 * @return Process exit status.
 */
int main(int argc, char** argv)
{
    noxmqtt_app_t app;
    int rc = 1;

    noxmqtt_app_defaults(&app);
    g_app = &app;

#ifdef _WIN32
    SetConsoleCtrlHandler(noxmqtt_app_console_handler, TRUE);
#else
    signal(SIGINT, noxmqtt_app_signal_handler);
    signal(SIGTERM, noxmqtt_app_signal_handler);
#endif

    rc = noxmqtt_app_parse_args(&app, argc, argv);
    if (rc != 0) {
        return (rc > 0) ? app.exit_code : 1;
    }

    if (noxmqtt_init(&app.client, app.debug_level) != NOXMQTT_SUCCESS) {
        fprintf(stderr, "Failed to initialize noxmqtt client\n");
        return 1;
    }

    if (noxmqtt_app_start_worker(&app) != 0) {
        fprintf(stderr, "Failed to start background worker\n");
        (void)noxmqtt_deinit(&app.client);
        return 1;
    }

    if (app.action == NOXMQTT_APP_ACTION_INTERACTIVE) {
        rc = noxmqtt_app_run_interactive(&app);
    } else {
        rc = noxmqtt_app_run_command(&app);
    }

    noxmqtt_app_disconnect(&app);
    noxmqtt_transport_wait();
    noxmqtt_app_stop_worker(&app);
    (void)noxmqtt_deinit(&app.client);

    return rc;
}
