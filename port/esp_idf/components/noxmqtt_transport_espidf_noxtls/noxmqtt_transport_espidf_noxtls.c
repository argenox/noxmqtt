/*
 * SPDX-License-Identifier: GPL-2.0-only OR LicenseRef-Argenox-Commercial
 *
 * File:    noxmqtt_transport_espidf_noxtls.c
 * Summary: Reusable NoxTLS transport for NoxMQTT on ESP-IDF
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "lwip/netdb.h"
#include "lwip/sockets.h"

#include "noxmqtt.h"
#include "noxmqtt_config.h"
#include "noxmqtt_tal.h"

#include "noxtls_common.h"
#include "noxtls_esp_idf.h"
#include "noxtls_x509.h"
#include "noxtls_tls12.h"
#include "noxtls_tls13.h"

#include "../../../common/noxmqtt_noxtls_client_identity.h"

#ifndef CONFIG_NOXMQTT_TRANSPORT_NOXTLS_RECV_TASK_STACK_SIZE
#define CONFIG_NOXMQTT_TRANSPORT_NOXTLS_RECV_TASK_STACK_SIZE 6144
#endif

#ifndef CONFIG_NOXMQTT_TRANSPORT_NOXTLS_RECV_TASK_PRIORITY
#define CONFIG_NOXMQTT_TRANSPORT_NOXTLS_RECV_TASK_PRIORITY 5
#endif

typedef struct
{
    int socket_fd;
    noxmqtt_transport_rcv_t receive_cb;
    noxmqtt_client_t* client;
    TaskHandle_t rx_task;
    volatile uint8_t running;
    volatile uint8_t local_disconnect;
    uint8_t tls_ready;
    uint8_t tls_active_version;
    uint8_t trust_store_loaded;
    tls12_context_t tls12;
    tls13_context_t tls13;
    noxmqtt_noxtls_client_identity_t client_identity;
} noxmqtt_esp_noxtls_transport_t;

/**
 * @brief Gets the ESP-IDF NoxTLS transport context for a client.
 *
 * @param[in] c NoxMQTT client instance.
 *
 * @return Transport context pointer, or `NULL` when unavailable.
 */
static noxmqtt_esp_noxtls_transport_t* noxmqtt_transport_ctx(noxmqtt_client_t* c)
{
    if (c == NULL) {
        return NULL;
    }

    return (noxmqtt_esp_noxtls_transport_t*)c->transport_ctx;
}

/**
 * @brief Closes the active ESP-IDF socket and TLS state.
 *
 * @param[in] ctx Transport context to clean up.
 */
static void noxmqtt_cleanup_transport(noxmqtt_esp_noxtls_transport_t* ctx)
{
    if (ctx == NULL) {
        return;
    }

    if (ctx->tls_ready) {
        if (ctx->tls_active_version == TLS_VERSION_1_3) {
            (void)noxtls_tls13_close(&ctx->tls13);
            (void)noxtls_tls13_context_free(&ctx->tls13);
        } else if (ctx->tls_active_version == TLS_VERSION_1_2) {
            (void)noxtls_tls12_close(&ctx->tls12);
            (void)noxtls_tls12_context_free(&ctx->tls12);
        }
        ctx->tls_ready = 0U;
        ctx->tls_active_version = 0U;
    }

    if (ctx->trust_store_loaded) {
        noxtls_x509_trust_store_clear();
        ctx->trust_store_loaded = 0U;
    }

    noxmqtt_noxtls_client_identity_free(&ctx->client_identity);

    if (ctx->socket_fd >= 0) {
        shutdown(ctx->socket_fd, SHUT_RDWR);
        close(ctx->socket_fd);
        ctx->socket_fd = -1;
    }
}

/**
 * @brief Opens a TCP socket to the requested host.
 *
 * @param[in] host Remote host name or address.
 * @param[in] port Remote port number.
 * @param[in] timeout_ms Socket send and receive timeout in milliseconds.
 * @param[out] out_socket Connected socket descriptor.
 *
 * @return 0 on success, otherwise `-1`.
 */
static int noxmqtt_socket_connect(const char* host, uint16_t port, uint32_t timeout_ms, int* out_socket)
{
    struct addrinfo hints;
    struct addrinfo* result = NULL;
    struct addrinfo* it = NULL;
    char port_str[16];
    int sock = -1;
    int rc = -1;
    struct timeval timeout;

    if (host == NULL || out_socket == NULL) {
        return -1;
    }

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    snprintf(port_str, sizeof(port_str), "%u", port);
    if (getaddrinfo(host, port_str, &hints, &result) != 0) {
        return -1;
    }

    timeout.tv_sec = (time_t)(timeout_ms / 1000U);
    timeout.tv_usec = (suseconds_t)((timeout_ms % 1000U) * 1000U);

    for (it = result; it != NULL; it = it->ai_next) {
        sock = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (sock < 0) {
            continue;
        }

        (void)setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        (void)setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

        if (connect(sock, it->ai_addr, it->ai_addrlen) == 0) {
            *out_socket = sock;
            rc = 0;
            break;
        }

        close(sock);
        sock = -1;
    }

    freeaddrinfo(result);
    return rc;
}

/**
 * @brief Sends bytes through the NoxTLS socket callback.
 *
 * @param[in] user_data Transport context.
 * @param[in] data Buffer containing bytes to send.
 * @param[in] len Number of bytes to send.
 *
 * @return Number of bytes sent, or `-1` on failure.
 */
static int32_t noxmqtt_noxtls_send_cb(void* user_data, const uint8_t* data, uint32_t len)
{
    noxmqtt_esp_noxtls_transport_t* ctx = (noxmqtt_esp_noxtls_transport_t*)user_data;
    uint32_t sent_total = 0U;

    if (ctx == NULL || data == NULL) {
        return -1;
    }

    while (sent_total < len) {
        int sent = send(ctx->socket_fd, data + sent_total, len - sent_total, 0);
        if (sent <= 0) {
            return -1;
        }

        sent_total += (uint32_t)sent;
    }

    return (int32_t)sent_total;
}

/**
 * @brief Receives bytes through the NoxTLS socket callback.
 *
 * @param[in] user_data Transport context.
 * @param[out] data Buffer that receives incoming bytes.
 * @param[in] len Number of bytes requested.
 *
 * @return Number of bytes received, or `-1` on failure.
 */
static int32_t noxmqtt_noxtls_recv_cb(void* user_data, uint8_t* data, uint32_t len)
{
    noxmqtt_esp_noxtls_transport_t* ctx = (noxmqtt_esp_noxtls_transport_t*)user_data;
    uint32_t recv_total = 0U;

    if (ctx == NULL || data == NULL) {
        return -1;
    }

    while (recv_total < len) {
        int received = recv(ctx->socket_fd, data + recv_total, len - recv_total, 0);
        if (received <= 0) {
            return -1;
        }

        recv_total += (uint32_t)received;
    }

    return (int32_t)recv_total;
}

/**
 * @brief Provides a monotonic timestamp to NoxTLS.
 *
 * @param[in] user_data Unused callback context.
 *
 * @return Current time in milliseconds.
 */
static uint64_t noxmqtt_noxtls_time_cb(void* user_data)
{
    (void)user_data;
    return (uint64_t)esp_timer_get_time() / 1000ULL;
}

/**
 * @brief Loads the configured CA certificate into the NoxTLS trust store.
 *
 * @param[in] ctx Transport context.
 * @param[in] ca_source PEM certificate text or a readable file path.
 *
 * @return 0 on success, otherwise `-1`.
 */
static int noxmqtt_noxtls_configure_trust_store(noxmqtt_esp_noxtls_transport_t* ctx, const char* ca_source)
{
    noxtls_return_t rc = NOXTLS_RETURN_SUCCESS;
    x509_certificate_t ca_cert;
    x509_certificate_chain_t trust_chain;

    if (ctx == NULL || ca_source == NULL || ca_source[0] == '\0') {
        return -1;
    }

    (void)noxtls_x509_certificate_init(&ca_cert);
    if (noxmqtt_noxtls_load_certificate_source(ca_source, &ca_cert) != 0) {
        (void)noxtls_x509_certificate_free(&ca_cert);
        return -1;
    }

    rc = noxtls_x509_certificate_chain_init(&trust_chain);
    if (rc != NOXTLS_RETURN_SUCCESS) {
        (void)noxtls_x509_certificate_free(&ca_cert);
        return -1;
    }

    rc = noxtls_x509_certificate_chain_add(&trust_chain, &ca_cert);
    if (rc == NOXTLS_RETURN_SUCCESS) {
        rc = noxtls_x509_trust_store_set(&trust_chain);
    }

    (void)noxtls_x509_certificate_chain_free(&trust_chain);
    (void)noxtls_x509_certificate_free(&ca_cert);

    if (rc != NOXTLS_RETURN_SUCCESS) {
        return -1;
    }

    ctx->trust_store_loaded = 1U;
    return 0;
}

/**
 * @brief Resolves the hostname value used for TLS hostname verification.
 *
 * @param[in] conf Client connection configuration.
 *
 * @return Hostname string, or `NULL` when hostname verification is disabled.
 */
static const char* noxmqtt_noxtls_hostname_for_verification(const noxmqtt_client_conf_t* conf)
{
    if (conf == NULL || !conf->server.tls.verify_hostname) {
        return NULL;
    }

    if (conf->server.tls.server_name != NULL && conf->server.tls.server_name[0] != '\0') {
        return conf->server.tls.server_name;
    }

    return conf->server.addr;
}

/**
 * @brief Performs the NoxTLS client handshake for a broker connection.
 *
 * @param[in] ctx Transport context.
 * @param[in] conf Client connection configuration.
 *
 * @return 0 on success, otherwise `-1`.
 */
static int noxmqtt_noxtls_handshake(noxmqtt_esp_noxtls_transport_t* ctx, const noxmqtt_client_conf_t* conf)
{
    noxtls_return_t rc = NOXTLS_RETURN_SUCCESS;
    const char* server_name = NULL;
    int require_tls13 = 0;

    if (ctx == NULL || conf == NULL) {
        return -1;
    }

    if (noxmqtt_noxtls_client_identity_load(&ctx->client_identity,
                                            conf->server.tls.client_cert,
                                            conf->server.tls.client_key) != 0) {
        return -1;
    }
    require_tls13 = (ctx->client_identity.configured != 0U) ? 1 : 0;

    if (conf->server.tls.verify_peer) {
        if (noxmqtt_noxtls_configure_trust_store(ctx, conf->server.tls.ca_cert) != 0) {
            return -1;
        }
    }

    (void)noxtls_esp_idf_init();
    server_name = noxmqtt_noxtls_hostname_for_verification(conf);

    rc = noxtls_tls13_context_init(&ctx->tls13, TLS_ROLE_CLIENT);
    if (rc != NOXTLS_RETURN_SUCCESS) {
        return -1;
    }

    rc = noxtls_tls_set_io_callbacks(&ctx->tls13.base.base,
                                     noxmqtt_noxtls_send_cb,
                                     noxmqtt_noxtls_recv_cb,
                                     ctx);
    if (rc != NOXTLS_RETURN_SUCCESS) {
        (void)noxtls_tls13_context_free(&ctx->tls13);
        return -1;
    }

    (void)noxtls_tls_set_time_callback(&ctx->tls13.base.base, noxmqtt_noxtls_time_cb);
    if (server_name != NULL && server_name[0] != '\0') {
        ctx->tls13.server_name = server_name;
        ctx->tls13.server_name_len = (uint16_t)strlen(server_name);
    }

    if (ctx->client_identity.configured) {
        rc = (noxmqtt_noxtls_client_identity_apply_tls13(&ctx->client_identity, &ctx->tls13) == 0)
            ? NOXTLS_RETURN_SUCCESS
            : NOXTLS_RETURN_FAILED;
    }
    if (rc != NOXTLS_RETURN_SUCCESS) {
        (void)noxtls_tls13_context_free(&ctx->tls13);
        return -1;
    }

    rc = noxtls_tls13_connect(&ctx->tls13);
    if (rc == NOXTLS_RETURN_SUCCESS) {
        ctx->tls_ready = 1U;
        ctx->tls_active_version = TLS_VERSION_1_3;
        return 0;
    }

    (void)noxtls_tls13_context_free(&ctx->tls13);
    if (require_tls13) {
        return -1;
    }

    rc = noxtls_tls12_context_init(&ctx->tls12, TLS_ROLE_CLIENT);
    if (rc != NOXTLS_RETURN_SUCCESS) {
        return -1;
    }

    rc = noxtls_tls_set_io_callbacks(&ctx->tls12.base.base,
                                     noxmqtt_noxtls_send_cb,
                                     noxmqtt_noxtls_recv_cb,
                                     ctx);
    if (rc != NOXTLS_RETURN_SUCCESS) {
        (void)noxtls_tls12_context_free(&ctx->tls12);
        return -1;
    }

    (void)noxtls_tls_set_time_callback(&ctx->tls12.base.base, noxmqtt_noxtls_time_cb);
    if (server_name != NULL && server_name[0] != '\0') {
        ctx->tls12.server_name = server_name;
        ctx->tls12.server_name_len = (uint16_t)strlen(server_name);
    }

    rc = noxtls_tls12_connect(&ctx->tls12);
    if (rc != NOXTLS_RETURN_SUCCESS) {
        (void)noxtls_tls12_context_free(&ctx->tls12);
        return -1;
    }

    ctx->tls_ready = 1U;
    ctx->tls_active_version = TLS_VERSION_1_2;
    return 0;
}

/**
 * @brief Receives TLS data for an NoxMQTT client on ESP-IDF.
 *
 * @param[in] arg Transport context passed to the task.
 */
static void noxmqtt_noxtls_receive_task(void* arg)
{
    noxmqtt_esp_noxtls_transport_t* ctx = (noxmqtt_esp_noxtls_transport_t*)arg;
    uint8_t rx_buffer[1024];

    if (ctx == NULL || ctx->client == NULL) {
        vTaskDelete(NULL);
        return;
    }

    while (ctx->running) {
        uint32_t len = sizeof(rx_buffer);
        noxtls_return_t rc = NOXTLS_RETURN_NOT_SUPPORTED;

        if (ctx->tls_active_version == TLS_VERSION_1_3) {
            rc = noxtls_tls13_recv(&ctx->tls13, rx_buffer, &len);
        } else if (ctx->tls_active_version == TLS_VERSION_1_2) {
            rc = noxtls_tls12_recv(&ctx->tls12, rx_buffer, &len);
        }

        if (rc == NOXTLS_RETURN_SUCCESS && len > 0U) {
            if (ctx->receive_cb != NULL) {
                ctx->receive_cb(ctx->client, rx_buffer, (uint16_t)len);
            }
            (void)noxmqtt_process(ctx->client);
            continue;
        }

        if (rc == NOXTLS_RETURN_TIMEOUT) {
            (void)noxmqtt_process(ctx->client);
            continue;
        }

        if (rc == NOXTLS_RETURN_SUCCESS && len == 0U) {
            (void)noxmqtt_process(ctx->client);
            continue;
        }

        ctx->running = 0U;
        noxmqtt_cleanup_transport(ctx);
        if (!ctx->local_disconnect) {
            noxmqtt_transport_notify_disconnected(ctx->client, NOXMQTT_RC_ERROR_TRANSPORT);
        }
        break;
    }

    ctx->rx_task = NULL;
    vTaskDelete(NULL);
}

/**
 * @brief Initializes the ESP-IDF NoxTLS transport for a client.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] rcv_cback Receive callback invoked with incoming data.
 *
 * @return 0 on success, otherwise `-1`.
 */
int noxmqtt_transport_init(noxmqtt_client_t* c, noxmqtt_transport_rcv_t rcv_cback)
{
    noxmqtt_esp_noxtls_transport_t* ctx = NULL;

    if (c == NULL || rcv_cback == NULL) {
        return -1;
    }

    ctx = noxmqtt_transport_ctx(c);
    if (ctx == NULL) {
        ctx = (noxmqtt_esp_noxtls_transport_t*)calloc(1, sizeof(*ctx));
        if (ctx == NULL) {
            return -1;
        }

        ctx->socket_fd = -1;
        noxmqtt_noxtls_client_identity_init(&ctx->client_identity);
        c->transport_ctx = ctx;
    }

    ctx->receive_cb = rcv_cback;
    ctx->client = c;
    ctx->local_disconnect = 0U;
    return 0;
}

/**
 * @brief Connects the ESP-IDF NoxTLS transport to the configured broker.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] conf Client connection configuration.
 *
 * @return 0 on success, otherwise `-1`.
 */
int noxmqtt_transport_connect(noxmqtt_client_t* c, const noxmqtt_client_conf_t* conf)
{
    noxmqtt_esp_noxtls_transport_t* ctx = noxmqtt_transport_ctx(c);
    uint32_t timeout_ms = NOXMQTT_DEFAULT_NETWORK_TIMEOUT_MS;
    BaseType_t task_rc = pdFAIL;

    if (ctx == NULL || conf == NULL) {
        return -1;
    }

    if (conf->server.mode != NOXMQTT_TRANSPORT_TLS) {
        return -1;
    }

    if (conf->server.network_timeout_ms != 0U) {
        timeout_ms = conf->server.network_timeout_ms;
    }

    if (noxmqtt_socket_connect(conf->server.addr, conf->server.port, timeout_ms, &ctx->socket_fd) != 0) {
        return -1;
    }

    if (noxmqtt_noxtls_handshake(ctx, conf) != 0) {
        noxmqtt_cleanup_transport(ctx);
        return -1;
    }

    ctx->running = 1U;
    ctx->local_disconnect = 0U;
    task_rc = xTaskCreate(noxmqtt_noxtls_receive_task,
                          "noxmqtt_tls_rx",
                          (uint32_t)(CONFIG_NOXMQTT_TRANSPORT_NOXTLS_RECV_TASK_STACK_SIZE / sizeof(StackType_t)),
                          ctx,
                          CONFIG_NOXMQTT_TRANSPORT_NOXTLS_RECV_TASK_PRIORITY,
                          &ctx->rx_task);
    if (task_rc != pdPASS) {
        ctx->running = 0U;
        noxmqtt_cleanup_transport(ctx);
        return -1;
    }

    return 0;
}

/**
 * @brief Sends raw MQTT bytes over the ESP-IDF NoxTLS transport.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] data Buffer containing bytes to send.
 * @param[in] len Number of bytes to send.
 *
 * @return 0 on success, otherwise `-1`.
 */
int noxmqtt_transport_send(noxmqtt_client_t* c, const uint8_t* data, uint16_t len)
{
    noxmqtt_esp_noxtls_transport_t* ctx = noxmqtt_transport_ctx(c);
    noxtls_return_t rc = NOXTLS_RETURN_FAILED;

    if (ctx == NULL || data == NULL || !ctx->tls_ready) {
        return -1;
    }

    if (ctx->tls_active_version == TLS_VERSION_1_3) {
        rc = noxtls_tls13_send(&ctx->tls13, data, len);
    } else if (ctx->tls_active_version == TLS_VERSION_1_2) {
        rc = noxtls_tls12_send(&ctx->tls12, data, len);
    }

    return (rc == NOXTLS_RETURN_SUCCESS) ? 0 : -1;
}

/**
 * @brief Disconnects the ESP-IDF NoxTLS transport.
 *
 * @param[in] c NoxMQTT client instance.
 *
 * @return 0.
 */
int noxmqtt_transport_disconnect(noxmqtt_client_t* c)
{
    noxmqtt_esp_noxtls_transport_t* ctx = noxmqtt_transport_ctx(c);

    if (ctx == NULL) {
        return 0;
    }

    ctx->local_disconnect = 1U;
    ctx->running = 0U;
    noxmqtt_cleanup_transport(ctx);
    return 0;
}

/**
 * @brief Compatibility stub for platforms that do not use this entry point.
 *
 * @param[in] ptr Unused thread argument.
 *
 * @return 0.
 */
int noxmqtt_transport_receive_thread(void* ptr)
{
    (void)ptr;
    return 0;
}

/**
 * @brief Waits for the transport task to finish.
 */
void noxmqtt_transport_wait(void)
{
}

/**
 * @brief Gets the current platform tick count in milliseconds.
 *
 * @return Current monotonic time in milliseconds.
 */
uint32_t noxmqtt_tal_time_ms(void)
{
    return (uint32_t)(esp_timer_get_time() / 1000ULL);
}

/**
 * @brief Prints a debug string through the platform console.
 *
 * @param[in] str Null-terminated string to print.
 */
void noxmqtt_hal_debug_printf(const char* str)
{
    if (str != NULL) {
        printf("%s", str);
    }
}
