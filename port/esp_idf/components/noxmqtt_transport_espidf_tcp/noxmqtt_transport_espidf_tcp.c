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
* File:    noxmqtt_transport_espidf_tcp.c
* Summary: Reusable plain TCP transport for NoxMQTT on ESP-IDF
*
*****************************************************************************/

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

#ifndef CONFIG_NOXMQTT_TRANSPORT_TCP_RECV_TASK_STACK_SIZE
#define CONFIG_NOXMQTT_TRANSPORT_TCP_RECV_TASK_STACK_SIZE 4096
#endif

#ifndef CONFIG_NOXMQTT_TRANSPORT_TCP_RECV_TASK_PRIORITY
#define CONFIG_NOXMQTT_TRANSPORT_TCP_RECV_TASK_PRIORITY 5
#endif

typedef struct
{
    int socket_fd;
    noxmqtt_transport_rcv_t receive_cb;
    noxmqtt_client_t* client;
    TaskHandle_t rx_task;
    volatile uint8_t running;
    volatile uint8_t local_disconnect;
} noxmqtt_esp_tcp_transport_t;

/**
 * @brief Gets the ESP-IDF TCP transport context for a client.
 *
 * @param[in] c NoxMQTT client instance.
 *
 * @return Transport context pointer, or `NULL` when unavailable.
 */
static noxmqtt_esp_tcp_transport_t* noxmqtt_transport_ctx(noxmqtt_client_t* c)
{
    if (c == NULL) {
        return NULL;
    }

    return (noxmqtt_esp_tcp_transport_t*)c->transport_ctx;
}

/**
 * @brief Closes the active ESP-IDF socket.
 *
 * @param[in] ctx Transport context to clean up.
 */
static void noxmqtt_cleanup_transport(noxmqtt_esp_tcp_transport_t* ctx)
{
    if (ctx == NULL) {
        return;
    }

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
 * @brief Receives TCP data for an NoxMQTT client on ESP-IDF.
 *
 * @param[in] arg Transport context passed to the task.
 */
static void noxmqtt_tcp_receive_task(void* arg)
{
    noxmqtt_esp_tcp_transport_t* ctx = (noxmqtt_esp_tcp_transport_t*)arg;
    uint8_t rx_buffer[1024];

    if (ctx == NULL || ctx->client == NULL) {
        vTaskDelete(NULL);
        return;
    }

    while (ctx->running) {
        int len = recv(ctx->socket_fd, rx_buffer, sizeof(rx_buffer), 0);

        if (len > 0) {
            if (ctx->receive_cb != NULL) {
                ctx->receive_cb(ctx->client, rx_buffer, (uint16_t)len);
            }
            (void)noxmqtt_process(ctx->client);
            continue;
        }

        if (len == 0) {
            ctx->running = 0;
            noxmqtt_cleanup_transport(ctx);
            if (!ctx->local_disconnect) {
                noxmqtt_transport_notify_disconnected(ctx->client, NOXMQTT_SUCCESS);
            }
            break;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ETIMEDOUT) {
            (void)noxmqtt_process(ctx->client);
            continue;
        }

        ctx->running = 0;
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
 * @brief Initializes the ESP-IDF TCP transport for a client.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] rcv_cback Receive callback invoked with incoming data.
 *
 * @return 0 on success, otherwise `-1`.
 */
int noxmqtt_transport_init(noxmqtt_client_t* c, noxmqtt_transport_rcv_t rcv_cback)
{
    noxmqtt_esp_tcp_transport_t* ctx = NULL;

    if (c == NULL || rcv_cback == NULL) {
        return -1;
    }

    ctx = noxmqtt_transport_ctx(c);
    if (ctx == NULL) {
        ctx = (noxmqtt_esp_tcp_transport_t*)calloc(1, sizeof(*ctx));
        if (ctx == NULL) {
            return -1;
        }

        ctx->socket_fd = -1;
        c->transport_ctx = ctx;
    }

    ctx->receive_cb = rcv_cback;
    ctx->client = c;
    ctx->local_disconnect = 0U;
    return 0;
}

/**
 * @brief Connects the ESP-IDF TCP transport to the configured broker.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] conf Client connection configuration.
 *
 * @return 0 on success, otherwise `-1`.
 */
int noxmqtt_transport_connect(noxmqtt_client_t* c, const noxmqtt_client_conf_t* conf)
{
    noxmqtt_esp_tcp_transport_t* ctx = noxmqtt_transport_ctx(c);
    uint32_t timeout_ms = NOXMQTT_DEFAULT_NETWORK_TIMEOUT_MS;
    BaseType_t task_rc = pdFAIL;

    if (ctx == NULL || conf == NULL) {
        return -1;
    }

    if (conf->server.mode != NOXMQTT_TRANSPORT_TCP) {
        return -1;
    }

    if (conf->server.network_timeout_ms != 0U) {
        timeout_ms = conf->server.network_timeout_ms;
    }

    if (noxmqtt_socket_connect(conf->server.addr, conf->server.port, timeout_ms, &ctx->socket_fd) != 0) {
        return -1;
    }

    ctx->running = 1U;
    ctx->local_disconnect = 0U;
    task_rc = xTaskCreate(noxmqtt_tcp_receive_task,
                          "noxmqtt_tcp_rx",
                          (uint32_t)(CONFIG_NOXMQTT_TRANSPORT_TCP_RECV_TASK_STACK_SIZE / sizeof(StackType_t)),
                          ctx,
                          CONFIG_NOXMQTT_TRANSPORT_TCP_RECV_TASK_PRIORITY,
                          &ctx->rx_task);
    if (task_rc != pdPASS) {
        ctx->running = 0U;
        noxmqtt_cleanup_transport(ctx);
        return -1;
    }

    return 0;
}

/**
 * @brief Sends raw MQTT bytes over the ESP-IDF TCP transport.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] data Buffer containing bytes to send.
 * @param[in] len Number of bytes to send.
 *
 * @return 0 on success, otherwise `-1`.
 */
int noxmqtt_transport_send(noxmqtt_client_t* c, const uint8_t* data, uint16_t len)
{
    noxmqtt_esp_tcp_transport_t* ctx = noxmqtt_transport_ctx(c);
    size_t sent_total = 0U;

    if (ctx == NULL || data == NULL || ctx->socket_fd < 0) {
        return -1;
    }

    while (sent_total < len) {
        int rc = send(ctx->socket_fd,
                      (const char*)data + sent_total,
                      (size_t)len - sent_total,
                      0);
        if (rc <= 0) {
            noxmqtt_cleanup_transport(ctx);
            return -1;
        }

        sent_total += (size_t)rc;
    }

    return 0;
}

/**
 * @brief Disconnects the ESP-IDF TCP transport.
 *
 * @param[in] c NoxMQTT client instance.
 *
 * @return 0.
 */
int noxmqtt_transport_disconnect(noxmqtt_client_t* c)
{
    noxmqtt_esp_tcp_transport_t* ctx = noxmqtt_transport_ctx(c);

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
