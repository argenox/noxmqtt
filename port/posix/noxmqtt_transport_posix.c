/*
 * SPDX-License-Identifier: GPL-2.0-only OR LicenseRef-Argenox-Commercial
 *
 * File:    noxmqtt_transport_posix.c
 * Summary: Reusable POSIX transport for NoxMQTT with optional NoxTLS support
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "noxmqtt.h"
#include "noxmqtt_config.h"
#include "noxmqtt_debug.h"
#include "noxmqtt_tal.h"

#if !defined(NOXMQTT_HAS_NOXTLS)
#define NOXMQTT_HAS_NOXTLS 0
#endif

#if NOXMQTT_ENABLE_NOXTLS
#if defined(__has_include)
#if __has_include("noxtls_common.h") && \
    __has_include("noxtls-lib/tls/noxtls_tls_common.h") && \
    __has_include("noxtls-lib/tls/noxtls_tls12.h") && \
    __has_include("noxtls-lib/tls/noxtls_tls13.h") && \
    __has_include("noxtls-lib/certs/noxtls_x509.h")
#undef NOXMQTT_HAS_NOXTLS
#define NOXMQTT_HAS_NOXTLS 1
#include "noxtls_common.h"
#include "noxtls-lib/certs/noxtls_x509.h"
#include "noxtls-lib/tls/noxtls_tls12.h"
#include "noxtls-lib/tls/noxtls_tls13.h"
#include "noxtls-lib/tls/noxtls_tls_common.h"
#endif
#endif
#endif

typedef struct
{
    int socket_fd;
    noxmqtt_transport_rcv_t receive_cb;
    noxmqtt_client_t* client;
    pthread_t thread;
    int thread_started;
    volatile sig_atomic_t running;
    noxmqtt_transport_mode_t mode;
#if NOXMQTT_HAS_NOXTLS
    tls12_context_t tls12;
    tls13_context_t tls13;
    uint8_t tls_active_version;
    uint8_t tls_ready;
    uint8_t trust_store_loaded;
#endif
} noxmqtt_posix_transport_t;

static pthread_t noxmqtt_last_thread;
static int noxmqtt_last_thread_valid = 0;

/**
 * @brief Gets the POSIX transport context for a client.
 *
 * @param[in] c NoxMQTT client instance.
 *
 * @return Transport context pointer, or `NULL` when unavailable.
 */
static noxmqtt_posix_transport_t* noxmqtt_transport_ctx(noxmqtt_client_t* c)
{
    if (c == NULL) {
        return NULL;
    }

    return (noxmqtt_posix_transport_t*)c->transport_ctx;
}

/**
 * @brief Opens a TCP socket to the requested host.
 *
 * @param[in] host Remote host name or address.
 * @param[in] port Remote port number.
 * @param[out] out_socket Connected socket file descriptor.
 *
 * @return 0 on success, otherwise `-1`.
 */
static int noxmqtt_socket_connect(const char* host, uint16_t port, int* out_socket)
{
    struct addrinfo hints;
    struct addrinfo* result = NULL;
    struct addrinfo* it = NULL;
    char port_str[16];
    int sock = -1;

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

    for (it = result; it != NULL; it = it->ai_next) {
        sock = (int)socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (sock < 0) {
            continue;
        }

        if (connect(sock, it->ai_addr, it->ai_addrlen) == 0) {
            break;
        }

        close(sock);
        sock = -1;
    }

    freeaddrinfo(result);

    if (sock < 0) {
        return -1;
    }

    *out_socket = sock;
    return 0;
}

/**
 * @brief Releases the POSIX socket and TLS transport state.
 *
 * @param[in] ctx Transport context to clean up.
 */
static void noxmqtt_cleanup_transport(noxmqtt_posix_transport_t* ctx)
{
    if (ctx == NULL) {
        return;
    }

#if NOXMQTT_HAS_NOXTLS
    if (ctx->tls_ready) {
        if (ctx->tls_active_version == TLS_VERSION_1_3) {
            noxtls_tls13_close(&ctx->tls13);
            noxtls_tls13_context_free(&ctx->tls13);
        } else if (ctx->tls_active_version == TLS_VERSION_1_2) {
            noxtls_tls12_close(&ctx->tls12);
            noxtls_tls12_context_free(&ctx->tls12);
        }

        ctx->tls_ready = 0;
        ctx->tls_active_version = 0;
    }

    if (ctx->trust_store_loaded) {
        noxtls_x509_trust_store_clear();
        ctx->trust_store_loaded = 0;
    }
#endif

    if (ctx->socket_fd >= 0) {
        shutdown(ctx->socket_fd, SHUT_RDWR);
        close(ctx->socket_fd);
        ctx->socket_fd = -1;
    }
}

#if NOXMQTT_HAS_NOXTLS
/**
 * @brief Sends bytes through the NoxTLS socket callback.
 *
 * @param[in] user_data POSIX transport context.
 * @param[in] data Buffer containing bytes to send.
 * @param[in] len Number of bytes to send.
 *
 * @return Number of bytes sent, or `-1` on failure.
 */
static int32_t noxmqtt_noxtls_send_cb(void* user_data, const uint8_t* data, uint32_t len)
{
    noxmqtt_posix_transport_t* ctx = (noxmqtt_posix_transport_t*)user_data;
    uint32_t sent_total = 0;

    if (ctx == NULL || data == NULL) {
        return -1;
    }

    while (sent_total < len) {
        ssize_t sent = send(ctx->socket_fd, data + sent_total, len - sent_total, 0);
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
 * @param[in] user_data POSIX transport context.
 * @param[out] data Buffer that receives incoming bytes.
 * @param[in] len Number of bytes requested.
 *
 * @return Number of bytes received, or `-1` on failure.
 */
static int32_t noxmqtt_noxtls_recv_cb(void* user_data, uint8_t* data, uint32_t len)
{
    noxmqtt_posix_transport_t* ctx = (noxmqtt_posix_transport_t*)user_data;
    uint32_t recv_total = 0;

    if (ctx == NULL || data == NULL) {
        return -1;
    }

    while (recv_total < len) {
        ssize_t rc = recv(ctx->socket_fd, data + recv_total, len - recv_total, 0);
        if (rc <= 0) {
            return -1;
        }

        recv_total += (uint32_t)rc;
    }

    return (int32_t)recv_total;
}

/**
 * @brief Provides a monotonic timestamp to NoxTLS.
 *
 * @param[in] user_data Unused callback context.
 *
 * @return Current monotonic time in milliseconds.
 */
static uint64_t noxmqtt_noxtls_time_cb(void* user_data)
{
    (void)user_data;
    return (uint64_t)noxmqtt_tal_time_ms();
}

/**
 * @brief Loads the configured CA certificate into the NoxTLS trust store.
 *
 * @param[in] ctx POSIX transport context.
 * @param[in] ca_file Path to the CA certificate file.
 *
 * @return 0 on success, otherwise `-1`.
 */
static int noxmqtt_noxtls_configure_trust_store(noxmqtt_posix_transport_t* ctx, const char* ca_file)
{
    noxtls_return_t rc = NOXTLS_RETURN_SUCCESS;
    x509_certificate_t ca_cert;
    x509_certificate_chain_t trust_chain;

    if (ctx == NULL || ca_file == NULL || ca_file[0] == '\0') {
        return -1;
    }

    noxtls_x509_certificate_init(&ca_cert);
    rc = noxtls_x509_certificate_load_file(&ca_cert, ca_file);
    if (rc != NOXTLS_RETURN_SUCCESS) {
        noxtls_x509_certificate_free(&ca_cert);
        return -1;
    }

    rc = noxtls_x509_certificate_chain_init(&trust_chain);
    if (rc != NOXTLS_RETURN_SUCCESS) {
        noxtls_x509_certificate_free(&ca_cert);
        return -1;
    }

    rc = noxtls_x509_certificate_chain_add(&trust_chain, &ca_cert);
    if (rc == NOXTLS_RETURN_SUCCESS) {
        rc = noxtls_x509_trust_store_set(&trust_chain);
    }

    noxtls_x509_certificate_chain_free(&trust_chain);
    noxtls_x509_certificate_free(&ca_cert);

    if (rc == NOXTLS_RETURN_SUCCESS) {
        ctx->trust_store_loaded = 1;
        return 0;
    }

    return -1;
}

/**
 * @brief Performs the NoxTLS client handshake for a broker connection.
 *
 * @param[in] ctx POSIX transport context.
 * @param[in] conf Client connection configuration.
 *
 * @return 0 on success, otherwise `-1`.
 */
static int noxmqtt_noxtls_handshake(noxmqtt_posix_transport_t* ctx, const noxmqtt_client_conf_t* conf)
{
    noxtls_return_t rc = NOXTLS_RETURN_SUCCESS;
    const char* server_name = NULL;

    if (ctx == NULL || conf == NULL) {
        return -1;
    }

    if (conf->server.tls.client_cert != NULL || conf->server.tls.client_key != NULL) {
        return -1;
    }

    server_name = conf->server.tls.server_name;
    if (server_name == NULL || server_name[0] == '\0') {
        server_name = conf->server.addr;
    }

    if (conf->server.tls.verify_peer) {
        if (noxmqtt_noxtls_configure_trust_store(ctx, conf->server.tls.ca_cert) != 0) {
            return -1;
        }
    }

    rc = noxtls_tls13_context_init(&ctx->tls13, TLS_ROLE_CLIENT);
    if (rc == NOXTLS_RETURN_SUCCESS) {
        ctx->tls13.server_name = server_name;
        ctx->tls13.server_name_len = (uint16_t)strlen(server_name);
        rc = noxtls_tls_set_io_callbacks(&ctx->tls13.base.base,
                                         noxmqtt_noxtls_send_cb,
                                         noxmqtt_noxtls_recv_cb,
                                         ctx);
        if (rc == NOXTLS_RETURN_SUCCESS) {
            (void)noxtls_tls_set_time_callback(&ctx->tls13.base.base, noxmqtt_noxtls_time_cb);
            rc = noxtls_tls13_connect(&ctx->tls13);
        }

        if (rc == NOXTLS_RETURN_SUCCESS) {
            ctx->tls_active_version = TLS_VERSION_1_3;
            ctx->tls_ready = 1;
            return 0;
        }

        noxtls_tls13_context_free(&ctx->tls13);
    }

    rc = noxtls_tls12_context_init(&ctx->tls12, TLS_ROLE_CLIENT);
    if (rc != NOXTLS_RETURN_SUCCESS) {
        return -1;
    }

    ctx->tls12.server_name = server_name;
    ctx->tls12.server_name_len = (uint16_t)strlen(server_name);
    rc = noxtls_tls_set_io_callbacks(&ctx->tls12.base.base,
                                     noxmqtt_noxtls_send_cb,
                                     noxmqtt_noxtls_recv_cb,
                                     ctx);
    if (rc != NOXTLS_RETURN_SUCCESS) {
        noxtls_tls12_context_free(&ctx->tls12);
        return -1;
    }

    (void)noxtls_tls_set_time_callback(&ctx->tls12.base.base, noxmqtt_noxtls_time_cb);
    rc = noxtls_tls12_connect(&ctx->tls12);
    if (rc != NOXTLS_RETURN_SUCCESS) {
        noxtls_tls12_context_free(&ctx->tls12);
        return -1;
    }

    ctx->tls_active_version = TLS_VERSION_1_2;
    ctx->tls_ready = 1;
    return 0;
}
#endif

/**
 * @brief Initializes the POSIX transport for a client.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] rcv_cback Receive callback invoked with incoming data.
 *
 * @return 0 on success, otherwise `-1`.
 */
int noxmqtt_transport_init(noxmqtt_client_t* c, noxmqtt_transport_rcv_t rcv_cback)
{
    noxmqtt_posix_transport_t* ctx = NULL;

    if (c == NULL || rcv_cback == NULL) {
        return -1;
    }

    ctx = noxmqtt_transport_ctx(c);
    if (ctx == NULL) {
        ctx = (noxmqtt_posix_transport_t*)calloc(1, sizeof(*ctx));
        if (ctx == NULL) {
            return -1;
        }

        ctx->socket_fd = -1;
        c->transport_ctx = ctx;
    }

    ctx->receive_cb = rcv_cback;
    ctx->client = c;
    return 0;
}

/**
 * @brief Connects the POSIX transport to the configured broker.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] conf Client connection configuration.
 *
 * @return 0 on success, otherwise `-1`.
 */
int noxmqtt_transport_connect(noxmqtt_client_t* c, const noxmqtt_client_conf_t* conf)
{
    noxmqtt_posix_transport_t* ctx = noxmqtt_transport_ctx(c);
    struct timeval timeout;

    if (ctx == NULL || conf == NULL) {
        return -1;
    }

    if (noxmqtt_socket_connect(conf->server.addr, conf->server.port, &ctx->socket_fd) != 0) {
        return -1;
    }

    ctx->mode = conf->server.mode;
    timeout.tv_sec = (time_t)(conf->server.network_timeout_ms / 1000U);
    timeout.tv_usec = (suseconds_t)((conf->server.network_timeout_ms % 1000U) * 1000U);
    if (conf->server.network_timeout_ms == 0U) {
        timeout.tv_sec = NOXMQTT_DEFAULT_NETWORK_TIMEOUT_MS / 1000U;
        timeout.tv_usec = (NOXMQTT_DEFAULT_NETWORK_TIMEOUT_MS % 1000U) * 1000U;
    }

    (void)setsockopt(ctx->socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    (void)setsockopt(ctx->socket_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    if (ctx->mode == NOXMQTT_TRANSPORT_TLS) {
#if NOXMQTT_HAS_NOXTLS
        if (noxmqtt_noxtls_handshake(ctx, conf) != 0) {
            noxmqtt_cleanup_transport(ctx);
            return -1;
        }
#else
        noxmqtt_cleanup_transport(ctx);
        return -1;
#endif
    } else if (ctx->mode != NOXMQTT_TRANSPORT_TCP) {
        noxmqtt_cleanup_transport(ctx);
        return -1;
    }

    ctx->running = 1;
    if (pthread_create(&ctx->thread, NULL, (void* (*)(void*))noxmqtt_transport_receive_thread, ctx) != 0) {
        ctx->running = 0;
        noxmqtt_cleanup_transport(ctx);
        return -1;
    }

    ctx->thread_started = 1;
    noxmqtt_last_thread = ctx->thread;
    noxmqtt_last_thread_valid = 1;
    return 0;
}

/**
 * @brief Sends raw MQTT bytes over the POSIX transport.
 *
 * @param[in] c NoxMQTT client instance.
 * @param[in] data Buffer containing bytes to send.
 * @param[in] len Number of bytes to send.
 *
 * @return 0 on success, otherwise `-1`.
 */
int noxmqtt_transport_send(noxmqtt_client_t* c, const uint8_t* data, uint16_t len)
{
    noxmqtt_posix_transport_t* ctx = noxmqtt_transport_ctx(c);
    ssize_t sent = 0;

    if (ctx == NULL || data == NULL) {
        return -1;
    }

#if NOXMQTT_HAS_NOXTLS
    if (ctx->mode == NOXMQTT_TRANSPORT_TLS) {
        noxtls_return_t rc = NOXTLS_RETURN_NOT_SUPPORTED;

        if (!ctx->tls_ready) {
            return -1;
        }

        if (ctx->tls_active_version == TLS_VERSION_1_3) {
            rc = noxtls_tls13_send(&ctx->tls13, data, len);
        } else if (ctx->tls_active_version == TLS_VERSION_1_2) {
            rc = noxtls_tls12_send(&ctx->tls12, data, len);
        }

        return (rc == NOXTLS_RETURN_SUCCESS) ? 0 : -1;
    }
#endif

    while ((size_t)sent < len) {
        ssize_t rc = send(ctx->socket_fd, data + sent, len - (size_t)sent, 0);
        if (rc <= 0) {
            noxmqtt_cleanup_transport(ctx);
            return -1;
        }

        sent += rc;
    }

    return 0;
}

/**
 * @brief Disconnects the POSIX transport.
 *
 * @param[in] c NoxMQTT client instance.
 *
 * @return 0.
 */
int noxmqtt_transport_disconnect(noxmqtt_client_t* c)
{
    noxmqtt_posix_transport_t* ctx = noxmqtt_transport_ctx(c);

    if (ctx == NULL) {
        return 0;
    }

    ctx->running = 0;
    noxmqtt_cleanup_transport(ctx);
    return 0;
}

/**
 * @brief Receives TCP or TLS data for a POSIX NoxMQTT client.
 *
 * @param[in] ptr Transport context passed to the thread.
 *
 * @return 0.
 */
int noxmqtt_transport_receive_thread(void* ptr)
{
    noxmqtt_posix_transport_t* ctx = (noxmqtt_posix_transport_t*)ptr;
    uint8_t rx_buffer[1024];

    if (ctx == NULL || ctx->client == NULL) {
        return 0;
    }

    while (ctx->running) {
#if NOXMQTT_HAS_NOXTLS
        if (ctx->mode == NOXMQTT_TRANSPORT_TLS) {
            noxtls_return_t rc = NOXTLS_RETURN_NOT_SUPPORTED;
            uint32_t len = sizeof(rx_buffer);

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

            ctx->running = 0;
            noxmqtt_cleanup_transport(ctx);
            noxmqtt_transport_notify_disconnected(ctx->client, NOXMQTT_RC_ERROR_TRANSPORT);
            break;
        }
#endif

        {
            ssize_t len = recv(ctx->socket_fd, rx_buffer, sizeof(rx_buffer), 0);

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
                noxmqtt_transport_notify_disconnected(ctx->client, NOXMQTT_SUCCESS);
                break;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                (void)noxmqtt_process(ctx->client);
                continue;
            }

            if (errno == EINTR) {
                continue;
            }

            ctx->running = 0;
            noxmqtt_cleanup_transport(ctx);
            noxmqtt_transport_notify_disconnected(ctx->client, NOXMQTT_RC_ERROR_TRANSPORT);
        }
    }

    ctx->thread_started = 0;
    return 0;
}

/**
 * @brief Waits for the POSIX receive thread to terminate.
 */
void noxmqtt_transport_wait(void)
{
    if (noxmqtt_last_thread_valid) {
        (void)pthread_join(noxmqtt_last_thread, NULL);
        noxmqtt_last_thread_valid = 0;
    }
}

/**
 * @brief Gets the current platform tick count in milliseconds.
 *
 * @return Current monotonic time in milliseconds.
 */
uint32_t noxmqtt_tal_time_ms(void)
{
#if defined(CLOCK_MONOTONIC)
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
        return (uint32_t)((uint64_t)ts.tv_sec * 1000ULL + (uint64_t)(ts.tv_nsec / 1000000ULL));
    }
#endif

    {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return (uint32_t)((uint64_t)tv.tv_sec * 1000ULL + (uint64_t)(tv.tv_usec / 1000ULL));
    }
}

/**
 * @brief Prints a debug string through the platform console.
 *
 * @param[in] str Null-terminated string to print.
 */
void noxmqtt_hal_debug_printf(const char* str)
{
    if (str != NULL) {
        fputs(str, stdout);
        fflush(stdout);
    }
}

#ifdef __cplusplus
}
#endif
