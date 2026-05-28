/*
 * SPDX-License-Identifier: GPL-2.0-only OR LicenseRef-Argenox-Commercial
 *
 * File:    noxmqtt_noxtls_client_identity.h
 * Summary: Shared NoxTLS client identity helpers for NoxMQTT transports
 */

#ifndef _NOXMQTT_NOXTLS_CLIENT_IDENTITY_H_
#define _NOXMQTT_NOXTLS_CLIENT_IDENTITY_H_

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "noxtls_common.h"
#include "noxtls-lib/certs/noxtls_x509.h"
#include "noxtls-lib/pkc/ecc/noxtls_ecc.h"
#include "noxtls-lib/pkc/rsa/noxtls_rsa.h"
#include "noxtls-lib/tls/noxtls_tls13.h"

typedef struct
{
    x509_certificate_t cert;
    x509_private_key_t key;
    rsa_key_t rsa_key;
    ecc_key_t ecc_key;
    uint8_t cert_loaded;
    uint8_t key_loaded;
    uint8_t rsa_ready;
    uint8_t ecc_ready;
    uint8_t configured;
} noxmqtt_noxtls_client_identity_t;

/**
 * @brief Detects whether a source string contains an inline PEM block.
 *
 * @param[in] source Source string to inspect.
 * @param[in] pem_marker PEM marker expected within the source text.
 *
 * @return 1 when the source contains a PEM block, otherwise 0.
 */
static int noxmqtt_noxtls_source_is_pem(const char* source, const char* pem_marker)
{
    if (source == NULL || pem_marker == NULL) {
        return 0;
    }

    return (strstr(source, "-----BEGIN") != NULL && strstr(source, pem_marker) != NULL) ? 1 : 0;
}

/**
 * @brief Loads a certificate from inline PEM text or a filesystem path.
 *
 * @param[in] source PEM certificate text or a readable file path.
 * @param[out] cert Parsed certificate object.
 *
 * @return 0 on success, otherwise `-1`.
 */
static int noxmqtt_noxtls_load_certificate_source(const char* source, x509_certificate_t* cert)
{
    noxtls_return_t rc = NOXTLS_RETURN_FAILED;
    size_t source_len = 0U;

    if (source == NULL || cert == NULL || source[0] == '\0') {
        return -1;
    }

    source_len = strlen(source);
    if (noxmqtt_noxtls_source_is_pem(source, "CERTIFICATE-----")) {
        rc = noxtls_x509_certificate_parse_pem(cert, (const uint8_t*)source, (uint32_t)source_len);
        if (rc == NOXTLS_RETURN_SUCCESS) {
            return 0;
        }
    }

    rc = noxtls_x509_certificate_load_file(cert, source);
    return (rc == NOXTLS_RETURN_SUCCESS) ? 0 : -1;
}

/**
 * @brief Loads a private key from inline PEM text or a filesystem path.
 *
 * @param[in] source PEM private key text or a readable file path.
 * @param[out] key Parsed private key object.
 *
 * @return 0 on success, otherwise `-1`.
 */
static int noxmqtt_noxtls_load_private_key_source(const char* source, x509_private_key_t* key)
{
    noxtls_return_t rc = NOXTLS_RETURN_FAILED;
    size_t source_len = 0U;

    if (source == NULL || key == NULL || source[0] == '\0') {
        return -1;
    }

    source_len = strlen(source);
    if (noxmqtt_noxtls_source_is_pem(source, "PRIVATE KEY-----")) {
        rc = noxtls_x509_private_key_parse_pem(key, (const uint8_t*)source, (uint32_t)source_len);
        if (rc == NOXTLS_RETURN_SUCCESS) {
            return 0;
        }
    }

    rc = noxtls_x509_private_key_load_file(key, source);
    return (rc == NOXTLS_RETURN_SUCCESS) ? 0 : -1;
}

/**
 * @brief Returns non-zero when the parsed certificate is usable for TLS client auth.
 *
 * @param[in] cert Parsed certificate to validate.
 *
 * @return 1 when the certificate is suitable for client auth, otherwise 0.
 */
static int noxmqtt_noxtls_certificate_supports_client_auth(const x509_certificate_t* cert)
{
    if (cert == NULL) {
        return 0;
    }

    if (cert->ext_key_usage_bits != 0U &&
        (cert->ext_key_usage_bits & X509_EKU_CLIENT_AUTH) == 0U &&
        (cert->ext_key_usage_bits & X509_EKU_ANY) == 0U) {
        return 0;
    }

    if (cert->key_usage_bits != 0U &&
        (cert->key_usage_bits & X509_KEY_USAGE_DIGITAL_SIGNATURE) == 0U) {
        return 0;
    }

    return 1;
}

/**
 * @brief Initializes the reusable client identity state.
 *
 * @param[out] identity Identity state to initialize.
 */
static void noxmqtt_noxtls_client_identity_init(noxmqtt_noxtls_client_identity_t* identity)
{
    if (identity == NULL) {
        return;
    }

    memset(identity, 0, sizeof(*identity));
    (void)noxtls_x509_certificate_init(&identity->cert);
    (void)noxtls_x509_private_key_init(&identity->key);
}

/**
 * @brief Releases all client identity resources held by the transport.
 *
 * @param[in,out] identity Identity state to release.
 */
static void noxmqtt_noxtls_client_identity_free(noxmqtt_noxtls_client_identity_t* identity)
{
    if (identity == NULL) {
        return;
    }

    if (identity->rsa_ready) {
        (void)noxtls_rsa_key_free(&identity->rsa_key);
    }

    if (identity->ecc_ready) {
        (void)noxtls_ecc_key_free(&identity->ecc_key);
    }

    if (identity->key_loaded) {
        (void)noxtls_x509_private_key_free(&identity->key);
    }

    if (identity->cert_loaded) {
        (void)noxtls_x509_certificate_free(&identity->cert);
    }

    memset(identity, 0, sizeof(*identity));
}

/**
 * @brief Loads and validates the configured client certificate and private key.
 *
 * @param[in,out] identity Identity state to populate.
 * @param[in] cert_source Client certificate PEM text or file path.
 * @param[in] key_source Client private key PEM text or file path.
 *
 * @return 0 on success, otherwise `-1`.
 */
static int noxmqtt_noxtls_client_identity_load(noxmqtt_noxtls_client_identity_t* identity,
                                               const char* cert_source,
                                               const char* key_source)
{
    if (identity == NULL) {
        return -1;
    }

    noxmqtt_noxtls_client_identity_free(identity);
    noxmqtt_noxtls_client_identity_init(identity);

    if ((cert_source == NULL || cert_source[0] == '\0') &&
        (key_source == NULL || key_source[0] == '\0')) {
        return 0;
    }

    if (cert_source == NULL || cert_source[0] == '\0' ||
        key_source == NULL || key_source[0] == '\0') {
        return -1;
    }

    if (noxmqtt_noxtls_load_certificate_source(cert_source, &identity->cert) != 0) {
        noxmqtt_noxtls_client_identity_free(identity);
        return -1;
    }
    identity->cert_loaded = 1U;

    if (!noxmqtt_noxtls_certificate_supports_client_auth(&identity->cert) ||
        identity->cert.raw_data == NULL ||
        identity->cert.raw_data_len == 0U) {
        noxmqtt_noxtls_client_identity_free(identity);
        return -1;
    }

    if (noxmqtt_noxtls_load_private_key_source(key_source, &identity->key) != 0) {
        noxmqtt_noxtls_client_identity_free(identity);
        return -1;
    }
    identity->key_loaded = 1U;
    identity->configured = 1U;

    return 0;
}

/**
 * @brief Applies the loaded client identity to a TLS 1.3 client context.
 *
 * @param[in,out] identity Loaded client identity state.
 * @param[in,out] tls13 TLS 1.3 client context.
 *
 * @return 0 on success, otherwise `-1`.
 */
static int noxmqtt_noxtls_client_identity_apply_tls13(noxmqtt_noxtls_client_identity_t* identity,
                                                      tls13_context_t* tls13)
{
    noxtls_return_t rc = NOXTLS_RETURN_SUCCESS;

    if (tls13 == NULL) {
        return -1;
    }

    if (identity == NULL || !identity->configured) {
        return 0;
    }

    if (identity->cert.raw_data == NULL || identity->cert.raw_data_len == 0U) {
        return -1;
    }

    switch (identity->key.key_type) {
        case X509_PRIVATE_KEY_RSA:
            if (!identity->rsa_ready) {
                rc = noxtls_x509_private_key_to_rsa_key(&identity->key, &identity->rsa_key);
                if (rc != NOXTLS_RETURN_SUCCESS) {
                    return -1;
                }
                identity->rsa_ready = 1U;
            }
            rc = noxtls_tls13_set_client_cert(tls13,
                                              identity->cert.raw_data,
                                              identity->cert.raw_data_len,
                                              &identity->rsa_key);
            break;

        case X509_PRIVATE_KEY_ECC:
            if (!identity->ecc_ready) {
                rc = noxtls_x509_private_key_to_ecc_key(&identity->key, &identity->ecc_key);
                if (rc != NOXTLS_RETURN_SUCCESS) {
                    return -1;
                }
                identity->ecc_ready = 1U;
            }
            rc = noxtls_tls13_set_client_cert_ecdsa(tls13,
                                                    identity->cert.raw_data,
                                                    identity->cert.raw_data_len,
                                                    &identity->ecc_key);
            break;

        case X509_PRIVATE_KEY_ED25519:
        {
            uint32_t seed_len = 0U;
            const uint8_t* seed = noxtls_x509_private_key_get_eddsa_seed(&identity->key, &seed_len);
            if (seed == NULL || seed_len != 32U) {
                return -1;
            }
            rc = noxtls_tls13_set_client_cert_ed25519(tls13,
                                                      identity->cert.raw_data,
                                                      identity->cert.raw_data_len,
                                                      seed);
            break;
        }

        case X509_PRIVATE_KEY_ED448:
        {
            uint32_t seed_len = 0U;
            const uint8_t* seed = noxtls_x509_private_key_get_eddsa_seed(&identity->key, &seed_len);
            if (seed == NULL || seed_len != 57U) {
                return -1;
            }
            rc = noxtls_tls13_set_client_cert_ed448(tls13,
                                                    identity->cert.raw_data,
                                                    identity->cert.raw_data_len,
                                                    seed);
            break;
        }

        case X509_PRIVATE_KEY_ML_DSA:
        {
            uint32_t secret_len = 0U;
            uint32_t param = 0U;
            const uint8_t* secret = noxtls_x509_private_key_get_pqc_secret(&identity->key, &secret_len, &param);
            if (secret == NULL || secret_len == 0U) {
                return -1;
            }
            rc = tls13_set_client_cert_mldsa(tls13,
                                             identity->cert.raw_data,
                                             identity->cert.raw_data_len,
                                             (noxtls_mldsa_param_t)param,
                                             secret);
            break;
        }

        case X509_PRIVATE_KEY_SLH_DSA:
        {
            uint32_t secret_len = 0U;
            uint32_t param = 0U;
            const uint8_t* secret = noxtls_x509_private_key_get_pqc_secret(&identity->key, &secret_len, &param);
            if (secret == NULL || secret_len == 0U) {
                return -1;
            }
            rc = tls13_set_client_cert_slhdsa(tls13,
                                              identity->cert.raw_data,
                                              identity->cert.raw_data_len,
                                              (noxtls_slhdsa_param_t)param,
                                              secret);
            break;
        }

        case X509_PRIVATE_KEY_FALCON:
        {
            uint32_t secret_len = 0U;
            uint32_t param = 0U;
            const uint8_t* secret = noxtls_x509_private_key_get_pqc_secret(&identity->key, &secret_len, &param);
            if (secret == NULL || secret_len == 0U) {
                return -1;
            }
            rc = tls13_set_client_cert_falcon(tls13,
                                              identity->cert.raw_data,
                                              identity->cert.raw_data_len,
                                              (noxtls_falcon_param_t)param,
                                              secret);
            break;
        }

        default:
            return -1;
    }

    return (rc == NOXTLS_RETURN_SUCCESS) ? 0 : -1;
}

#endif /* _NOXMQTT_NOXTLS_CLIENT_IDENTITY_H_ */
