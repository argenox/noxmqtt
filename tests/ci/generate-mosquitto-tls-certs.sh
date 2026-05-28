#!/usr/bin/env bash
set -euo pipefail

cert_dir="${1:-tests/tls}"

rm -rf "$cert_dir"
mkdir -p "$cert_dir"

cat >"$cert_dir/server.ext" <<'EOF'
subjectAltName = DNS:localhost,IP:127.0.0.1
basicConstraints = CA:FALSE
extendedKeyUsage = serverAuth
keyUsage = digitalSignature,keyEncipherment
EOF

cat >"$cert_dir/client.ext" <<'EOF'
basicConstraints = CA:FALSE
extendedKeyUsage = clientAuth
keyUsage = digitalSignature
EOF

openssl req -x509 -newkey rsa:2048 -sha256 -days 7 -nodes \
    -subj "/CN=NoxMQTT Test CA" \
    -keyout "$cert_dir/ca.key" \
    -out "$cert_dir/ca.crt"

openssl req -newkey rsa:2048 -sha256 -nodes \
    -subj "/CN=localhost" \
    -keyout "$cert_dir/server.key" \
    -out "$cert_dir/server.csr"

openssl x509 -req -sha256 -days 7 \
    -in "$cert_dir/server.csr" \
    -CA "$cert_dir/ca.crt" \
    -CAkey "$cert_dir/ca.key" \
    -CAcreateserial \
    -out "$cert_dir/server.crt" \
    -extfile "$cert_dir/server.ext"

openssl req -newkey rsa:2048 -sha256 -nodes \
    -subj "/CN=noxmqtt-client" \
    -keyout "$cert_dir/client.key" \
    -out "$cert_dir/client.csr"

openssl x509 -req -sha256 -days 7 \
    -in "$cert_dir/client.csr" \
    -CA "$cert_dir/ca.crt" \
    -CAkey "$cert_dir/ca.key" \
    -CAcreateserial \
    -out "$cert_dir/client.crt" \
    -extfile "$cert_dir/client.ext"

chmod 600 "$cert_dir/ca.key" "$cert_dir/server.key" "$cert_dir/client.key"
