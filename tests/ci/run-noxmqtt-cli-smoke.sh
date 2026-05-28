#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <tcp|tls|mtls> <noxmqtt-binary>" >&2
    exit 1
fi

mode="$1"
binary="$2"
log_dir="${NOXMQTT_CI_LOG_DIR:-tests/ci/logs}"

mkdir -p "$log_dir"

topic="noxmqtt/ci/${mode}/$$/$(date +%s)"
payload="ci-${mode}-payload"
sub_log="$log_dir/sub-${mode}.log"
pub_log="$log_dir/pub-${mode}.log"
version_log="$log_dir/version-${mode}.log"

common_args=(
    --protocol-version 5
    --debug info
)

transport_args=()

case "$mode" in
    tcp)
        transport_args=(
            --tcp
            --host 127.0.0.1
            --port 1883
        )
        ;;
    tls)
        transport_args=(
            --tls
            --host localhost
            --port 8883
            --cafile tests/tls/ca.crt
            --sni localhost
            --verify-peer 1
            --verify-hostname 1
        )
        ;;
    mtls)
        transport_args=(
            --tls
            --host localhost
            --port 8884
            --cafile tests/tls/ca.crt
            --cert tests/tls/client.crt
            --key tests/tls/client.key
            --sni localhost
            --verify-peer 1
            --verify-hostname 1
        )
        ;;
    *)
        echo "Unsupported mode: $mode" >&2
        exit 1
        ;;
esac

"$binary" --version >"$version_log" 2>&1

"$binary" sub "${common_args[@]}" "${transport_args[@]}" \
    -t "$topic" \
    --count 1 \
    --wait-ms 5000 >"$sub_log" 2>&1 &
sub_pid=$!

cleanup() {
    if kill -0 "$sub_pid" >/dev/null 2>&1; then
        kill "$sub_pid" >/dev/null 2>&1 || true
        wait "$sub_pid" >/dev/null 2>&1 || true
    fi
}

trap cleanup EXIT

sleep 1

"$binary" pub "${common_args[@]}" "${transport_args[@]}" \
    -t "$topic" \
    -m "$payload" >"$pub_log" 2>&1

wait "$sub_pid"
trap - EXIT

grep -F "message topic=$topic payload=$payload" "$sub_log" >/dev/null
