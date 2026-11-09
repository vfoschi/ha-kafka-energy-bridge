#!/usr/bin/with-contenv bashio
# shellcheck shell=bash

# Legge le opzioni dell'add-on (da /data/options.json) e le esporta come env var.
export KAFKA_BOOTSTRAP="$(bashio::config 'kafka_bootstrap')"
export KAFKA_TOPIC="$(bashio::config 'kafka_topic')"
export PUBLISH_MODE="$(bashio::config 'publish_mode')"
export SNAPSHOT_INTERVAL="$(bashio::config 'snapshot_interval')"
export SECURITY_PROTOCOL="$(bashio::config 'security_protocol')"
export SASL_MECHANISM="$(bashio::config 'sasl_mechanism')"
export SASL_USERNAME="$(bashio::config 'sasl_username')"
export SASL_PASSWORD="$(bashio::config 'sasl_password')"
export LOG_LEVEL="$(bashio::config 'log_level')"

# Lista sensori -> stringa separata da virgole
export SENSORS="$(bashio::config 'sensors | join(",")')"
export CUMULATIVE_SENSORS="$(bashio::config 'cumulative_sensors | join(",")')"

# Token e URL interni del Supervisor per l'API WebSocket di Home Assistant
export HA_TOKEN="${SUPERVISOR_TOKEN}"
export HA_WS_URL="ws://supervisor/core/websocket"

bashio::log.info "Avvio Kafka Energy Bridge -> ${KAFKA_BOOTSTRAP} topic=${KAFKA_TOPIC} mode=${PUBLISH_MODE}"

exec python3 /bridge.py
