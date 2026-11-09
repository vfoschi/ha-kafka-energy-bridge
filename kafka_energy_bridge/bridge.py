#!/usr/bin/env python3
"""
Kafka Energy Bridge per Home Assistant.

Legge gli stati dei sensori energetici (es. integration Sinapsi Alfa) tramite
l'API WebSocket interna di Home Assistant e li pubblica su un topic Kafka.

Due modalita':
  - event    : pubblica un messaggio ad ogni cambio di stato di un sensore in lista
  - snapshot : pubblica periodicamente un unico messaggio con tutti i valori insieme

Tutti i parametri arrivano da variabili d'ambiente impostate da run.sh,
che a sua volta le legge dalle opzioni dell'add-on (configurabili dalla UI di HA).

Usa kafka-python (puro Python) per evitare la compilazione di librdkafka.
"""

import os
import sys
import json
import time
import signal
import asyncio
import logging

import websockets
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --------------------------------------------------------------------------- #
# Configurazione da ambiente
# --------------------------------------------------------------------------- #
HA_WS_URL = os.environ.get("HA_WS_URL", "ws://supervisor/core/websocket")
HA_TOKEN = os.environ["HA_TOKEN"]

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "consumi-enel")
PUBLISH_MODE = os.environ.get("PUBLISH_MODE", "event").lower()
SNAPSHOT_INTERVAL = int(os.environ.get("SNAPSHOT_INTERVAL", "60"))

SECURITY_PROTOCOL = os.environ.get("SECURITY_PROTOCOL", "PLAINTEXT")
SASL_MECHANISM = os.environ.get("SASL_MECHANISM", "PLAIN")
SASL_USERNAME = os.environ.get("SASL_USERNAME", "")
SASL_PASSWORD = os.environ.get("SASL_PASSWORD", "")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "info").upper()

SENSORS = {
    s.strip()
    for s in os.environ.get("SENSORS", "").split(",")
    if s.strip()
}

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("kafka-energy-bridge")

# --------------------------------------------------------------------------- #
# Producer Kafka
# --------------------------------------------------------------------------- #
def build_producer() -> KafkaProducer:
    bootstrap = [b.strip() for b in KAFKA_BOOTSTRAP.split(",") if b.strip()]
    conf = {
        "bootstrap_servers": bootstrap,
        "client_id": "ha-kafka-energy-bridge",
        "security_protocol": SECURITY_PROTOCOL,
        "acks": "all",
        "retries": 5,
        "value_serializer": lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        "key_serializer": lambda k: k.encode("utf-8") if isinstance(k, str) else k,
    }
    if SECURITY_PROTOCOL in ("SASL_PLAINTEXT", "SASL_SSL"):
        conf["sasl_mechanism"] = SASL_MECHANISM
        conf["sasl_plain_username"] = SASL_USERNAME
        conf["sasl_plain_password"] = SASL_PASSWORD
    log.info("Kafka producer: %s (%s)", bootstrap, SECURITY_PROTOCOL)
    return KafkaProducer(**conf)


def get_producer() -> KafkaProducer:
    """Crea il producer con retry: non fa crashare l'add-on se Kafka non e' ancora pronto."""
    global producer
    while _running:
        try:
            producer = build_producer()
            return producer
        except KafkaError as exc:
            log.error("Kafka non raggiungibile (%s): nuovo tentativo tra 10s", exc)
            time.sleep(10)
    return None


producer = None
_running = True


def publish(key: str, value: dict) -> None:
    if producer is None:
        log.warning("Producer non pronto, messaggio %s scartato", key)
        return
    try:
        future = producer.send(KAFKA_TOPIC, key=key, value=value)
        future.add_errback(lambda e: log.error("Delivery fallita %s: %s", key, e))
        log.debug("Inviato %s -> %s", key, KAFKA_TOPIC)
    except KafkaError as exc:
        log.error("Errore invio Kafka %s: %s", key, exc)


# --------------------------------------------------------------------------- #
# Client WebSocket Home Assistant
# --------------------------------------------------------------------------- #
async def ha_connect():
    ws = await websockets.connect(HA_WS_URL, max_size=4 * 1024 * 1024)
    await ws.recv()  # auth_required
    await ws.send(json.dumps({"type": "auth", "access_token": HA_TOKEN}))
    auth = json.loads(await ws.recv())
    if auth.get("type") != "auth_ok":
        raise RuntimeError(f"Auth HA fallita: {auth}")
    log.info("Autenticato su Home Assistant WebSocket")
    return ws


async def get_states(ws, msg_id: int) -> dict:
    await ws.send(json.dumps({"id": msg_id, "type": "get_states"}))
    while True:
        msg = json.loads(await ws.recv())
        if msg.get("id") == msg_id and msg.get("type") == "result":
            return {s["entity_id"]: s for s in msg.get("result", [])}


def state_payload(state: dict) -> dict:
    attrs = state.get("attributes", {})
    return {
        "entity_id": state["entity_id"],
        "state": state.get("state"),
        "unit": attrs.get("unit_of_measurement"),
        "friendly_name": attrs.get("friendly_name"),
        "last_updated": state.get("last_updated"),
    }


# --------------------------------------------------------------------------- #
# Modalita' EVENT
# --------------------------------------------------------------------------- #
async def run_event_mode():
    while _running:
        try:
            ws = await ha_connect()
            await ws.send(json.dumps({
                "id": 1, "type": "subscribe_events", "event_type": "state_changed",
            }))
            log.info("Sottoscritto a state_changed. Sensori monitorati: %s",
                     ", ".join(sorted(SENSORS)) or "(nessuno!)")
            async for raw in ws:
                if not _running:
                    break
                msg = json.loads(raw)
                if msg.get("type") != "event":
                    continue
                data = msg["event"]["data"]
                eid = data.get("entity_id")
                new = data.get("new_state")
                if eid in SENSORS and new:
                    publish(eid, state_payload(new))
        except Exception as exc:  # noqa: BLE001
            log.error("Errore WS (event): %s - riconnessione tra 5s", exc)
            await asyncio.sleep(5)


# --------------------------------------------------------------------------- #
# Modalita' SNAPSHOT
# --------------------------------------------------------------------------- #
async def run_snapshot_mode():
    while _running:
        try:
            ws = await ha_connect()
            mid = 100
            log.info("Snapshot ogni %ds. Sensori: %s",
                     SNAPSHOT_INTERVAL, ", ".join(sorted(SENSORS)) or "(nessuno!)")
            while _running:
                mid += 1
                states = await get_states(ws, mid)
                snap = {
                    "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "values": {},
                }
                for eid in SENSORS:
                    st = states.get(eid)
                    if st:
                        snap["values"][eid] = state_payload(st)
                if snap["values"]:
                    publish("snapshot", snap)
                    log.debug("Snapshot pubblicato (%d valori)", len(snap["values"]))
                else:
                    log.warning("Nessun sensore trovato per lo snapshot")
                await asyncio.sleep(SNAPSHOT_INTERVAL)
        except Exception as exc:  # noqa: BLE001
            log.error("Errore WS (snapshot): %s - riconnessione tra 5s", exc)
            await asyncio.sleep(5)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def _handle_sigterm(*_):
    global _running
    log.info("Arresto richiesto, flush producer...")
    _running = False
    try:
        producer.flush(10)
        producer.close(10)
    except Exception:  # noqa: BLE001
        pass
    sys.exit(0)


def main():
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    if not SENSORS:
        log.error("Nessun sensore configurato nell'opzione 'sensors'. Esco.")
        sys.exit(1)

    get_producer()  # attende che Kafka sia raggiungibile prima di partire

    if PUBLISH_MODE == "snapshot":
        asyncio.run(run_snapshot_mode())
    else:
        asyncio.run(run_event_mode())


if __name__ == "__main__":
    main()
