# Kafka Energy Bridge — Home Assistant Add-on

Add-on che legge i sensori energetici di Home Assistant (es. quelli esposti
dall'integration [ha-sinapsi-alfa](https://github.com/alexdelprete/ha-sinapsi-alfa))
e **pubblica i consumi su un topic Kafka**, senza modificare l'integration originale.

## Perché così

L'integration `ha-sinapsi-alfa` fa un solo lavoro: leggere l'Alfa via Modbus ed
esporre sensori. Invece di forkarla, questo add-on lavora **a valle**: legge quei
sensori tramite l'API interna di Home Assistant e li inoltra a Kafka. Puoi
aggiornare l'integration liberamente; il bridge resta indipendente.

## Installazione

1. Copia la cartella `kafka_energy_bridge` in `/addons/` della tua installazione
   Home Assistant (via add-on *Samba share* o *SSH & Web Terminal*).
   > La cartella deve chiamarsi come lo `slug` (`kafka_energy_bridge`).
2. In HA: **Settings → Add-ons → Add-on Store → ⋮ (in alto a dx) → Check for updates**.
3. Trovi **"Kafka Energy Bridge"** in fondo (sezione *Local add-ons*). Installalo.
4. Apri la tab **Configuration** e imposta i parametri (vedi sotto).
5. Avvia l'add-on. Controlla la tab **Log**.

## Parametri (tutti dalla UI, tab Configuration)

| Opzione | Descrizione | Esempio |
|---|---|---|
| `kafka_bootstrap` | host:porta dei broker Kafka | `10.0.0.5:9092` |
| `kafka_topic` | topic di destinazione | `consumi-enel` |
| `publish_mode` | `event` (un msg per cambio sensore) o `snapshot` (tutti i valori periodicamente) | `event` |
| `snapshot_interval` | secondi tra snapshot (solo modalità snapshot) | `60` |
| `sensors` | lista degli `entity_id` da pubblicare | vedi nota |
| `security_protocol` | `PLAINTEXT` / `SASL_PLAINTEXT` / `SASL_SSL` / `SSL` | `PLAINTEXT` |
| `sasl_mechanism` | `PLAIN` / `SCRAM-SHA-256` / `SCRAM-SHA-512` | `PLAIN` |
| `sasl_username` | utente SASL (se autenticazione attiva) | |
| `sasl_password` | password SASL (campo mascherato) | |
| `log_level` | `debug` / `info` / `warning` / `error` | `info` |

### Nota sui nomi dei sensori

Gli `entity_id` di default assumono device name `alfa`. **Verifica i tuoi reali**
in *Developer Tools → States* cercando `potenza` o `energia`, e aggiorna la lista.
Sensori tipici dell'integration Alfa:

- `sensor.alfa_potenza_prelevata` — potenza prelevata dalla rete
- `sensor.alfa_potenza_immessa` — potenza immessa in rete (FV)
- `sensor.alfa_potenza_prodotta` — produzione fotovoltaica
- `sensor.alfa_potenza_consumata` — consumo totale casa
- `sensor.alfa_energia_prelevata` — energia prelevata (cumulativa, kWh)
- `sensor.alfa_energia_prodotta` — energia prodotta (cumulativa, kWh)

## Formato dei messaggi

**Modalità `event`** — key = `entity_id`:

```json
{
  "entity_id": "sensor.alfa_potenza_prelevata",
  "state": "1.234",
  "unit": "kW",
  "friendly_name": "Alfa Potenza Prelevata",
  "last_updated": "2026-11-09T15:30:00.123456+00:00"
}
```

**Modalità `snapshot`** — key = `snapshot`:

```json
{
  "ts": "2026-11-09T15:30:00+0100",
  "values": {
    "sensor.alfa_potenza_prelevata": { "state": "1.234", "unit": "kW", ... },
    "sensor.alfa_energia_prelevata": { "state": "4567.8", "unit": "kWh", ... }
  }
}
```

## Come funziona

- Si autentica sull'API WebSocket interna di HA usando il `SUPERVISOR_TOKEN`
  (nessuna password da configurare lato HA).
- In modalità `event` si sottoscrive a `state_changed` e filtra la lista `sensors`.
- In modalità `snapshot` interroga `get_states` ogni `snapshot_interval` secondi.
- Pubblica con client Kafka nativo (`kafka-python`), con supporto SASL/TLS,
  idempotenza e riconnessione automatica.

## Troubleshooting

- **Nessun messaggio** → controlla i Log dell'add-on; verifica che gli `entity_id`
  esistano in *Developer Tools → States*.
- **Timeout Kafka** → `kafka_bootstrap` raggiungibile dalla rete dell'host HA?
- **Auth Kafka fallita** → verifica `security_protocol`, meccanismo e credenziali.
