# Home Assistant Add-on: Kafka Energy Bridge

Repository di add-on per Home Assistant che pubblica i consumi energetici
(es. sensori dell'integration [ha-sinapsi-alfa](https://github.com/alexdelprete/ha-sinapsi-alfa))
su un **topic Kafka**.

## ⚠️ Questo è un ADD-ON, non un'integration HACS

HACS gestisce *integrations, temi e card*, **non gli add-on**. Questo è un add-on
Supervisor (container Docker), quindi si aggiunge come **repository di add-on**:

1. In Home Assistant: **Settings → Add-ons → Add-on Store**
2. Menu **⋮** in alto a destra → **Repositories**
3. Incolla l'URL di questo repository:

   ```
   https://github.com/vfoschi/ha-kafka-energy-bridge
   ```

4. **Add** → chiudi. Ricarica lo store: comparirà la sezione
   **"Kafka Energy Bridge Add-ons"** con l'add-on **Kafka Energy Bridge**.
5. Installa, apri **Configuration**, imposta i parametri, poi **Start**.

> Richiede Home Assistant OS o Supervised (serve il Supervisor per gli add-on).
> Su Home Assistant Container gli add-on non sono supportati: in quel caso usa
> lo script `kafka_energy_bridge/bridge.py` come container separato.

## Add-on incluso

| Add-on | Descrizione |
|---|---|
| [**Kafka Energy Bridge**](./kafka_energy_bridge) | Legge i sensori energetici via API HA e li pubblica su Kafka (modalità event o snapshot, con supporto SASL/TLS). |

La documentazione completa dei parametri è in
[`kafka_energy_bridge/DOCS.md`](./kafka_energy_bridge/DOCS.md).

## Licenza

[MIT](./LICENSE) © 2026 Vittorio Foschi
