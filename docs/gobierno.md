# Gobierno preliminar

Controles iniciales, metadatos, linaje, responsabilidades, acceso y evidencias. Es lo que pide la consigna como "Gobierno preliminar" para la segunda entrega.

---

## 1. Responsabilidades

Cada zona tiene un rol dueño, que decide su esquema y sus reglas y responde si un control falla. Los roles son los de [`plan_inicial.md`](plan_inicial.md) §3.

| Zona                           | Rol dueño       | Escribe                            | Control a cargo                                        |
|--------------------------------|-----------------|------------------------------------|--------------------------------------------------------|
| Landing                        | Datos y calidad | Nadie                              | Que no se modifique                                    |
| Bronze                         | Data Lake       | `src/ingest/`                      | Landing = Bronze (O2, O3)                              |
| Quarantine                     | Datos y calidad | `src/quality/`, `batch_masters.py` | Que cada rechazo tenga regla, fecha y origen           |
| Silver                         | Procesamiento   | `src/silver/`                      | `bronze = silver + quarantine` por fecha (O4)          |
| Gold                           | Procesamiento   | `src/gold/`                        | Grano de los marts y `as_of_date`                      |
| Serving                        | Arquitectura    | `src/serving/load_cassandra.py`    | Gold = Cassandra                                       |
| `_checkpoints/` y `_metadata/` | Data Lake       | Spark y `track()`                  | Que el checkpoint se borre solo junto con Bronze (D13) |

Un cambio de esquema, de regla o de clave en Cassandra se registra primero en [`decisions.md`](decisions.md).

---

## 2. Controles de calidad

`BLOQUEA` manda la fila a Quarantine. `MARCA` la deja pasar a Silver con el id de la regla en `dq_flags`.

| Se espera                                  | Si no se cumple               | Dónde                         | Filas que no cumplen (lake) | Filas que no cumplen (sandbox, D20) |
|--------------------------------------------|-------------------------------|-------------------------------|-----------------------------|-------------------------------------|
| `event_id` no nulo                         | BLOQUEA                       | `src/quality/rules.py`        | 0                           | 1                                   |
| `event_id` único                           | BLOQUEA                       | `src/quality/rules.py`        | 0                           | cubierta por pytest                 |
| `value` casteable a double                 | BLOQUEA                       | `src/quality/rules.py`        | 0                           | 2                                   |
| `cost_usd_increment >= -0.01`              | MARCA y prende `cost_anomaly` | `src/quality/rules.py`        | 211                         | 9                                   |
| `unit` no nula si hay `value`              | MARCA e imputa                | `src/quality/rules.py`        | 2.038                       | 78                                  |
| `value` no nulo                            | MARCA, sin imputar (D19)      | `src/quality/rules.py`        | 877                         | 45                                  |
| `nps_score ∈ [-100,100]`                   | MARCA y anula                 | `src/silver/dims.py`          | 1                           | —                                   |
| PK única en maestros                       | BLOQUEA                       | `src/ingest/batch_masters.py` | 0                           | —                                   |
| CSV con el esquema y la cabecera esperados | Falla el job (`FAILFAST`)     | `src/ingest/batch_masters.py` | —                           | —                                   |

En el lake principal Quarantine queda vacía porque el dataset no tiene violaciones de las reglas `BLOQUEA`, igual que en el perfilado. Las reglas se prueban con fixtures en un sandbox aparte (D20).

Controles por zona:

| Control                                         | Resultado        |
|-------------------------------------------------|------------------|
| Eventos Landing = Bronze                        | 43.200 de 43.200 |
| Filas de maestros Landing = Bronze              | 4.112 de 4.112   |
| `bronze = silver + quarantine` por `event_date` | 60 de 60 fechas  |
| Costo total Silver = Landing                    | 147.433,98 USD   |
| Eventos huérfanos en el join                    | 0                |

Quedan para la entrega final las reglas de FX en billing y de CSAT en tickets.

---

## 3. Metadatos

Tres niveles, según D11.

| Nivel                                           | Dónde vive                                  |
|-------------------------------------------------|---------------------------------------------|
| Registro: de dónde vino cada fila y qué le pasó | Columnas técnicas en el Parquet             |
| Esquema: columnas, tipos y nulabilidad          | `src/common/schemas.py` y footer de Parquet |
| Corrida: qué leyó, escribió y rechazó cada job  | `datalake/_metadata/runs/runs.jsonl`        |

| Zona       | Columnas técnicas                                                 |
|------------|-------------------------------------------------------------------|
| Bronze     | `ingest_ts`, `source_file`, `schema_version` en eventos           |
| Silver     | Las de Bronze, `dq_flags` y los `*_raw` de los valores corregidos |
| Quarantine | Las de Bronze, `dq_rule`, `rejected_ts` y `event_date`            |
| Gold       | `processed_ts`; el Top-N suma `as_of_date` y `window_start`       |

Cada línea del registro de corridas tiene `run_id`, `job`, `profile`, zona de origen y destino, inicio, duración, estado, error, filas leídas, escritas y rechazadas, y particiones escritas. Si el job falla, la línea se escribe igual con `status="failed"`.

---

## 4. Linaje

**Por fila**, de Landing a Silver y Quarantine: `source_file` se propaga sin cambios.

**Por tabla**, de Silver a Gold y Cassandra: una fila de Gold agrega eventos de varios archivos, así que el linaje lo da el registro de corridas.

```
landing/usage_events_stream/*.jsonl ─ bronze_stream ──► bronze/usage_events_stream
landing/*.csv ────────────────────── bronze_masters ─► bronze/<maestro>
bronze/usage_events_stream ───────── quality ────────► quarantine/usage_events_stream
bronze/* ─────────────────────────── silver ─────────► silver/usage_events · dim_org · dim_resource · usage_daily_features
silver/usage_daily_features ──────── gold ───────────► gold/org_daily_usage_by_service · org_service_cost_14d · cost_anomaly_mart
gold/org_daily_usage_by_service ──── serving ────────► cloud_analytics.org_daily_usage_by_service
gold/org_service_cost_14d ────────── serving ────────► cloud_analytics.org_service_cost_14d
```

---

## 5. Acceso y secretos

| Control | Implementación |
|---|---|
| Ninguna credencial en el repo | `.gitignore` excluye `.env`, `config/pipeline.*.yaml`, `secure-connect-*.zip`, `*.pem` y `**/token*.json` |
| Token de Astra solo por variable de entorno | `ASTRA_DB_TOKEN`, desde `.env` en Docker o desde Secrets en Colab (D17, D22) |
| El YAML no admite secretos escritos | `src/common/config.py` falla si `token`, `password` o `secret` tienen un valor literal |
| Notebook sin salidas versionadas | `tests/test_notebooks.py` |
| Landing de solo lectura | Ningún job escribe ahí, y `make clean` se niega a borrar una ruta que se superponga con Landing |
| Una zona, un job que la escribe | Las rutas salen de la config, no del código (D17) |
| Cassandra local | Sin autenticación, publicada solo en `127.0.0.1:9042` |
| AstraDB | Token y secure connect bundle; el keyspace se crea desde la consola |

El único dato personal del dataset es `users.email`, sintético (`<user_id>@example.com`), y no llega a Gold ni a Cassandra.

---

## 6. Evidencias

| Control | Archivo |
|---|---|
| Reglas y Quarantine | [`quality/`](../evidence/entrega2/quality/) · [`quality_sandbox/`](../evidence/entrega2/quality_sandbox/) |
| Registro de corridas, log, rutas y tamaños | [`ejecucion/`](../evidence/entrega2/ejecucion/) |
| Idempotencia y Gold = Cassandra | [`idempotencia/`](../evidence/entrega2/idempotencia/) |
| Anomalías | [`gold/anomalies.md`](../evidence/entrega2/gold/anomalies.md) |
