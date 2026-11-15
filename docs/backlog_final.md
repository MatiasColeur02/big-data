# Backlog final

Trabajo restante para la entrega final del 07/12/2026, priorizado como obligatorio, deseable y fuera de alcance. Es lo que pide la consigna como "Backlog final".

---

## 1. Obligatorio

| # | Ítem |
|---|---|
| 1 | Silver de `billing_monthly` con la regla de FX en USD (D7). `apply_billing_rules` ya existe, falta llamarla |
| 2 | Silver de `support_tickets` con la regla de CSAT en [1,5] (D7) |
| 3 | Mart `revenue_by_org_month`: revenue en USD, créditos, impuestos y FX aplicado |
| 4 | Mart `tickets_by_org_date`: conteos, tasa de SLA breach y CSAT promedio |
| 5 | Mart `genai_tokens_by_org_date`: tokens y costo de GenAI por día, desde `usage_daily_features` |
| 6 | Tablas y carga en Cassandra para P3, P4 y P5 (D8) |
| 7 | P3, P4 y P5 en `03_queries.cql`, con salida en Cassandra y en Astra |
| 8 | Tests de las reglas y los marts nuevos |
| 9 | Sumar las tablas nuevas a la prueba de idempotencia y volver a correrla |
| 10 | Corrida en Colab real y capturas de las consultas en la consola de Astra |
| 11 | Evidencia de near real-time (O7) con `stream_events --demo` |
| 12 | Diccionario de datos |
| 13 | Arquitectura final con los cinco marts |
| 14 | Gobierno completo: seguridad y observabilidad |
| 15 | Quickstart validado desde un clon limpio por alguien que no escribió el código |
| 16 | Presentación ejecutiva |
| 17 | Video explicativo |
| 18 | Ensayo de la defensa, con plan alternativo si falla Astra |
| 19 | Revisión de credenciales y tag `v3.0-final` |

---

## 2. Deseable

| # | Ítem |
|---|---|
| 1 | Gráficos en notebook: costo diario, Top-N y anomalías |
| 2 | Publicar `cost_anomaly_mart` en Cassandra |
| 3 | Sensibilidad del umbral de anomalías (3,0 / 3,5 / 4,0) |
| 4 | CI con GitHub Actions que corra `pytest` |
| 5 | Enmascarar `users.email` en Silver |
| 6 | Alertas a partir de `runs.jsonl` |
| 7 | Reproceso incremental por fecha en Silver (D16) |

---

## 3. Fuera de alcance

| Ítem | Por qué |
|---|---|
| Airflow u otro orquestador | `src/pipeline.py` ya corre las etapas en orden y se detiene ante un error |
| Kafka | La fuente es un directorio de archivos re-leíble |
| Kappa | Obliga a convertir 7 CSV estáticos en streams (D1) |
| SCD tipo 2 en `dim_org` | `customers_orgs.csv` no tiene historia de cambios |
| Metastore o catálogo | No persiste en Colab; lo reemplazan las rutas y el diccionario (D11) |
| Delta Lake o Iceberg | La consigna pide Parquet y el file sink ya es exactly-once (D13) |
| Spark 4.x | El connector de Cassandra es estable para 3.5 (D2) |
| Lake en Drive | Sin renames atómicos rompe el checkpoint (D22) |
| ML de churn | El componente analítico es anomalías con MAD (D9) |
