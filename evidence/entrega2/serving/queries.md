# Consultas sobre el serving · segunda entrega

Provider `cassandra` · keyspace `cloud_analytics` · perfil `local` · 2026-11-10T18:01:44+00:00

Generado por `python -m src.serving.queries` a partir de [`03_queries.cql`](../../../src/serving/cql/03_queries.cql). La salida de `cqlsh` está en [`cqlsh_queries.txt`](cqlsh_queries.txt) (`make queries`).

## P1 · Costos y requests diarios por organización y servicio, última semana (25/08 → 31/08)

**Query-first:** Partition key `org_id` y clustering `usage_date DESC, service`: el rango de fechas es un slice contiguo dentro de una sola partición. Sin `ALLOW FILTERING` ni índices secundarios.

```sql
SELECT org_id, usage_date, service, daily_cost_usd, requests, events
FROM org_daily_usage_by_service
WHERE org_id = 'org_53lc58dr'
  AND usage_date >= '2025-08-25' AND usage_date <= '2025-08-31';
```

**Resultado:** 34 fila(s)

| org_id       | usage_date   | service    |   daily_cost_usd |   requests |   events |
|:-------------|:-------------|:-----------|-----------------:|-----------:|---------:|
| org_53lc58dr | 2025-08-31   | analytics  |          47.3679 |        604 |        6 |
| org_53lc58dr | 2025-08-31   | compute    |           1.0125 |          0 |        6 |
| org_53lc58dr | 2025-08-31   | database   |           1.7011 |          0 |        3 |
| org_53lc58dr | 2025-08-31   | genai      |          15.1432 |        112 |        3 |
| org_53lc58dr | 2025-08-31   | storage    |           2.6436 |        125 |        3 |
| org_53lc58dr | 2025-08-30   | compute    |          11.3755 |        112 |        6 |
| org_53lc58dr | 2025-08-30   | database   |          19.2887 |        365 |        6 |
| org_53lc58dr | 2025-08-30   | genai      |          33.0729 |        243 |        3 |
| org_53lc58dr | 2025-08-30   | networking |           3.405  |        321 |        3 |
| org_53lc58dr | 2025-08-30   | storage    |           2.6326 |        114 |        3 |
| org_53lc58dr | 2025-08-29   | analytics  |          17.2923 |        234 |        3 |
| org_53lc58dr | 2025-08-29   | compute    |           0.4647 |          0 |        3 |
| org_53lc58dr | 2025-08-29   | database   |          10.3658 |        201 |        3 |
| org_53lc58dr | 2025-08-29   | networking |           1.2445 |        109 |        3 |
| org_53lc58dr | 2025-08-28   | analytics  |          14.5366 |        223 |        3 |
| org_53lc58dr | 2025-08-28   | compute    |          28.9662 |        370 |        6 |
| org_53lc58dr | 2025-08-28   | database   |          22.1574 |        374 |        6 |
| org_53lc58dr | 2025-08-28   | genai      |          14.9901 |        120 |        3 |
| org_53lc58dr | 2025-08-28   | networking |           2.4788 |        240 |        3 |
| org_53lc58dr | 2025-08-28   | storage    |           4.9234 |        238 |        3 |
| org_53lc58dr | 2025-08-27   | analytics  |          18.0925 |        226 |        3 |
| org_53lc58dr | 2025-08-27   | compute    |          42.1894 |        516 |        6 |
| org_53lc58dr | 2025-08-27   | database   |          10.6345 |        238 |        3 |
| org_53lc58dr | 2025-08-27   | genai      |          43.6883 |        349 |        3 |
| org_53lc58dr | 2025-08-27   | storage    |           0.2769 |          0 |        3 |
| org_53lc58dr | 2025-08-26   | analytics  |          30.2047 |        335 |        6 |
| org_53lc58dr | 2025-08-26   | compute    |          18.5113 |        225 |        3 |
| org_53lc58dr | 2025-08-26   | database   |          24.9761 |        355 |        6 |
| org_53lc58dr | 2025-08-26   | genai      |           1.8175 |          0 |        3 |
| org_53lc58dr | 2025-08-26   | networking |           1.192  |        122 |        3 |
| org_53lc58dr | 2025-08-26   | storage    |           2.7482 |        121 |        3 |
| org_53lc58dr | 2025-08-25   | analytics  |          22.3512 |        340 |        6 |
| org_53lc58dr | 2025-08-25   | database   |           6.2304 |        107 |        3 |
| org_53lc58dr | 2025-08-25   | networking |           2.5736 |        248 |        3 |

## P2 · Top-5 servicios por costo acumulado en los últimos 14 días (18/08 → 31/08)

**Query-first:** Spark pre-agrega la ventana de 14 días y el total va como clustering key `DESC`: `LIMIT N` devuelve el Top-N ya ordenado desde una sola partición (D8, D14, D15).

```sql
SELECT org_id, service, total_cost_usd, total_requests, active_days, window_start, as_of_date
FROM org_service_cost_14d
WHERE org_id = 'org_53lc58dr'
LIMIT 5;
```

**Resultado:** 5 fila(s)

| org_id       | service   |   total_cost_usd |   total_requests |   active_days | window_start   | as_of_date   |
|:-------------|:----------|-----------------:|-----------------:|--------------:|:---------------|:-------------|
| org_53lc58dr | analytics |          282.075 |             3671 |            13 | 2025-08-18     | 2025-08-31   |
| org_53lc58dr | genai     |          271.274 |             2016 |            11 | 2025-08-18     | 2025-08-31   |
| org_53lc58dr | compute   |          237.477 |             2807 |            12 | 2025-08-18     | 2025-08-31   |
| org_53lc58dr | database  |          182.779 |             3103 |            14 | 2025-08-18     | 2025-08-31   |
| org_53lc58dr | storage   |           36.266 |             1556 |            11 | 2025-08-18     | 2025-08-31   |
