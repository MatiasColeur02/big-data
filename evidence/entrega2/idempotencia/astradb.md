# Prueba de idempotencia (O6)

Provider `astradb` · keyspace `cloud_analytics` · perfil `cloud` · 2026-11-10T18:11:13+00:00

Generado por `python -m src.idempotency`. Corre el pipeline completo tres veces sobre el mismo lake y el mismo serving: (1) `clean` + pipeline, (2) pipeline sin limpiar, (3) `clean` + pipeline. La foto previa es informativa.

| Métrica | Antes | Corrida 1 · desde cero | Corrida 2 · sin limpiar | Corrida 3 · reproceso total |
|---|---:|---:|---:|---:|
| lake · bronze.billing_monthly · filas | 240 | 240 | 240 | 240 |
| lake · bronze.customers_orgs · filas | 80 | 80 | 80 | 80 |
| lake · bronze.marketing_touches · filas | 1500 | 1500 | 1500 | 1500 |
| lake · bronze.nps_surveys · filas | 92 | 92 | 92 | 92 |
| lake · bronze.resources · filas | 400 | 400 | 400 | 400 |
| lake · bronze.support_tickets · filas | 1000 | 1000 | 1000 | 1000 |
| lake · bronze.usage_events_stream · filas | 43200 | 43200 | 43200 | 43200 |
| lake · bronze.users · filas | 800 | 800 | 800 | 800 |
| lake · quarantine.billing_monthly · filas | 0 | 0 | 0 | 0 |
| lake · quarantine.customers_orgs · filas | 0 | 0 | 0 | 0 |
| lake · quarantine.marketing_touches · filas | 0 | 0 | 0 | 0 |
| lake · quarantine.nps_surveys · filas | 0 | 0 | 0 | 0 |
| lake · quarantine.resources · filas | 0 | 0 | 0 | 0 |
| lake · quarantine.support_tickets · filas | 0 | 0 | 0 | 0 |
| lake · quarantine.usage_events_stream · filas | 0 | 0 | 0 | 0 |
| lake · quarantine.users · filas | 0 | 0 | 0 | 0 |
| lake · silver.dim_org · filas | 80 | 80 | 80 | 80 |
| lake · silver.dim_resource · filas | 400 | 400 | 400 | 400 |
| lake · silver.usage_daily_features · filas | 11050 | 11050 | 11050 | 11050 |
| lake · silver.usage_events · filas | 43200 | 43200 | 43200 | 43200 |
| lake · gold.cost_anomaly_mart · filas | 11050 | 11050 | 11050 | 11050 |
| lake · gold.org_daily_usage_by_service · filas | 11050 | 11050 | 11050 | 11050 |
| lake · gold.org_service_cost_14d · filas | 262 | 262 | 262 | 262 |
| duplicados · bronze.usage_events_stream | 0 | 0 | 0 | 0 |
| duplicados · silver.usage_events | 0 | 0 | 0 | 0 |
| duplicados · silver.usage_daily_features | 0 | 0 | 0 | 0 |
| duplicados · gold.org_daily_usage_by_service | 0 | 0 | 0 | 0 |
| duplicados · gold.org_service_cost_14d | 0 | 0 | 0 | 0 |
| cassandra · org_daily_usage_by_service · rows | 11050 | 11050 | 11050 | 11050 |
| cassandra · org_daily_usage_by_service · sum_daily_cost_usd | 147433.9778 | 147433.9778 | 147433.9778 | 147433.9778 |
| cassandra · org_service_cost_14d · rows | 262 | 262 | 262 | 262 |
| cassandra · org_service_cost_14d · sum_total_cost_usd | 33998.5709 | 33998.5709 | 33998.5709 | 33998.5709 |

## Veredicto

- OK · la corrida 2 (sin limpiar) deja las mismas filas que la 1
- OK · la corrida 3 (reprocesamiento total) deja las mismas filas que la 1
- OK · 0 claves duplicadas en Bronze, Silver, Gold y en el Top-N
