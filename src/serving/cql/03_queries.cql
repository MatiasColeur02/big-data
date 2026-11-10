-- Consultas mínimas de la segunda entrega (consigna §6.2, §7.4), sobre las tablas de 02_tables.cql.
-- Cada consulta toca una sola partición (org_id) y no usa ALLOW FILTERING (D8).
--   make queries                              cqlsh en Docker → evidence/entrega2/serving/cqlsh_queries.txt
--   python -m src.serving.queries             cualquier perfil → evidence/entrega2/serving/queries.md
-- Valores de ejemplo del dataset provisto: org_53lc58dr usa los 6 servicios y es la de mayor costo
-- entre las que los usan todos; as_of_date = 2025-08-31 (D15).
USE cloud_analytics;

-- P1 · Costos y requests diarios por organización y servicio, última semana (25/08 → 31/08)
SELECT org_id, usage_date, service, daily_cost_usd, requests, events
FROM org_daily_usage_by_service
WHERE org_id = 'org_53lc58dr'
  AND usage_date >= '2025-08-25' AND usage_date <= '2025-08-31';

-- P2 · Top-5 servicios por costo acumulado en los últimos 14 días (18/08 → 31/08)
SELECT org_id, service, total_cost_usd, total_requests, active_days, window_start, as_of_date
FROM org_service_cost_14d
WHERE org_id = 'org_53lc58dr'
LIMIT 5;
