# Muestras de Quarantine · eventos

Perfil `local` · lake `sandbox`

| Regla | Acción | Filas |
|---|---|---|
| `event_id_null` | BLOQUEA | 1 |
| `event_id_duplicate` | BLOQUEA | 0 |
| `value_not_castable` | BLOQUEA | 2 |
| `cost_negative` | MARCA | 9 |
| `unit_imputed` | MARCA | 78 |
| `value_null` | MARCA | 45 |

## `event_id_null`

| dq_rule       | event_id   | event_ts            | event_date   | org_id       | service   | metric    |   value | unit   |   cost_usd_increment | source_file                                | rejected_ts                |
|:--------------|:-----------|:--------------------|:-------------|:-------------|:----------|:----------|--------:|:-------|---------------------:|:-------------------------------------------|:---------------------------|
| event_id_null |            | 2025-08-10 12:00:00 | 2025-08-10   | org_xaji0y6d | compute   | cpu_hours |     2.5 | hours  |                 1.25 | usage_events_stream/fixtures_calidad.jsonl | 2026-11-10 01:01:22.135821 |

## `event_id_duplicate`

Sin filas: ningún evento viola esta regla en este lake.

## `value_not_castable`

| dq_rule            | event_id       | event_ts            | event_date   | org_id       | service   | metric    | value   | unit   |   cost_usd_increment | source_file                                | rejected_ts                |
|:-------------------|:---------------|:--------------------|:-------------|:-------------|:----------|:----------|:--------|:-------|---------------------:|:-------------------------------------------|:---------------------------|
| value_not_castable | fx_value_texto | 2025-08-11 12:00:00 | 2025-08-11   | org_xaji0y6d | compute   | cpu_hours | abc     | hours  |                 1.25 | usage_events_stream/fixtures_calidad.jsonl | 2026-11-10 01:01:22.135821 |
| value_not_castable | fx_value_coma  | 2025-08-12 12:00:00 | 2025-08-12   | org_xaji0y6d | compute   | cpu_hours | 1,5     | hours  |                 1.25 | usage_events_stream/fixtures_calidad.jsonl | 2026-11-10 01:01:22.135821 |
