# Perfilado de Landing · evidencia

Salida de `notebooks/01_profiling_landing.ipynb`. Primera entrega · Cloud Provider Analytics · ITBA Big Data 2C 2026.

Regenerar con: `jupyter nbconvert --execute --to notebook --inplace notebooks/01_profiling_landing.ipynb`

---
### Inventario

| fuente                      |   filas |   columnas | PK                 |   duplicados_PK |      KB |
|:----------------------------|--------:|-----------:|:-------------------|----------------:|--------:|
| billing_monthly.csv         |     240 |          8 | invoice_id         |               0 |    15.5 |
| customers_orgs.csv          |      80 |         11 | org_id             |               0 |     7.6 |
| marketing_touches.csv       |    1500 |          7 | touch_id           |               0 |    99.6 |
| nps_surveys.csv             |      92 |          4 | org_id+survey_date |               0 |     3.9 |
| resources.csv               |     400 |          7 | resource_id        |               0 |    35.8 |
| support_tickets.csv         |    1000 |          8 | ticket_id          |               0 |    69.3 |
| users.csv                   |     800 |          7 | user_id            |               0 |    69   |
| usage_events_stream/*.jsonl |   43200 |         13 | event_id           |             nan | 12624.5 |

Total: 47,312 filas · 12.6 MB

### Nulos por columna

- **billing_monthly.csv** — `credits` 137 (57.1%)
- **customers_orgs.csv** — `nps_score` 11 (13.8%)
- **marketing_touches.csv** — sin nulos
- **nps_surveys.csv** — `nps_score` 19 (20.7%), `comment` 10 (10.9%)
- **resources.csv** — `tags_json` 83 (20.8%)
- **support_tickets.csv** — `resolved_at` 240 (24.0%), `csat` 254 (25.4%)
- **users.csv** — `last_login` 139 (17.4%)

### Dominios categóricos

- `customers_orgs.csv::industry` → 10 valores: Gaming (12), Education (10), Manufacturing (10), Government (9), Retail (9), Healthcare (8), Fintech (7), E-commerce (6), Energy (5), Media (4)
- `customers_orgs.csv::hq_region` → 7 valores: eu-central (16), ap-south (15), us-east (14), ap-northeast (11), sa-east (10), eu-west (7), us-west (7)
- `customers_orgs.csv::plan_tier` → 4 valores: standard (41), pro (21), enterprise (10), free (8)
- `customers_orgs.csv::lifecycle_stage` → 5 valores: active (54), at_risk (11), churned (6), prospect (6), lead (3)
- `customers_orgs.csv::marketing_source` → 5 valores: event (21), organic (17), partner (16), ads (14), referral (12)
- `users.csv::role` → 6 valores: data_engineer (201), devops (162), developer (130), analyst (113), ml_engineer (105), admin (89)
- `users.csv::active` → 2 valores: True (728), False (72)
- `resources.csv::service` → 6 valores: compute (116), storage (71), database (68), networking (64), analytics (42), genai (39)
- `resources.csv::region` → 7 valores: ap-northeast (72), us-east (69), us-west (61), sa-east (57), ap-south (55), eu-west (46), eu-central (40)
- `resources.csv::state` → 3 valores: running (242), stopped (119), terminated (39)
- `support_tickets.csv::category` → 6 valores: integration (181), billing (180), availability (165), usability (161), performance (158), security (155)
- `support_tickets.csv::severity` → 4 valores: low (412), medium (328), high (204), critical (56)
- `support_tickets.csv::sla_breached` → 2 valores: False (905), True (95)
- `marketing_touches.csv::channel` → 4 valores: event (401), email (385), ads (358), in_app (356)
- `marketing_touches.csv::converted` → 2 valores: False (1356), True (144)
- `billing_monthly.csv::month` → 3 valores: 2025-06-01 (80), 2025-07-01 (80), 2025-08-01 (80)
- `billing_monthly.csv::currency` → 3 valores: USD (160), ARS (51), EUR (29)

**Sin variantes de formato**: ninguna región, servicio o plan aparece con mayúsculas, guiones bajos o espacios distintos. La función de conformance se implementa igual, porque una v2 del dataset podría no venir limpia.

### Problemas de calidad

| problema                             | magnitud   | interpretación                                                           |
|:-------------------------------------|:-----------|:-------------------------------------------------------------------------|
| billing · currency=USD con fx != 1   | 160/160    | rango 0.8546–1.1179 → distorsiona el revenue hasta ±15% (D7)             |
| billing · subtotal negativo          | 13/240     | mínimo -1671.83 → notas de crédito, NO se bloquean                       |
| billing · credits nulo               | 137/240    | se interpreta como 0, no como faltante (supuesto 2 de `plan_inicial.md`) |
| billing · fx USD                     | 160 filas  | min 0.8546 / mediana 0.9951 / max 1.1179                                 |
| billing · fx EUR                     | 29 filas   | min 0.9981 / mediana 1.1047 / max 1.1981                                 |
| billing · fx ARS                     | 51 filas   | min 0.0013 / mediana 0.0015 / max 0.0016                                 |
| tickets · csat fuera de [1,5]        | 40/746     | valores [0.0, 6.0, 7.0] (D7)                                             |
| tickets · resolved_at nulo           | 240/1000   | tickets abiertos, NO es un defecto                                       |
| tickets · resolved_at < created_at   | 0          | ninguno                                                                  |
| tickets · SLA breach                 | 95/1000    | por severidad: {'medium': 37, 'low': 36, 'high': 20, 'critical': 2}      |
| orgs · nps_score fuera de [-100,100] | 1          | valor [101.0] (D7)                                                       |

### Integridad referencial

- `users.csv` → 80 orgs distintas, **0 huérfanas**
- `resources.csv` → 80 orgs distintas, **0 huérfanas**
- `support_tickets.csv` → 80 orgs distintas, **0 huérfanas**
- `marketing_touches.csv` → 80 orgs distintas, **0 huérfanas**
- `nps_surveys.csv` → 60 orgs distintas, **0 huérfanas**
- `billing_monthly.csv` → 80 orgs distintas, **0 huérfanas**

Todas las fuentes apuntan a las 80 organizaciones de `customers_orgs.csv`. Sin huérfanos.

### Eventos · 43,200 filas en 120 archivos

- **event_id únicos:** 43,200 de 43,200 → **0 duplicados**
- **Rango temporal:** 2025-07-03 → 2025-08-31 (60 días, ~720 eventos/día)
- **schema_version:** v1 10,800 (25%) · v2 32,400 (75%)
- **Corte de esquema:** v1 cubre 2025-07-03 → 2025-07-17 · v2 cubre 2025-07-18 → 2025-08-31 — sin solapamiento
- **`genai_tokens`** aparece solo en `service=genai` y `schema_version=2`: 3,132 de 4,158 eventos genai
- **Servicios:** {'networking': 6921, 'database': 7419, 'compute': 12498, 'analytics': 4590, 'storage': 7614, 'genai': 4158}
- **Métricas:** {'requests': 19512, 'cpu_hours': 10692, 'storage_gb_hours': 12996} — las 3 aparecen en los 6 servicios
  - `requests` → {'count': 18596, None: 916}
  - `cpu_hours` → {'hours': 10187, None: 505}
  - `storage_gb_hours` → {'gb_hours': 12342, None: 654}

**Tipos de `value`: {'NoneType': 877, 'float': 41014, 'str': 1309}**
→ 1309 strings (3.03%). Declarar `DoubleType` los convierte en null **sin error**: los nulos pasarían de 2.03% a 5.06%. Por eso D4 lee `value` como string en Bronze.

**`unit` nulo con `value` presente:** 2038 (4.72%) → imputable por `metric`, que es 1:1 con `unit` (D7).

### Distribución de `cost_usd_increment`

|     min |   p50 |   media |   p95 |   p99 |   p99.9 |    max |
|--------:|------:|--------:|------:|------:|--------:|-------:|
| -154.46 |     1 |    3.41 | 11.93 | 16.72 |  133.14 | 317.43 |

- **Costos negativos:** 211 (0.49%) → se marcan con flag de anomalía (D7)
- **Spikes > 100 USD:** 48
- La **media (3.41) es 3.4× la mediana (1.00)** y el salto de p99 a p99.9 es de 8×: distribución muy asimétrica. Por eso D9 usa MAD y no z-score: media y desvío están contaminados por los mismos outliers que se busca detectar.

### Impacto del watermark

| watermark   |   eventos |   descartados |   % descartado |
|:------------|----------:|--------------:|---------------:|
| 1 días      |     43200 |         40709 |           94.2 |
| 2 días      |     43200 |         40029 |           92.7 |
| 7 días      |     43200 |         36577 |           84.7 |
| 30 días     |     43200 |         20674 |           47.9 |
| 60 días     |     43200 |             0 |            0   |

**Por qué pasa:** el dataset no está ordenado por tiempo. Las primeras 5 líneas de `events_part_0000.jsonl` son ['2025-08-17T01:55:00Z', '2025-07-16T11:20:00Z', '2025-08-18T18:52:00Z', '2025-07-29T03:32:00Z', '2025-08-11T15:19:00Z'] — los 60 días vienen barajados dentro de cada archivo.

En el primer micro-lote Spark ya ve un evento del 31/08 y el watermark salta al máximo. Un watermark de 2 días **descarta el 92.7% de los eventos sin lanzar ninguna excepción**. Por eso D6 fija 60 días y resuelve la idempotencia por checkpoint y anti-join, no por watermark.

### Grano de `org_daily_usage_by_service`

- **Claves materializadas:** 11,050 de 80 × 60 × 6 = 28.800 posibles
- **Eventos por clave:** min 3 · mediana 3 · p99 9 · max 15
- **Sin skew**: la clave más pesada tiene 15 eventos, ninguna partición domina el tiempo total.

**Particionamiento (D3):**

| Esquema | Particiones | Filas/partición | Tamaño/archivo |
|---|---|---|---|
| Sin partición | 1 | 43,200 | ~1,5 MB |
| **Por `event_date`** | **60** | **~720** | **~28 KB** |
| Por `event_date` × `service` | 360 | ~120 | ~5 KB |
| Por `event_date` × `service` × `region` | 2520 | ~17 | <1 KB |

Un bloque HDFS son 128 MB. Incluso la mejor opción produce archivos 4.000× más chicos: estamos en pleno *small files problem*. Se elige `event_date` porque las 5 consultas obligatorias filtran por fecha y ninguna por servicio solo.
