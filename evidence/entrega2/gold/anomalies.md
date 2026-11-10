# Anomalías de costo diario · MAD (D9)

`as_of_date` 2025-08-31 · umbral `|0.6745·(x−mediana)/MAD| > 3.5` por `(org_id, service)`; si MAD = 0, `x > p99` del grupo.

| Métrica | Valor |
|---|---|
| Días-servicio evaluados | 11050 |
| Anomalías | 134 (1.21 %) |
| Por encima / por debajo de la mediana | 124 / 10 |
| Grupos `(org, servicio)` con al menos una | 93 de 262 |
| Método `mad` | 11050 días-servicio, 134 anomalías |

## Las 10 de mayor score

| org_id       | usage_date   | service    |   daily_cost_usd |   median_cost_usd |   mad_cost_usd |   anomaly_score | method   |
|:-------------|:-------------|:-----------|-----------------:|------------------:|---------------:|----------------:|:---------|
| org_okep7y6w | 2025-07-03   | compute    |         202.646  |          11.1426  |         2.7475 |         47.0133 | mad      |
| org_x7eedtjv | 2025-07-09   | compute    |         311.255  |          13.8981  |         4.3827 |         45.7634 | mad      |
| org_okep7y6w | 2025-08-07   | compute    |         191.897  |          11.1426  |         2.7475 |         44.3744 | mad      |
| org_w3zp08j3 | 2025-07-05   | networking |          27.4203 |           1.4849  |         0.4944 |         35.3831 | mad      |
| org_w3zp08j3 | 2025-07-16   | networking |          26.4157 |           1.4849  |         0.4944 |         34.0126 | mad      |
| org_x7eedtjv | 2025-08-23   | compute    |         211.248  |          13.8981  |         4.3827 |         30.3723 | mad      |
| org_pht0hl9x | 2025-08-25   | analytics  |         257.042  |          14.6066  |         5.4627 |         29.9344 | mad      |
| org_874frhoc | 2025-07-17   | networking |          27.651  |           1.708   |         0.6065 |         28.8517 | mad      |
| org_d14ve92m | 2025-07-20   | analytics  |         181.799  |          10.7011  |         4.071  |         28.3482 | mad      |
| org_pnsm43d8 | 2025-07-17   | storage    |          56.6704 |           3.58035 |         1.291  |         27.7376 | mad      |

**Limitaciones:** sin etiquetas de verdad no hay precisión ni recall; el umbral 3.5 es el de Iglewicz y Hoaglin. Con pocos días por grupo, el fallback p99 marca a lo sumo el máximo.
