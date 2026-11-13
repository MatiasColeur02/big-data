# Rutas y tamaños del Data Lake

Perfil `local` · corrida del 12/11/2026 desde cero (`make clean` + `make pipeline`). Log completo en [`pipeline_log.txt`](pipeline_log.txt) y registro de corridas en [`runs.jsonl`](runs.jsonl).

Tamaño = suma de los archivos Parquet de la tabla, sin metadatos de Spark.

## Bronze

| Ruta                                   |  Filas |       Particiones | Archivos |  Tamaño |
|----------------------------------------|-------:|------------------:|---------:|--------:|
| `datalake/bronze/billing_monthly/`     |    240 |       3 · `month` |        3 | 18,4 KB |
| `datalake/bronze/customers_orgs/`      |     80 |                 0 |        1 |  6,4 KB |
| `datalake/bronze/marketing_touches/`   |  1.500 |                 0 |        1 | 22,3 KB |
| `datalake/bronze/nps_surveys/`         |     92 |                 0 |        1 |  3,4 KB |
| `datalake/bronze/resources/`           |    400 |                 0 |        1 | 11,3 KB |
| `datalake/bronze/support_tickets/`     |  1.000 |                 0 |        1 | 18,3 KB |
| `datalake/bronze/users/`               |    800 |                 0 |        1 | 22,9 KB |
| `datalake/bronze/usage_events_stream/` | 43.200 | 60 · `event_date` |    1.440 | 9,29 MB |

## Silver

| Ruta                                    |  Filas |       Particiones | Archivos |   Tamaño |
|-----------------------------------------|-------:|------------------:|---------:|---------:|
| `datalake/silver/dim_org/`              |     80 |                 0 |        1 |   7,4 KB |
| `datalake/silver/dim_resource/`         |    400 |                 0 |        1 |  12,3 KB |
| `datalake/silver/usage_events/`         | 43.200 | 60 · `event_date` |       60 |  3,05 MB |
| `datalake/silver/usage_daily_features/` | 11.050 | 60 · `usage_date` |       60 | 596,9 KB |

## Gold

| Ruta                                        |  Filas |       Particiones | Archivos |   Tamaño |
|---------------------------------------------|-------:|------------------:|---------:|---------:|
| `datalake/gold/org_daily_usage_by_service/` | 11.050 | 60 · `usage_date` |       60 | 614,5 KB |
| `datalake/gold/org_service_cost_14d/`       |    262 |                 0 |        1 |   7,3 KB |
| `datalake/gold/cost_anomaly_mart/`          | 11.050 | 60 · `usage_date` |       60 | 742,6 KB |

## Quarantine

| Ruta                                       | Filas | Particiones | Archivos | Tamaño |
|--------------------------------------------|------:|------------:|---------:|-------:|
| `datalake/quarantine/billing_monthly/`     |     0 |           0 |        0 | 0,0 KB |
| `datalake/quarantine/customers_orgs/`      |     0 |           0 |        0 | 0,0 KB |
| `datalake/quarantine/marketing_touches/`   |     0 |           0 |        0 | 0,0 KB |
| `datalake/quarantine/nps_surveys/`         |     0 |           0 |        0 | 0,0 KB |
| `datalake/quarantine/resources/`           |     0 |           0 |        0 | 0,0 KB |
| `datalake/quarantine/support_tickets/`     |     0 |           0 |        0 | 0,0 KB |
| `datalake/quarantine/users/`               |     0 |           0 |        0 | 0,0 KB |
| `datalake/quarantine/usage_events_stream/` |     0 |           0 |        0 | 0,0 KB |

## Fuera de las zonas

| Ruta                                         | Contenido                                                |  Tamaño |
|----------------------------------------------|----------------------------------------------------------|--------:|
| `datalake/_checkpoints/bronze_usage_events/` | Offsets, commits y estado de la deduplicación (24 lotes) | 1,89 MB |
| `datalake/_metadata/runs/runs.jsonl`         | Una línea por job (7 en esta corrida)                    |       — |

## Totales

| Zona       |  Tamaño |
|------------|--------:|
| Bronze     | 9,39 MB |
| Silver     | 3,65 MB |
| Gold       | 1,33 MB |
| Quarantine |  0,0 KB |

Bronze de eventos queda en 1.440 archivos (60 fechas × 24 lotes, D13) y Silver lo compacta a 60, uno por fecha (D21).
