# Matriz requisito-componente

**Primera entrega · 05/10/2026**

Trazabilidad entre lo que pide la consigna y lo que propone la solución. Tiene cuatro partes: el punto 6 del alcance (§5.2) pide dos cosas distintas —requisitos a componentes **y** las 5V a las decisiones de arquitectura— y §5.3 agrega la trazabilidad desde las preguntas y los objetivos del negocio.

Las decisiones citadas (D1–D12) están en [`decisions.md`](decisions.md).

---

## A · Preguntas del negocio → componentes

| # | Pregunta del negocio | Dominio | Mart en Gold | Tabla en Cassandra | Entrega |
|---|---|---|---|---|---|
| 1 | Costos y requests diarios por organización y servicio en un rango de fechas | FinOps | `org_daily_usage_by_service` | `org_daily_usage_by_service` | 2.ª |
| 2 | Top-N servicios por costo acumulado de los últimos 14 días | FinOps | `org_daily_usage_by_service` | `org_service_cost_14d` | 2.ª |
| 3 | Evolución de tickets críticos y tasa de SLA breach de los últimos 30 días | Soporte | `tickets_by_org_date` | `tickets_by_org_date` | final |
| 4 | Revenue mensual con créditos e impuestos, normalizado a USD | FinOps | `revenue_by_org_month` | `revenue_by_org_month` | final |
| 5 | Tokens GenAI y costo estimado por día | Producto | `genai_tokens_by_org_date` | `genai_tokens_by_org_date` | final |

---

## B · Requisitos técnicos → componentes

| # | Requisito | Componente propuesto | Zona | Decisión | Entrega |
|---|---|---|---|---|---|
| 1 | Ingesta batch: CSV/JSON a Bronze Parquet particionado, esquemas explícitos y columnas técnicas | `src/ingest/batch_masters.py` | Landing → Bronze | D2, D3 | 2.ª |
| 2 | Ingesta streaming: esquema explícito, watermark, dedupe por `event_id`, late data y checkpointing | `src/ingest/stream_events.py` | Landing → Bronze | D4, D6 | 2.ª |
| 3 | Calidad: reglas verificables, separación de inválidos y quarantine en Parquet | `src/quality/rules.py` | Bronze → Silver | D7 | 2.ª |
| 4 | Silver: normalización, joins con dimensiones, nulos y outliers, compatibilidad v1/v2 | `src/silver/conform_events.py` | Bronze → Silver | D5 | 2.ª |
| 5 | Features: `daily_cost_usd`, `requests`, `cpu_hours`, `storage_gb_hours` y las de v2 cuando existan | `src/silver/features.py` | Silver | §7 del diseño | 2.ª |
| 6 | Anomalías: flags o scores con método justificado | `src/gold/anomalies.py` | Silver → Gold | D9 | 2.ª |
| 7 | Gold: marts por dominio con granos claros | `src/gold/marts.py` | Gold | D10 | 2.ª / final |
| 8 | Serving: keyspace, tablas query-first y carga desde Spark | `src/serving/load_astra.py` | Gold → Cassandra | D8 | 2.ª |
| 9 | Idempotencia: reprocesamiento sin duplicados | Checkpoints + `overwrite` dinámico por partición + anti-join | todas | D6, D10 | 2.ª |
| 10 | Performance: particionado sensato, control de archivos, coalesce/repartition | Particionado por fecha + `coalesce(1)` + compactación de Bronze | todas | D3 | 2.ª |
| 11 | Gobierno: calidad, metadatos, linaje, responsabilidades, seguridad y observabilidad | Columnas técnicas, linaje por `source_file`, registro de corridas, quarantine con la regla que rechazó, README y convenciones | todas | D7, D10, D11, D12 | 2.ª / final |
| 12 | Documentación: diagrama, diccionario, decisiones, trade-offs, pruebas y evidencias | `docs/` + `decisions.md` + `evidence/` | — | D10 | **1.ª** |

---

## C · Las 5V → decisiones de arquitectura

Qué problema concreto introduce cada dimensión en este caso, y qué decisión lo responde.

| V | El problema en este caso | Decisión que lo responde | Por qué esa y no otra |
|---|---|---|---|
| **Volumen** | Hoy son 43.200 eventos y 12,6 MB, pero un proveedor real con un millón de recursos generaría del orden de cientos de GB por día | **D3** · Parquet columnar particionado por fecha y procesamiento distribuido con Spark | Particionar también por servicio daría 360 particiones de ~5 KB, cuatro mil veces más chicas que un bloque HDFS, sin ganar poda: ninguna consulta filtra por servicio solo |
| **Velocidad** | Los eventos llegan fragmentados en 120 archivos y FinOps necesita el costo incremental del día en curso, mientras que la facturación es mensual | **D1** · Patrón Lambda: streaming para eventos, batch para maestros | Kappa obligaría a convertir siete CSV estáticos en streams sintéticos sin resolver ningún problema real |
| **Variedad** | JSONL semiestructurado con dos versiones de esquema, más siete CSV, más un campo `tags_json` anidado | **D5** · Un esquema único que abarca ambas versiones, con nulls en los campos que la v1 no medía | La unión es aditiva: la v2 solo agrega campos. Dos tablas separadas obligarían a un `union` en cada lectura de Silver |
| **Veracidad** | 3,03 % de tipos ambiguos, 4,72 % de `unit` nulo, el 100 % de las facturas en USD con tipo de cambio inconsistente, costos negativos y spikes de magnitud alta | **D4** y **D7** · `value` como string en Bronze y siete reglas de calidad con quarantine | Declarar `value` como `DoubleType` convertiría el 3,03 % en null sin lanzar ningún error, y ninguna regla podría distinguir esos nulls de los legítimos |
| **Valor** | FinOps, Soporte y Producto necesitan responder preguntas concretas, no explorar datos crudos | **D8** y **D10** · Marts en Gold por dominio y modelo query-first en Cassandra | Una tabla genérica con índices secundarios obligaría a `ALLOW FILTERING`, que es un antipatrón en Cassandra y contradice el modelado query-first |

La veracidad es la dimensión más cargada en este dataset: cuatro de las siete reglas de calidad y dos de las doce decisiones existen solo por problemas medidos en el perfilado.

---

## D · Objetivos medibles → componentes

Los objetivos O1 a O7 están definidos en [`diseno_v1.md`](diseno_v1.md) §1.4, con su métrica, meta y verificación.

| Objetivo | Componente que lo cumple | Decisión | Relación con las partes A y B |
|---|---|---|---|
| O1 · Responder las preguntas del negocio desde el serving | Tablas query-first en Cassandra · `src/serving/load_astra.py` | D8 | A1 a A5 · B8 |
| O2 · Ingestar los eventos sin pérdida | `src/ingest/stream_events.py`, con watermark de 60 días | D4, D6 | B2 |
| O3 · Ingestar los maestros sin pérdida | `src/ingest/batch_masters.py` | D2, D3 | B1 |
| O4 · No perder registros entre zonas | Regla de promoción entre zonas | D10 | B4, B9 |
| O5 · Aplicar las reglas de calidad | `src/quality/rules.py` y Quarantine | D7 | B3 |
| O6 · Reprocesar sin duplicar | Checkpoints, `overwrite` dinámico por partición y anti-join | D6, D10 | B9 |
| O7 · Mantener las métricas de uso en near real-time | Structured Streaming con trigger de 10 s | D1, D6 | B2 |
