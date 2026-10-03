# Documento de Diseño v1 · Cloud Provider Analytics

**Primera entrega · 05/10/2026** · ITBA · Big Data 2C 2026

> ESQUELETO. Cada sección indica de dónde sale el contenido. Se redacta una vez que las
> decisiones están congeladas. Mantenerlo conciso y visual: la consigna
> pide que "permita una revisión rápida".

## 1. Interpretación del problema

### 1.1 Problema

El equipo actúa como área de datos de un proveedor de nube. Tres áreas de negocio —FinOps, Soporte
y Producto— necesitan analítica sobre los datos de sus clientes, pero las fuentes llegan crudas y
no son consultables de forma directa: siete maestros en CSV y un flujo de eventos de uso
fragmentado en 120 archivos JSONL, con nulos, tipos ambiguos, valores fuera de rango y un cambio
de esquema a mitad del histórico (la versión 2 incorpora `carbon_kg` y `genai_tokens`).

El problema es construir un pipeline que ingeste, limpie, conforme y publique esos datos con dos
capacidades complementarias:

| Capacidad | Datos | Necesidad que cubre |
|---|---|---|
| Near real-time | Eventos de uso (`usage_events_stream`) | Métricas operativas de uso, consumo y costo incremental |
| Batch diario o mensual | Maestros de CRM, tickets, encuestas y facturación | Dimensiones de referencia, soporte y revenue |

El resultado son marts analíticos en Parquet, publicados en Cassandra/AstraDB, que responden las
preguntas de cada área sin acceder a los datos crudos.

### 1.2 Usuarios

| Usuario | Qué necesita | Actualización | Fuentes |
|---|---|---|---|
| FinOps | Costos, consumo, revenue, créditos, impuestos y anomalías por organización y servicio | Costos de uso: near real-time · Revenue: mensual | `usage_events_stream`, `billing_monthly.csv`, `resources.csv`, `customers_orgs.csv` |
| Soporte | Volumen de tickets, severidad, cumplimiento de SLA y CSAT por organización y fecha | Diaria | `support_tickets.csv`, `customers_orgs.csv` |
| Producto / Usage | Uso de servicios, requests, métricas operativas, tokens GenAI y carbono | Near real-time | `usage_events_stream`, `resources.csv` |

`users.csv`, `marketing_touches.csv` y `nps_surveys.csv` se ingestan a Bronze como fuentes de
referencia; ninguna de las cinco consultas obligatorias depende de ellas.

### 1.3 Preguntas principales

Son las cinco consultas obligatorias de la consigna (§7.4). Cada una se responde desde una tabla
de serving propia (D8).

| # | Pregunta | Usuario | Fuente | Mart en Gold | Ingesta |
|---|---|---|---|---|---|
| P1 | ¿Cuánto costó y cuántos requests tuvo cada organización, por servicio y por día, en un rango de fechas? | FinOps | `usage_events_stream` | `org_daily_usage_by_service` | Streaming |
| P2 | ¿Cuáles son los N servicios de mayor costo acumulado de una organización en los últimos 14 días? | FinOps | `usage_events_stream` | `org_daily_usage_by_service` | Streaming |
| P3 | ¿Cómo evolucionaron los tickets críticos y la tasa de incumplimiento de SLA, por día, en los últimos 30 días? | Soporte | `support_tickets.csv` | `tickets_by_org_date` | Batch |
| P4 | ¿Cuál fue el revenue mensual de cada organización, con créditos e impuestos, normalizado a USD? | FinOps | `billing_monthly.csv` | `revenue_by_org_month` | Batch |
| P5 | ¿Cuántos tokens GenAI consumió cada organización por día y a qué costo estimado? | Producto | `usage_events_stream` (v2) | `genai_tokens_by_org_date` | Streaming |

Además, FinOps requiere identificar costos diarios anómalos por organización y servicio. Se resuelve
en Gold con `cost_anomaly_mart` (D9).

### 1.4 Objetivos medibles y criterios de éxito

El proyecto cumple su propósito cuando se alcanzan las siete metas siguientes. Las metas se
expresan sobre el dataset provisto; las cifras de referencia están medidas en
`evidence/profiling_landing.md`.

| # | Objetivo | Métrica | Meta | Verificación | Entrega |
|---|---|---|---|---|---|
| O1 | Responder las preguntas del negocio desde el serving | Preguntas P1–P5 respondidas en Cassandra/AstraDB leyendo una sola partición, sin `ALLOW FILTERING` | 5 de 5 | CQL y salida de cada consulta | 2.ª (P1 y P2) · final (P3 a P5) |
| O2 | Ingestar los eventos sin pérdida | Eventos de Landing presentes en Bronze | 43.200 de 43.200 | Conteo Landing contra Bronze | 2.ª |
| O3 | Ingestar los maestros sin pérdida | Filas de los 7 CSV presentes en Bronze | 4.112 de 4.112 | Conteo por fuente | 2.ª (3 maestros) · final (7) |
| O4 | No perder registros entre zonas | Fechas que cumplen `bronze = silver + quarantine` | 60 de 60 | Conteo por `event_date` (D10) | 2.ª |
| O5 | Aplicar las reglas de calidad | Reglas de D7 cuyo conteo de violaciones coincide con el medido en el perfilado | 7 de 7 | Conteo por regla en Silver y Quarantine | 2.ª (3 reglas) · final (7) |
| O6 | Reprocesar sin duplicar | Diferencia de filas por zona entre dos ejecuciones consecutivas · `event_id` duplicados | 0 · 0 | Conteos antes y después de re-ejecutar | 2.ª |
| O7 | Mantener las métricas de uso en near real-time | Micro-lotes hasta que un archivo de eventos queda disponible en Bronze | 1, con trigger de 10 s | Progreso de la consulta de streaming | 2.ª |

La trazabilidad de cada objetivo con el componente que lo cumple está en la parte D de
`matriz_requisito_componente.md`.

## 2. Justificación de Big Data · las 5V

| V | Hoy (medido) | A escala real (supuesto) | Decisión que lo responde |
|---|---|---|---|
| Volumen | 43.200 eventos · 4.112 filas maestras · 12,6 MB | 1 evento/recurso/minuto × 1 M recursos ⇒ ~1.440 M eventos/día ≈ 300 GB/día | Parquet columnar particionado (D3) |
| Velocidad | 120 micro-lotes de 360 eventos | Ingesta continua; FinOps necesita el costo del día en curso | Structured Streaming (D1, D6) |
| Variedad | JSONL con 2 esquemas + 7 CSV + `tags_json` anidado | Se suman logs, métricas de infra, telemetría | Esquema explícito unificado (D5) |
| Veracidad | 3,03 % tipos ambiguos · 4,72 % `unit` nulo · 100 % FX inconsistente en USD | Los mismos defectos en millones de filas | 7 reglas + Quarantine (D7) |
| Valor | 5 marts para 3 dominios | Alertas de sobrecosto, forecast, churn | Gold + serving Cassandra (D8) |

## 3. Inventario y perfil de fuentes
Fuente: `evidence/profiling_landing.md` y la sección "Hallazgos" de `decisions.md`.
Grano, frecuencia, tipos, calidad, trazabilidad y riesgos por fuente.

## 4. Arquitectura v1 y patrón elegido
Fuente: `decisions.md` D1. Diagrama `arquitectura_v1.png`. Lambda justificado más las capacidades
transversales (gobierno, calidad, seguridad, metadatos, observabilidad).

## 5. Diseño del Data Lake
Fuente: `decisions.md` D3 y D10. Zonas, formatos, particionamiento con la tabla de números,
naming, retención y reglas de promoción.

## 6. Flujos batch y streaming
Fuente: `decisions.md` D4, D5, D6, D7.
**Incluir la tabla de descarte por watermark de D6**: es el hallazgo más fuerte de la entrega.

## 7. Lógica MapReduce del flujo batch

Se expresa el mart `org_daily_usage_by_service` como job MapReduce canónico.

**Map** — por cada evento de Silver:
```
clave = (org_id, event_date, service)
valor = (cost_usd_increment,
         value si metric = "requests"         else 0,
         value si metric = "cpu_hours"        else 0,
         value si metric = "storage_gb_hours" else 0,
         genai_tokens ?? 0,
         carbon_kg    ?? 0,
         1)                                    # contador
```

**Combiner** — suma componente a componente dentro de cada mapper. Válido porque todas las
métricas son sumas, y la suma es asociativa y conmutativa.

**Shuffle** — agrupa por `(org_id, event_date, service)`. 80 × 60 × 6 = 28.800 claves posibles.

**Reduce** — suma los vectores parciales y emite una fila del mart:
```
(org_id, usage_date, service, daily_cost_usd, requests,
 cpu_hours, storage_gb_hours, genai_tokens, carbon_kg, event_count)
```

**Equivalencia en Spark:**
```python
(silver_events
   .groupBy("org_id", "event_date", "service")
   .agg(F.sum("cost_usd_increment").alias("daily_cost_usd"),
        F.sum(F.when(F.col("metric") == "requests", F.col("value_num")).otherwise(0)).alias("requests"),
        ...))
```

**Correspondencia:** el `groupBy().agg()` equivale a este MapReduce. El `groupBy` define la clave
del shuffle, el `agg` es el reduce, y Spark aplica agregación parcial en el mapper, que cumple el
rol del combiner. La diferencia (clase 03, p. 45) es que MapReduce escribe el resultado de cada
trabajo en HDFS con replicación, mientras Spark mantiene los resultados intermedios en memoria y
encadena las transformaciones en un DAG con evaluación perezosa.

**Sobre el skew:** no hay riesgo. La clave más pesada tiene 15 eventos y la mediana es 3, así que
ninguna partición domina el tiempo total.

## 8. Matriz requisito-componente
Archivo aparte: `matriz_requisito_componente.md`.

## 9. Supuestos, riesgos y decisiones abiertas
Fuente: secciones finales de `decisions.md`. Las decisiones abiertas se listan como abiertas
— lo pide la consigna §5.2.10.

## 10. Estimación de esfuerzo y recursos
Fuente: `plan_inicial.md` §3.
