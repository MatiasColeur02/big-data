# DECISIONS.md

**Cloud Provider Analytics · ITBA Big Data 2C 2026** · v1.1

> Cifras verificadas sobre el **dataset completo** (120 partes, 43.200 eventos). Fuente: `evidence/profiling_landing.md`.

Decisiones técnicas del proyecto. Roadmap y calendario en [`docs/roadmap.md`](roadmap.md).

---

## D1 · Patrón: **Lambda**

Streaming para `usage_events_stream` (43.200 eventos en 120 JSONL), batch para los 7 CSV maestros (4.112 filas).

La consigna exige sí o sí streaming de eventos y batch de maestros. Kappa obligaría a convertir 7 CSV en streams sintéticos sin resolver nada: `billing_monthly` tiene 3 cortes mensuales y las dimensiones cambian a ritmo diario.

**Riesgo:** duplicar lógica entre las dos ramas. **Mitigación:** las funciones de conformance viven una sola vez en `src/common/` y se importan desde los dos jobs.

---

## D2 · Entorno: Colab + PySpark **3.5.x** + Drive

`master("local[*]")`, Data Lake en `/content/drive/MyDrive/bigdata_itba/datalake/`.

Se fija 3.5.x y no 4.x porque el `spark-cassandra-connector` publica assembly estable para Spark 3.5: arrancar en 4.x nos obliga a migrar en noviembre. Drive y no el disco de Colab porque el disco se borra al reciclarse la sesión y ahí se pierden Bronze, Silver y los checkpoints — sin eso no se puede demostrar idempotencia.

**Sobre `local[*]`:** se corre en un nodo y hay que poder defenderlo. El argumento es que usamos solo la API de DataFrames, particionado explícito y Parquet, así que el mismo código corre distribuido sin reescribir nada. Es la pregunta más probable del docente.

---

## D3 · Particionar solo por fecha, **nunca por fecha × servicio**

Eventos por `event_date`. Maestros chicos sin particionar (`coalesce(1)`). `billing_monthly` por `month`.

| Esquema | Particiones | Filas/partición | Tamaño/archivo |
|---|---|---|---|
| Sin partición | 1 | 43.200 | ~1,5 MB |
| **Por `event_date`** | **60** | **~720** | **~28 KB** |
| Por `event_date` × `service` | 360 | ~120 | ~5 KB |
| Por `event_date` × `service` × `region` | 2.520 | ~17 | <1 KB |

Un bloque HDFS son 128 MB: incluso la mejor opción da archivos 4.000× más chicos, así que estamos en pleno *small files problem*. Particionar por servicio lo multiplica por seis sin ganar poda, porque las 5 consultas obligatorias filtran todas por organización y fecha, y ninguna por servicio solo.

Se acepta el costo de archivos chicos a cambio de poda temporal (las consultas piden "últimos 14/30 días") y de un layout que escala. Se compacta Bronze con un job posterior.

**No por `org_id`:** 80 orgs × 60 días = 4.800 particiones de 9 filas. `org_id` es la clave de partición en Cassandra (D8), no en el lago.

---

## D4 · `value` se lee como **string** en Bronze

El campo llega con tres tipos: número (41.014), string numérico (1.309) y null (877). Si se declara `DoubleType`, el lector JSON en modo `PERMISSIVE` — el default — convierte los strings en null **sin avisar**: los nulos pasarían de 2,03 % a 5,06 % y ninguna regla de calidad lo detectaría, porque los nulls legítimos y los nulls por casteo fallido serían indistinguibles.

Bronze guarda el string, Silver castea con `try_cast` y manda a Quarantine lo que falle. Es la división de responsabilidades que define la consigna: Bronze preserva la fuente, Silver impone el tipo.

---

## D5 · Esquema único v1+v2, no dos tablas

Una sola tabla Bronze con las columnas de v2; los eventos v1 llevan `carbon_kg` y `genai_tokens` en null. Se conserva `schema_version` como columna.

La unión es *additive*: v2 solo agrega campos. El corte es limpio, sin solapamiento — v1 cubre 03/07 → 17/07 y v2 cubre 18/07 → 31/08. `genai_tokens` aparece solo en `service=genai` con `schema_version=2`: 3.132 de 4.158 eventos genai.

**Consecuencia:** el mart `genai_tokens_by_org_date` no tiene datos de julio, y la respuesta correcta a "¿por qué falta?" es "el esquema v1 no medía tokens", no un bug.

---

## D6 · Watermark de **60 días** — cualquier valor menor destruye el dataset

`withWatermark("event_ts", "60 days")` + `dropDuplicatesWithinWatermark("event_id")`. La idempotencia real se apoya en el checkpoint y un anti-join contra Bronze, **no** en el watermark.

**El dataset no está ordenado temporalmente.** Cada archivo contiene los 60 días barajados: las primeras líneas de `events_part_0000.jsonl` son del 17/08, 16/07, 18/08, 29/07 y 11/08. En el primer micro-lote Spark ya ve un evento del 31/08 y el watermark salta al máximo.

Simulando el comportamiento real de Structured Streaming con micro-lotes de 5 archivos:

| Watermark | Eventos descartados |
|---|---|
| 1 día | **94,2 %** |
| 2 días | **92,7 %** |
| 7 días | 84,7 % |
| 30 días | 47,9 % |
| **60 días** | **0 %** |

Un watermark de 2 días — el valor "razonable" por reflejo — tira el 93 % de los eventos **sin excepción ni log**. El *event time* y el *processing time* no correlacionan: esto es un backfill, no un stream en vivo, y el watermark es la herramienta correcta solo cuando ambos tiempos correlacionan.

**Riesgo:** 60 días de estado sería inviable a escala real. Acá son 43.200 claves, unos pocos MB. Se documenta que en producción habría que separar backfill histórico (batch) de stream en vivo (watermark de minutos) — que es justamente lo que Lambda permite.

---

## D7 · Siete reglas de calidad

`BLOQUEA` manda a Quarantine; `MARCA` deja pasar con bandera.

| Regla | Acción | Violaciones medidas |
|---|---|---|
| `event_id` no nulo y único | BLOQUEA | 0 (43.200 únicos) |
| `cost_usd_increment >= -0.01` | MARCA | 211 / 43.200 (0,49 %) |
| `unit` no nulo si hay `value` | MARCA e imputa | 2.038 (4,72 %) |
| `value` casteable a double | BLOQUEA si falla | 0 fallan |
| `currency='USD'` ⇒ `fx = 1.0` | CORRIGE | **160 / 160 filas USD** |
| `csat ∈ [1,5]` | MARCA y anula | 40 / 746 (0.0×11, 6.0×28, 7.0×1) |
| `nps_score ∈ [-100,100]` | MARCA y anula | 1 (valor 101.0) |

**Por qué `event_id` bloquea con cero violaciones:** no protege contra Landing, que está limpio, sino contra el reproceso. Es una regla de idempotencia disfrazada de regla de calidad.

**Por qué `unit` imputa en vez de bloquear:** la unidad se deduce de la métrica sin ambigüedad (`requests→count`, `cpu_hours→hours`, `storage_gb_hours→gb_hours`, sin una sola excepción en 43.200 eventos). Bloquear 4,72 % por un campo derivable es destruir datos buenos.

**Por qué el FX se corrige:** las 160 facturas en USD traen tipo de cambio entre 0,8546 y 1,1179, cuando un importe en USD convertido a USD tiene fx = 1 por definición. Es ruido sintético. Sin corregir, el revenue queda distorsionado ±15 % y la consulta obligatoria 4 da un número sin significado. Se conserva el original en `exchange_rate_raw`.

**Lo que no es error:** el `subtotal` negativo de billing (13/240) son notas de crédito y se conservan. Los tipos de cambio de ARS (~0,0015) son reales, no ceros mal cargados — es exactamente el valor que alguien "arregla" por las dudas y termina inflando el revenue argentino por 650.

---

## D8 · Cassandra query-first: una tabla por consulta

| Consulta (§7.4) | Tabla | Partition key | Clustering key |
|---|---|---|---|
| 1 · Costos y requests diarios | `org_daily_usage_by_service` | `(org_id)` | `usage_date DESC, service` |
| 2 · Top-N servicios, 14 días | `org_service_cost_14d` | `(org_id)` | `total_cost_usd DESC, service` |
| 3 · Tickets críticos y SLA | `tickets_by_org_date` | `(org_id)` | `date DESC, severity` |
| 4 · Revenue mensual en USD | `revenue_by_org_month` | `(org_id)` | `month DESC` |
| 5 · Tokens GenAI por día | `genai_tokens_by_org_date` | `(org_id)` | `date DESC` |

`org_id` como partition key en todas porque las cinco consultas filtran por organización: se toca una sola partición. El mart diario tiene 11.050 filas sobre 80 organizaciones: ~138 por partición, muy por debajo del límite práctico.

**Tabla aparte para el Top-N** porque Cassandra no ordena por columna agregada: hay que pre-agregar en Spark y escribir el total como clustering key descendente. Resolverlo con `ALLOW FILTERING` sobre la tabla diaria es el antipatrón que la consigna quiere ver evitado.

Fechas en `DESC` porque todas las consultas piden datos recientes.

---

## D9 · Anomalías: **MAD**, no z-score

Z-score modificado por grupo `(org_id, service)` sobre `daily_cost_usd`, umbral `|0.6745·(x−mediana)/MAD| > 3.5`.

| min | p50 | media | p95 | p99 | p99,9 | max |
|---|---|---|---|---|---|---|
| −154,46 | 1,00 | **3,41** | 11,93 | 16,72 | 133,14 | 317,43 |

La media es 3,4× la mediana y el salto de p99 a p99,9 es de 8×: distribución muy asimétrica. El z-score usa media y desvío, y ambos están contaminados por los mismos outliers que se busca detectar. Mediana y MAD son robustas por construcción.

Por grupo porque 200 USD/día es normal para una enterprise en compute y anómalo para una free en analytics. **Si MAD = 0** (organización con poca actividad), se cae a percentil 99 del grupo y se registra el método usado.

---

## D10 · Zonas, promoción y repo

```
datalake/
├── landing/        # inmutable, solo lectura
├── bronze/         # Parquet, mismo grano, + ingest_ts y source_file
├── silver/         # conformado, joins, calidad aplicada
├── gold/           # 5 marts con los nombres de la consigna
├── quarantine/     # rechazados, particionado por regla
└── _checkpoints/   # fuera de las zonas: es estado del motor, no dato
```

`_checkpoints/` afuera porque si vive dentro de `bronze/`, un `spark.read.parquet()` lo intenta leer y rompe.

**Promoción:** `overwrite` por partición (`partitionOverwriteMode=dynamic`), no global — es lo que permite reprocesar un día sin borrar el histórico, y es el mecanismo concreto detrás de la idempotencia. Ninguna fecha pasa a Gold si su Silver no está completa, y se verifica con `count(bronze[d]) == count(silver[d]) + count(quarantine[d])`.

**Repo:** la estructura recomendada por la consigna §8.1, con `roadmap.md` dentro de `docs/` como único agregado. Nombres de marts en Gold iguales a los de la consigna, para no tener que documentar correspondencias. El dataset (13 MB) sí se commitea; las zonas generadas y cualquier credencial, nunca.

---

## Hallazgos del perfilado

47.312 filas, 12,6 MB, 60 días (03/07 → 31/08/2025). Detalle completo en `evidence/profiling_landing.md`.

**Lo que está bien:** integridad referencial perfecta (cero huérfanos en las 6 fuentes que referencian organizaciones, los 400 `resource_id` existen). Cero duplicados de PK. Cero fechas inválidas. Dominios ya conformados: no hay variantes tipo `us_east` / `US-East`. Ningún evento tiene `service` o `region` distintos de los de su recurso.

Esto último importa: el join con `dim_resource` **no** es necesario para conformar, pero se hace igual como control de calidad y para traer `state` y `tags_json`. Conviene decirlo así — se verificó en lugar de asumirlo.

**Lo que está roto:** ver D7. Más `credits` nulo en billing (57 %, se interpreta como 0), `resolved_at` nulo en tickets (24 %, son tickets abiertos, no un defecto) y `last_login` nulo en users (17 %).

**Sin skew:** de las 28.800 claves posibles de `(org_id, date, service)` se materializan 11.050, con mediana de 3 eventos y un máximo de 15. Ninguna partición domina el tiempo total. Vale decirlo en el documento: muestra que se evaluó el riesgo.

---

## Decisiones abiertas

Se declaran como abiertas en la entrega.

| Pregunta | Se decide antes de | Inclinación |
|---|---|---|
| ¿AstraDB o Cassandra en Docker? | 16/11 | AstraDB, por la modalidad remota |
| ¿El componente de ML es anomalías o churn? | 16/11 | Anomalías: reutiliza D9 y alimenta un mart ya pedido |
| ¿SCD tipo 2 en `dim_org`? | 16/11 | No: el dataset no trae historia de cambios de plan |
| ¿Herramienta de visualización? | 07/12 | Notebook con matplotlib |
| ¿Airflow o notebook secuencial? | 07/12 | Secuencial: Airflow en Colab es overhead sin beneficio |

---

## Qué no está en este archivo

Los **supuestos**, los **riesgos con sus mitigaciones** y la **estimación de esfuerzo** viven en
[`plan_inicial.md`](plan_inicial.md), que es el artefacto 5 de la entrega. Acá quedan solo las
decisiones técnicas y la evidencia que las sostiene.
