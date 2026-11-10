# DECISIONS

**Cloud Provider Analytics · ITBA Big Data 2C 2026** · v1.4 (D13–D21, segunda entrega)

> Cifras verificadas sobre el **dataset completo** (120 partes, 43.200 eventos). Fuente: `evidence/profiling_landing.md`.

Decisiones técnicas del proyecto.

---

## D1 · Patrón: **Lambda**

Streaming para `usage_events_stream` (43.200 eventos en 120 JSONL), batch para los 7 CSV maestros (4.112 filas).

La consigna exige streaming de eventos y batch de maestros (§4.3). Kappa obligaría a convertir 7 CSV en streams sintéticos sin resolver nada: `billing_monthly` tiene 3 cortes mensuales y las dimensiones cambian a ritmo diario.

**Riesgo:** duplicar lógica entre las dos ramas. **Mitigación:** las funciones de conformance viven una sola vez en `src/common/` y se importan desde los dos jobs.

---

## D2 · Entorno: Colab + PySpark **3.5.x** + Drive

> **Actualizada por D17:** el entorno principal pasa a ser local (venv + Cassandra en Docker) y Colab + AstraDB queda como segundo perfil.

`master("local[*]")`, Data Lake en `/content/drive/MyDrive/bigdata_itba/datalake/`.

Se fija 3.5.x y no 4.x porque el `spark-cassandra-connector` publica assembly estable para Spark 3.5: arrancar en 4.x nos obliga a migrar en noviembre. Drive y no el disco de Colab porque el disco se borra al reciclarse la sesión y ahí se pierden Bronze, Silver y los checkpoints — sin eso no se puede demostrar idempotencia.

**Sobre `local[*]`:** la ejecución es en un solo nodo. Se usa únicamente la API de DataFrames, particionado explícito y Parquet, por lo que el mismo código corre distribuido en un clúster sin reescribirse.

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

Se acepta el costo de archivos chicos a cambio de poda temporal (las consultas piden "últimos 14/30 días") y de un layout que escala. ~~Se compacta Bronze con un job posterior.~~ **Actualizado por D13:** Bronze no se compacta en el lugar; la compactación ocurre al escribir Silver.

**No por `org_id`:** 80 orgs × 60 días = 4.800 particiones de 9 filas. `org_id` es la clave de partición en Cassandra (D8), no en el lago.

---

## D4 · `value` se lee como **string** en Bronze

El campo llega con tres tipos: número (41.014), string numérico (1.309) y null (877). Si se declara `DoubleType`, el lector JSON en modo `PERMISSIVE` — el default — convierte los strings en null **sin avisar**: los nulos pasarían de 2,03 % a 5,06 % y ninguna regla de calidad lo detectaría, porque los nulls legítimos y los nulls por casteo fallido serían indistinguibles.

Bronze guarda el string, Silver castea con `try_cast` y manda a Quarantine lo que falle. Es la división de responsabilidades que define la consigna: Bronze preserva la fuente, Silver impone el tipo.

---

## D5 · Esquema único v1+v2, no dos tablas

Una sola tabla Bronze con las columnas de v2; los eventos v1 llevan `carbon_kg` y `genai_tokens` en null. Se conserva `schema_version` como columna.

La unión es *additive*: v2 solo agrega campos. El corte es limpio, sin solapamiento — v1 cubre 03/07 → 17/07 y v2 cubre 18/07 → 31/08. `genai_tokens` aparece solo en `service=genai` con `schema_version=2`: 3.132 de 4.158 eventos genai.

**Consecuencia:** el mart `genai_tokens_by_org_date` no tiene datos anteriores al 18/07. La ausencia es esperada y no un defecto: el esquema v1 no medía tokens.

---

## D6 · Watermark de **60 días** — cualquier valor menor destruye el dataset

`withWatermark("event_ts", "60 days")` + `dropDuplicatesWithinWatermark("event_id")`. La idempotencia real se apoya en el checkpoint y un anti-join contra Bronze, **no** en el watermark.

> **Actualizada por D13:** el anti-join se descarta. La idempotencia sale del checkpoint y de `_spark_metadata/` del file sink, y la regla de `event_id` único en Silver actúa como red de seguridad.

**El dataset no está ordenado temporalmente.** Cada archivo contiene los 60 días barajados: las primeras líneas de `events_part_0000.jsonl` son del 17/08, 16/07, 18/08, 29/07 y 11/08. En el primer micro-lote Spark ya ve un evento del 31/08 y el watermark salta al máximo.

Simulando el comportamiento real de Structured Streaming con micro-lotes de 5 archivos:

| Watermark | Eventos descartados |
|---|---|
| 1 día | **94,2 %** |
| 2 días | **92,7 %** |
| 7 días | 84,7 % |
| 30 días | 47,9 % |
| **60 días** | **0 %** |

Un watermark de 2 días, valor habitual en un stream en vivo, descarta el 93 % de los eventos **sin excepción ni log**. El *event time* y el *processing time* no correlacionan: esto es un backfill, no un stream en vivo, y el watermark es la herramienta correcta solo cuando ambos tiempos correlacionan.

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

**Por qué `event_id` bloquea con cero violaciones:** no protege contra Landing, que está limpio, sino contra el reproceso. Cumple la función de regla de idempotencia además de regla de calidad.

**Por qué `unit` imputa en vez de bloquear:** la unidad se deduce de la métrica sin ambigüedad (`requests→count`, `cpu_hours→hours`, `storage_gb_hours→gb_hours`, sin una sola excepción en 43.200 eventos). Bloquear 4,72 % por un campo derivable es destruir datos buenos.

**Por qué el FX se corrige:** las 160 facturas en USD traen tipo de cambio entre 0,8546 y 1,1179, cuando un importe en USD convertido a USD tiene fx = 1 por definición. Es ruido sintético. Sin corregir, el revenue queda distorsionado ±15 % y la consulta obligatoria 4 da un número sin significado. Se conserva el original en `exchange_rate_raw`.

**Lo que no es error:** el `subtotal` negativo de billing (13/240) son notas de crédito y se conservan. Los tipos de cambio de ARS (~0,0015) son reales, no ceros mal cargados: forzarlos a 1 inflaría el revenue en ARS unas 650 veces.

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

**Tabla aparte para el Top-N** porque Cassandra no ordena por columna agregada: hay que pre-agregar en Spark y escribir el total como clustering key descendente. Resolverlo con `ALLOW FILTERING` sobre la tabla diaria es un antipatrón en Cassandra y contradice el modelado query-first que exige la consigna (§4.4).

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
├── _checkpoints/   # fuera de las zonas: es estado del motor, no dato
└── _metadata/      # fuera de las zonas: registro de corridas (D11)
```

`_checkpoints/` afuera porque si vive dentro de `bronze/`, un `spark.read.parquet()` lo intenta leer y rompe.

**Promoción:** `overwrite` por partición (`partitionOverwriteMode=dynamic`) en Silver y Gold (Bronze de eventos usa `append`, ver D13), no global — es lo que permite reprocesar un día sin borrar el histórico, y es el mecanismo concreto detrás de la idempotencia. Ninguna fecha pasa a Gold si su Silver no está completa, y se verifica con `count(bronze[d]) == count(silver[d]) + count(quarantine[d])`.

**Repo:** la estructura recomendada por la consigna §8.1, con el registro de decisiones dentro de `docs/`. Nombres de marts en Gold iguales a los de la consigna, para no tener que documentar correspondencias. El dataset (13 MB) sí se commitea; las zonas generadas y cualquier credencial, nunca.

---

## D11 · Metadatos en tres niveles, **sin metastore**

Cada nivel responde una pregunta distinta y vive donde se lo consulta.

| Nivel | Qué registra | Dónde vive |
|---|---|---|
| Registro | De dónde vino y qué le pasó a cada fila | Columnas técnicas dentro del Parquet de cada zona |
| Esquema | Nombre, tipo y nulabilidad de cada columna | `StructType` explícitos en `src/common/`, footer de Parquet y diccionario de datos en `docs/` |
| Corrida | Qué leyó, escribió y rechazó cada ejecución | `datalake/_metadata/runs/`, un registro por job ejecutado |

Columnas técnicas por zona:

| Zona | Columnas técnicas | Qué permiten |
|---|---|---|
| Landing | Ninguna: los archivos no se modifican | El nombre y la ruta del archivo son el metadato |
| Bronze | `ingest_ts`, `source_file` | Saber cuándo y desde qué archivo entró cada fila |
| Silver | Las de Bronze y `dq_flags` | Conservar el origen y saber qué reglas de D7 marcaron la fila |
| Quarantine | Las de Bronze, `dq_rule`, `rejected_ts` y `event_date` (D16) | Saber qué regla rechazó cada fila y cuándo, y cerrar O4 por fecha |
| Gold | `processed_ts` | Saber cuándo se calculó cada fila del mart |

`ingest_ts` y `source_file` son las columnas que exige la consigna (§4.2). `dq_flags` es una lista con los identificadores de las reglas `MARCA` de D7 que se aplicaron a la fila. `dq_rule` es la regla `BLOQUEA` que la rechazó y es además la columna de partición de Quarantine (D10). Los eventos conservan `schema_version` como columna (D5).

**Registro de corridas:** cada job agrega una línea con `run_id`, `job`, zona de origen, zona de destino, inicio, duración, filas leídas, filas escritas, filas rechazadas y particiones escritas. Es la práctica indicada para el diseño de pipelines ("registrar métricas: filas leídas, descartadas, duplicados, tiempo por etapa") y es la fuente con la que se verifican los objetivos O4 y O6 del documento de diseño. Se guarda en JSON Lines y no en Parquet: se lee sin Spark y se copia a `evidence/` como evidencia de ejecución.

**Linaje:** a nivel de fila lo da `source_file`, que se propaga de Bronze a Silver y a Quarantine. A nivel de tabla lo da el registro de corridas, que indica qué job escribió cada zona y desde cuál leyó. En Gold el linaje es por tabla: una fila del mart agrega eventos de varios archivos.

**Sin metastore:** las tablas se referencian por ruta y el esquema sale del código. En Colab con `local[*]` (D2) el metastore embebido de Spark se crea en el disco de la sesión y se pierde al reciclarse; la ruta en Drive y el esquema versionado en el repositorio persisten.

**Riesgo:** sin catálogo no hay un lugar único donde descubrir las tablas. **Mitigación:** la convención de rutas de D12 y el diccionario de datos, que forma parte de la documentación técnica (consigna §4.4).

---

## D12 · Naming por convención de rutas y retención **por partición de fecha**

**Naming.** Todo en minúsculas y `snake_case`.

| Elemento | Convención | Ejemplo |
|---|---|---|
| Ruta | `<zona>/<tabla>/<columna>=<valor>/` | `bronze/usage_events_stream/event_date=2025-07-03/` |
| Tabla en Bronze | Nombre de la fuente, sin extensión | `bronze/billing_monthly/` |
| Tabla en Silver | Entidad conformada; prefijo `dim_` para dimensiones | `silver/usage_events/`, `silver/dim_org/` |
| Tabla en Gold | Nombre del mart de la consigna (D10) | `gold/org_daily_usage_by_service/` |
| Tabla en Quarantine | Tabla de origen, particionada por regla | `quarantine/usage_events_stream/dq_rule=value_not_castable/` |
| Columna | Sufijo según el tipo de dato: `_ts`, `_date`, `_usd`, `_raw` | `ingest_ts`, `event_date`, `daily_cost_usd`, `exchange_rate_raw` |

La forma `<columna>=<valor>` es la que Spark genera con `partitionBy` y la que le permite podar particiones al leer. El sufijo `_raw` conserva el valor original cuando una regla lo corrige (D7).

**Retención.** Se define para la operación a escala real: el dataset provisto cubre 60 días y ninguna política llega a aplicarse sobre él.

| Zona | Retención | Justificación |
|---|---|---|
| Landing | Indefinida | Es inmutable (consigna §4.2) y la única copia desde la que se reconstruyen las demás zonas |
| Bronze | 13 meses | Se regenera desde Landing; 13 meses permiten comparar un mes con el mismo mes del año anterior sin reingestar |
| Silver | 13 meses | Se regenera desde Bronze; mismo horizonte que Bronze para que el reproceso de una fecha sea siempre posible |
| Gold | Indefinida | Es agregado y chico (11.050 filas en el mart diario para 60 días) y contiene el histórico de revenue |
| Quarantine | 90 días | Plazo para revisar el rechazo, corregir la regla o la fuente y reprocesar |
| `_checkpoints/` | Mientras exista la consulta de streaming | Borrarlo hace que el stream vuelva a leer Landing desde el inicio; solo se borra en un reproceso completo |
| `_metadata/` | Indefinida | Una línea por corrida; su tamaño es despreciable |

**Cómo se aplica:** borrando particiones completas por `event_date` (D3), sin reescribir archivos. La retención y la poda de lectura usan la misma columna, lo que refuerza la decisión de particionar por fecha. Los maestros no se particionan (D3): cada carga reemplaza la anterior y no acumulan histórico.

**Riesgo:** una retención mal aplicada borra datos todavía necesarios. **Mitigación:** Landing no se borra, así que toda zona es reconstruible.

---

## D13 · Bronze de eventos con **file sink nativo**, no `foreachBatch` con overwrite

`writeStream.format("parquet").partitionBy("event_date").outputMode("append")`, con checkpoint en `_checkpoints/bronze_usage_events/`.

Cada JSONL trae eventos de los 60 días mezclados (D6), así que cada micro-lote toca casi todas las particiones. Con `foreachBatch` y `partitionOverwriteMode=dynamic`, el lote *n* reemplaza las particiones que escribió el lote *n−1*: al terminar, Bronze tendría solo los eventos del último lote, y Spark no da ningún error.

| Opción | Garantía | Problema |
|---|---|---|
| **File sink `append`** | Exactly-once: el checkpoint registra qué archivos se leyeron y `_spark_metadata/` qué archivos se escribieron | Muchos archivos chicos |
| `foreachBatch` + overwrite dinámico | Ninguna | Pisa los lotes anteriores |
| `foreachBatch` + `append` | At-least-once: si falla después de escribir, el lote se repite | Hay que deduplicar por `batch_id` a mano |

**Reglas que se derivan:**

- **El checkpoint y la salida se borran siempre juntos.** Si se borra solo el checkpoint, la consulta vuelve a numerar los lotes desde 0, el sink los encuentra ya registrados en `_spark_metadata/` y no escribe nada. Si se borra solo la salida, el checkpoint da los 120 archivos por leídos y Bronze queda vacío. Ninguno de los dos casos da error.
- **Bronze se lee siempre desde la raíz** (`bronze/usage_events_stream/`) y se filtra por `event_date`. Así Spark usa `_spark_metadata/` y descarta los archivos de lotes que fallaron a mitad de camino. Si se lee directo la carpeta de una partición, ese control se pierde.
- **`repartition("event_date")` antes de escribir:** un archivo por fecha y por lote, como máximo 60 × 24 = 1.440. Sin eso, cada una de las 8 tareas del shuffle de la deduplicación escribe en cada fecha y se llega a 11.520 archivos.
- **Bronze no se compacta en el lugar (actualiza D3):** si se reescriben los archivos, `_spark_metadata/` queda apuntando a archivos que ya no existen. La compactación pasa a Silver, que reescribe cada fecha en un solo archivo.
- **La idempotencia del stream sale del checkpoint y de `_spark_metadata/` (actualiza D6):** se descarta el anti-join contra Bronze porque el file sink no admite lógica por lote. Si un duplicado llega después de que venció el watermark, lo bloquea la regla de `event_id` único en Silver (D7).
- **`spark.sql.shuffle.partitions` queda fijo** desde la primera corrida: define cuántas particiones tiene el estado de la deduplicación y se guarda en el checkpoint. Para cambiarlo hay que reprocesar desde cero.

El overwrite dinámico (D10) queda solo para Silver y Gold, que son batch.

---

## D14 · Top-N: **`TRUNCATE` y recarga completa** de `org_service_cost_14d`

En `org_service_cost_14d` la clave primaria es `(org_id, total_cost_usd, service)` (D8). Un upsert no reemplaza la fila: si el total cambia (por un evento tardío, una regla nueva o un fixture), la recarga inserta una fila nueva y la vieja sigue ahí. P2 mostraría el mismo servicio dos veces, con dos totales distintos.

Antes de cada carga se vacía la tabla con `TRUNCATE` desde `cassandra-driver`, porque el connector de Spark solo inserta. Son **262 filas**: las 262 combinaciones org × servicio que existen en el dataset, y todas tienen actividad en los últimos 14 días. El máximo teórico de 480 (80 × 6) no se alcanza.

**Orden:** (1) calcular el Top-N en Spark y materializarlo, (2) `TRUNCATE`, (3) escribir. Si el cálculo falla, la tabla queda como estaba. Entre los pasos 2 y 3 la tabla está vacía, algo aceptable en una carga batch sin lectores concurrentes.

| Alternativa | Por qué no |
|---|---|
| Sacar `total_cost_usd` de la clave | P2 tendría que ordenar en el cliente o usar `ALLOW FILTERING` (D8) |
| `DELETE ... WHERE org_id = ?` antes de cargar cada org | El resultado es el mismo con 80 sentencias en vez de una. Solo conviene con cargas parciales por org, que no hacemos |

La tabla suma `as_of_date` como columna regular, fuera de la clave, para que quien consulte sepa qué ventana se usó (D15).

`org_daily_usage_by_service` no lo necesita: su clave `(org_id, usage_date, service)` no incluye métricas, así que la recarga es un upsert puro.

---

## D15 · Fecha de referencia `as_of_date` y fechas en **UTC**

Las ventanas de "últimos N días" se calculan contra `as_of_date`, nunca contra `current_date()`. Se toma de `analysis.as_of_date` en la config; si está vacío, se usa `max(event_date)` de Silver: **2025-08-31**.

Con `current_date()`, en noviembre de 2026 los últimos 14 días no tienen eventos y P2 devuelve vacío. Además, el resultado cambiaría según el día de la corrida, lo que rompe la idempotencia (O6).

Las ventanas incluyen los dos extremos: 14 días = `[as_of − 13, as_of]` = 18/08 → 31/08; 30 días = 02/08 → 31/08. El `as_of_date` usado se guarda en el registro de corridas (D11) y en la tabla del Top-N (D14).

**UTC:** `spark.sql.session.timeZone=UTC` en los dos perfiles. El `timestamp` del JSON viene en UTC (`...Z`), pero `to_date(event_ts)` usa la zona horaria de la sesión. En una máquina configurada en Argentina (UTC−3), los **5.455 eventos (12,6 %)** registrados entre las 00:00 y las 02:59 UTC caerían en el día anterior. Dejarían de coincidir las 60 particiones, los conteos por fecha de O4 y las cifras del perfilado, que cortó por el prefijo del string (o sea, en UTC). Colab corre en UTC y una máquina local normalmente no: es justo el tipo de diferencia entre perfiles que D17 tiene que eliminar.

---

## D16 · Quarantine de eventos lleva `event_date`

Quarantine se particiona por `dq_rule` (D10, D12) y guarda `event_date` como columna, derivada con la misma expresión que en Bronze (`to_date(event_ts)` en UTC, D15). Actualiza la fila de Quarantine en la tabla de D11: columnas de Bronze, `dq_rule`, `rejected_ts` y `event_date`.

Sin esa columna, el control `count(bronze[d]) == count(silver[d]) + count(quarantine[d])` (O4) solo se puede hacer sobre el total y no por fecha: un error que mueva eventos de un día a otro pasaría sin detectarse.

Para que la ecuación cierre:

- **Fecha nula:** si `event_ts` es nulo, `event_date` también lo es. El control trata el nulo como una fecha más (join null-safe con `<=>`) para no dejar esas filas fuera de la cuenta. Hoy el dataset no tiene timestamps nulos.
- **Una sola regla por fila:** si un evento viola dos reglas `BLOQUEA`, entra a Quarantine una sola vez, con la primera en este orden: `event_id_null` → `event_id_duplicate` → `value_not_castable`.
- **Duplicados:** de cada `event_id` repetido queda en Silver la fila con menor `ingest_ts`; las demás van a Quarantine.
- **Reemplazo completo:** Silver recalcula las 60 fechas en cada corrida (43.200 filas, segundos), así que Quarantine de eventos se reescribe completa y no por partición. Con un overwrite dinámico por `dq_rule`, una regla que deja de tener rechazos conservaría filas viejas. El reproceso incremental por fecha queda para el backlog.

Implementado en `src/quality/` (`rules.py`, `events.py`) y verificado en `tests/test_quality.py`: en el lake principal, 43.200 válidos, 0 en Quarantine y 0 fechas sin balance.

---

## D17 · Dos perfiles de ejecución: **local** y **cloud** (actualiza D2)

| | Local · fase 1, principal | Cloud · fase 2 |
|---|---|---|
| Ejecución | Docker Compose (`docker-compose.yml`): contenedor `spark` con Python 3.11, Java 17 y PySpark 3.5.9 en `local[*]`. Alternativa: venv propio con esas versiones | Notebook en Colab |
| Data Lake | `data/datalake/` en el repo, montado en el contenedor (las zonas generadas ya están en `.gitignore`) | Se decide en la fase 2, ver abajo |
| Serving | Cassandra 4.1.12 en el mismo Compose | AstraDB |
| Config | `config/pipeline.local.yaml` | `config/pipeline.cloud.yaml` |

**Por qué cambia D2:** D2 fijaba Colab como único entorno. Desarrollar ahí obliga a reinstalar todo en cada sesión y a depender de la conectividad con Astra. Cassandra en Docker se levanta sin cuenta ni token y permite iterar rápido. La consigna acepta Colab "o un entorno equivalente validado" (§2.2). Colab + Astra se mantiene como segundo perfil porque la entrega final exige demo remota, y además puede volverse obligatorio en esta entrega según la respuesta a B4.

**Por qué Spark también va en Docker y no en un venv:** PySpark 3.5 no soporta oficialmente Java 21 ni Python 3.12+, que son justamente las versiones que traen hoy macOS y Homebrew (la primera máquina en la que se probó tenía Python 3.14 y Java 21). Con el contenedor, los cinco integrantes y quien corrija ejecutan con las mismas versiones en macOS, Windows o Linux, y el único requisito del "entorno limpio" (§6.4) pasa a ser Docker. El venv queda como alternativa para quien ya tenga Python 3.11 y Java 17. Uso y problemas conocidos en [`infra/README.md`](../infra/README.md).

**Versiones:** fijas en `requirements.txt` (PySpark 3.5.9, cassandra-driver 3.30.1) y en `infra/spark/Dockerfile` (`spark-cassandra-connector_2.12:3.5.1`, que se descarga al construir la imagen). `infra/check_env.py` las verifica (B2, B3).

**Reglas de portabilidad** que el código de la fase 1 tiene que cumplir:

1. **Ninguna ruta ni host hardcodeado:** todo sale de la config, que admite variables de entorno con valor por defecto (`host: "${CASSANDRA_HOST:-localhost}"`). Así el mismo perfil `local` sirve dentro del contenedor (Compose define `CASSANDRA_HOST=cassandra`) y desde un venv (`localhost`). Las rutas relativas se resuelven contra la raíz del repo y no contra el directorio de trabajo, para que el notebook (que corre en `notebooks/`) y pytest (que corre en la raíz) usen el mismo lake.
2. **Elección del perfil:** con `PIPELINE_PROFILE=local|cloud` o por argumento. Si no se indica, `local`.
3. **Un solo punto de conexión:** `serving.provider: cassandra | astradb` y una única función que arma las opciones tanto para Spark como para `cassandra-driver`:

   | | `cassandra` | `astradb` |
   |---|---|---|
   | Spark connector | `spark.cassandra.connection.host` y `.port` | `spark.cassandra.connection.config.cloud.path` (bundle), `spark.cassandra.auth.username=token`, `spark.cassandra.auth.password` desde `ASTRA_DB_TOKEN` |
   | `cassandra-driver` | `Cluster([host], port=port)` | `Cluster(cloud={"secure_connect_bundle": ruta}, auth_provider=PlainTextAuthProvider("token", token))` |

4. **La lógica vive en `src/`:** el notebook solo importa y llama, nunca define transformaciones.
5. **Credenciales solo por variables de entorno** (`ASTRA_DB_TOKEN`). El YAML guarda la ruta del bundle, nunca el token. Si la variable falta, el pipeline falla con un mensaje claro antes de crear la sesión de Spark.
6. **Una base y dos perfiles versionados:** `pipeline.base.yaml` tiene todo lo común y `pipeline.local.example.yaml` / `pipeline.cloud.example.yaml` solo lo que cambia, para que los perfiles no se desincronicen. Reemplazan a `pipeline.example.yaml`. Si existe una copia sin `.example` (en `.gitignore`), tiene prioridad; si no, se usa la plantilla, así el perfil local corre sin copiar nada. El cargador rechaza cualquier clave `token`, `password` o `secret` con un valor escrito.
7. **Keyspace:** en Docker lo crea `cql/01_keyspace.cql`; en Astra se crea desde la consola, porque Astra no permite `CREATE KEYSPACE` por CQL. `cql/02_tables.cql` es el mismo en los dos.

**Data Lake en Colab** (se cierra en la fase 2). La inclinación es el disco de la sesión. D2 elegía Drive para no perder Bronze ni los checkpoints entre sesiones, pero la demostración de idempotencia son dos corridas seguidas en la misma sesión y el lake se regenera desde Landing en minutos. Drive montado no garantiza renames atómicos, y el checkpoint y `_spark_metadata/` dependen de ellos para confirmar cada lote.

---

## D18 · Trigger `availableNow` para el pipeline, `processingTime` para la demo

El pipeline usa `trigger(availableNow=True)` + `awaitTermination()`: procesa todo lo que está disponible al arrancar en **24 micro-lotes** (120 archivos con `maxFilesPerTrigger=5`) y termina solo, así que el notebook pasa a Silver sin intervención.

| Trigger | Respeta `maxFilesPerTrigger` | Termina solo |
|---|---|---|
| `processingTime="10 seconds"` | Sí | No: hay que pararlo a mano |
| `once=True` | No: un solo lote con los 120 archivos. Deprecado desde Spark 3.4 | Sí |
| **`availableNow=True`** | **Sí** | **Sí** |

Mantener los micro-lotes importa: así el watermark avanza entre lotes y aparece late data para medir (D6, D20).

`processingTime="10 seconds"` queda para la demo de near real-time (O7): se corre contra el sandbox de D20, se agrega un archivo y se muestra que aparece en Bronze en el micro-lote siguiente.

**Medido:** la corrida completa procesa 24 lotes con datos en 13 s, y la re-ejecución no lee nada en 4 s. Como la consulta tiene estado, Spark agrega al final un lote **sin datos** que solo avanza el watermark: no se cuenta como lote.

**Métricas:** se leen de `query.recentProgress`, que tiene todos los lotes, y no de `lastProgress`, que solo tiene el último. Se suman `numInputRows`, `stateOperators[].numRowsDroppedByWatermark` y `stateOperators[].customMetrics.numDroppedDuplicateRows`, y el total va al registro de corridas (D11). `recentProgress` guarda 100 lotes por defecto (`spark.sql.streaming.numRecentProgressUpdates`), más que los 24 de una corrida.

---

## D19 · `value` nulo: **se conserva sin imputar**

877 eventos (2,03 %) llegan con `value` nulo: 399 de `requests`, 283 de `storage_gb_hours` y 195 de `cpu_hours`, repartidos en las 60 fechas. **Todos tienen `cost_usd_increment`:** suman 2.968 USD, el 2,01 % del costo total.

- **El evento pasa a Silver** con `value` nulo y la marca `value_null` en `dq_flags`. No es una regla `BLOQUEA`: `value_not_castable` (D7) se aplica solo cuando `value IS NOT NULL AND try_cast(value AS DOUBLE) IS NULL`. Sin esa condición, los 877 eventos irían a Quarantine y se llevarían su costo.
- **El costo se suma siempre.**
- **Las métricas de uso** (`requests`, `cpu_hours`, `storage_gb_hours`) suman solo los valores no nulos, que es lo que `sum` hace por defecto. El mart agrega `value_null_events`, con la cantidad de eventos sin medición en cada grupo.
- **Si todos los eventos de un grupo tienen `value` nulo, la métrica queda nula y no en 0.** Pasa en 68 de las 9.634 combinaciones `(org_id, usage_date, service)` que tienen eventos de `requests`.

| Alternativa | Por qué no |
|---|---|
| Imputar 0 | Afirma "0 requests" en un grupo que generó costo, subestima la métrica y no se distingue de un cero real |
| Imputar la mediana del recurso | Inventa una medición que la fuente no tiene |
| Mandar a Quarantine | Pierde 2.968 USD de costo que sí es válido |

Se aplica el mismo criterio que con `unit` (D7), aunque el resultado es el opuesto: `unit` se imputa porque se deduce de la métrica sin ambigüedad, mientras que `value` no se puede deducir de nada.

---

## D20 · Fixtures en un **sandbox** separado del lake principal

Las dos reglas `BLOQUEA` tienen 0 violaciones en el dataset real (D7) y el watermark de 60 días descarta 0 eventos (D6). Si se corren solo los datos reales, Quarantine queda vacía y no se ve late data. La consigna pide "quarantine con muestras" (§6.2) y "ejemplos verificables" (§6.4).

Los fixtures son JSONL chicos versionados en `data/fixtures/`, fuera de Landing. Se procesan en un lake aparte, `data/sandbox/`, con el mismo código y otra raíz en la config. El lake principal procesa solo los 43.200 eventos reales.

| Opción | Ventaja | Problema |
|---|---|---|
| **Sandbox separado** | O2 (43.200 de 43.200) y O4 se verifican sobre los datos reales sin descontar fixtures | Una corrida más |
| Fixtures en la corrida principal | Una sola corrida | Landing no se puede modificar (§3.2), así que hace falta un segundo origen. Los conteos dejan de coincidir con el perfilado y cada control tiene que filtrar por `source_file` |
| Solo pytest | Simple | No deja Quarantine en Parquet ni progreso del stream como evidencia |

**Secuencia en el sandbox:**

1. Copiar 5 archivos reales y `fixtures_calidad.jsonl` a `data/sandbox/landing/usage_events_stream/`.
2. Correr el pipeline con `availableNow`. Como esos archivos traen eventos del 31/08, el watermark queda en 31/08 − 60 días = **02/07**.
3. Agregar `fixtures_tardios.jsonl` y volver a correr el stream. El watermark se guarda en el checkpoint, así que el archivo nuevo se evalúa contra él.

| Fixture | Dónde se ve | Resultado esperado |
|---|---|---|
| `event_id` nulo (un solo evento) | Quarantine `event_id_null` | 1 fila |
| `event_id` igual al de un evento real de los 5 archivos copiados | Deduplicación del stream en Bronze | `numDroppedDuplicateRows` = 1; no llega a Silver |
| `value` = `"abc"` y `"1,5"` | Quarantine `value_not_castable` | 2 filas |
| `cost_usd_increment` = −5.0 | Silver, marca de costo negativo (`MARCA`) | 1 fila marcada |
| `unit` nula con `value` | Silver, `unit` imputada y marcada | 1 fila |
| Evento del 15/07 en el lote tardío | Bronze: dentro del watermark | Entra (late data aceptada) |
| Evento del 15/06 en el lote tardío | Bronze: fuera del watermark | `numRowsDroppedByWatermark` = 1 |

Hay **un solo** evento con `event_id` nulo porque la deduplicación agrupa todos los nulos como una misma clave: con dos, uno se perdería en Bronze sin llegar a Quarantine. Verificado en `tests/test_stream_events.py`, que también confirma que el watermark se recupera del checkpoint entre corridas: un evento del 15/07 que llega en una corrida posterior entra, y uno del 15/06 se descarta. La regla `event_id_duplicate` de Silver no se puede alcanzar por el stream mientras el watermark retenga la clave, así que se prueba en pytest con un DataFrame armado a mano.

**Evidencia:** las muestras de `data/sandbox/quarantine/` y las métricas del stream van a `evidence/entrega2/`. En el lake principal, que Quarantine esté vacía es el resultado esperado y se documenta así, porque coincide con las 0 violaciones del perfilado (O5). Se escribe igual y se lee con esquema explícito: sin archivos Parquet, Spark no puede inferirlo. `data/sandbox/` va a `.gitignore`.

---

## D21 · Features en una tabla Silver diaria; Gold solo publica

`silver/usage_daily_features`, con grano `(org_id, usage_date, service)` y particionada por `usage_date`: `daily_cost_usd`, `requests`, `cpu_hours`, `storage_gb_hours`, `genai_tokens` y `carbon_kg`, más `events`, `value_null_events` y `cost_anomaly_events`.

La consigna pide las features en Silver (§6.2). Si se calcularan de nuevo en Gold, la misma agregación viviría en dos lugares. Gold (`org_daily_usage_by_service`, Top-N y anomalías) lee esta tabla y agrega `processed_ts`, sin volver a agregar eventos.

**Métricas de uso** (D19): valen 0 si el grupo no tuvo eventos de esa métrica, y null si los tuvo pero todos llegaron sin `value`. En `requests` son 68 grupos.

**Medido:** 11.050 filas con 11.050 claves distintas (el grano de D8), costo total de 147.433,98 USD (igual a la suma de Landing), 0 huérfanos, 0 fechas sin balance (O4). `silver/usage_events` compacta Bronze: pasa de 1.440 archivos (18 MB) a 60 (3,5 MB).

---

## Hallazgos del perfilado

47.312 filas, 12,6 MB, 60 días (03/07 → 31/08/2025). Detalle completo en `evidence/profiling_landing.md`.

**Lo que está bien:** integridad referencial perfecta (cero huérfanos en las 6 fuentes que referencian organizaciones, los 400 `resource_id` existen). Cero duplicados de PK. Cero fechas inválidas. Dominios ya conformados: no hay variantes tipo `us_east` / `US-East`. Ningún evento tiene `service` o `region` distintos de los de su recurso.

En consecuencia, el join con `dim_resource` **no** es necesario para conformar; se realiza igualmente como control de calidad y para traer `state` y `tags_json`.

**Encontrado al implementar Bronze (segunda entrega):** `resources.csv` escapa las comillas como `""` (RFC 4180), mientras que Spark espera `\` por defecto. Afecta a los 317 `tags_json` no nulos, y en 211 (los que tienen varias etiquetas) la fila se corre de columna. Con el modo por defecto (`PERMISSIVE`) habría pasado sin aviso. Los maestros se leen con `escape='"'`, en modo `FAILFAST` y validando la cabecera contra el esquema (`enforceSchema=false`), así que un problema como este hace fallar el job en lugar de corromper Bronze.

**Lo que está roto:** ver D7. Más `credits` nulo en billing (57 %, se interpreta como 0), `resolved_at` nulo en tickets (24 %, son tickets abiertos, no un defecto) y `last_login` nulo en users (17 %).

**Sin skew:** de las 28.800 claves posibles de `(org_id, date, service)` se materializan 11.050, con mediana de 3 eventos y un máximo de 15. Ninguna partición domina el tiempo total.

---

## Decisiones abiertas

Decisiones no tomadas a la fecha, con su fecha límite y la opción preferida.

| Pregunta | Se decide antes de | Inclinación |
|---|---|---|
| ¿Data Lake de Colab en el disco de la sesión o en Drive? | Fase 2 | Disco de la sesión (D17) |
| ¿Herramienta de visualización? | 07/12 | Notebook con matplotlib |
| ¿Airflow o notebook secuencial? | 07/12 | Secuencial: Airflow en Colab es overhead sin beneficio |

### Cerradas en la segunda entrega

| Pregunta | Decisión |
|---|---|
| ¿AstraDB o Cassandra en Docker? | Las dos, en fases: Cassandra en Docker para el perfil local y AstraDB para el perfil cloud (D17) |
| ¿El componente de ML es anomalías o churn? | Anomalías con MAD (D9): reutiliza el análisis del perfilado y alimenta `cost_anomaly_mart`, que es un mart que la consigna ya pide |
| ¿SCD tipo 2 en `dim_org`? | No. `customers_orgs.csv` es una sola foto, sin historia de cambios de plan: `dim_org` es SCD tipo 1 y se reemplaza en cada carga |

---

## Qué no está en este archivo

Los **supuestos**, los **riesgos con sus mitigaciones** y la **estimación de esfuerzo** viven en
[`plan_inicial.md`](plan_inicial.md), que es el artefacto 5 de la entrega. Acá quedan solo las
decisiones técnicas y la evidencia que las sostiene.
