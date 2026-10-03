# Documento de Diseño v1 · Cloud Provider Analytics

**Primera entrega · 05/10/2026** · ITBA · Big Data 2C 2026

> ESQUELETO. Cada sección indica de dónde sale el contenido. Se redacta una vez que las
> decisiones están congeladas. Mantenerlo conciso y visual: la consigna
> pide que "permita una revisión rápida".

## 1. Interpretación del problema
Contexto, usuarios (FinOps / Soporte / Producto), preguntas principales y objetivos medibles.
Las preguntas se derivan de las 5 consultas obligatorias de la consigna §7.4.

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
