# Trazabilidad de la segunda entrega

**Fecha límite: lunes 16/11/2026 · 18:30 h** · Última revisión: 12/11/2026

Relación entre lo que pide la consigna para la segunda evaluación y el archivo del repositorio que lo cubre.

Estados: **Cubierto** · **Parcial** · **Pendiente**

---

## 1. Artefactos de entrega (§6.3)

| # | Artefacto | Archivo | Estado | Observación |
|---|---|---|---|---|
| 1 | Repositorio con código reproducible, Bronze/Silver/Gold en Parquet, scripts CQL y README Quickstart | `src/` · `src/serving/cql/` · [`../README.md`](../README.md) | Cubierto | — |
| 2 | Diagrama actualizado | [`arquitectura_v2.svg`](arquitectura_v2.svg), exportado a PNG | Cubierto | — |
| 3 | Log de decisiones | [`decisions.md`](decisions.md) | Cubierto | D1 patrón · D3 y D13 particiones · D8 y D14 claves · D9 umbral |
| 4 | Evidencias de ejecución | [`../evidence/entrega2/`](../evidence/entrega2/) | Parcial | Falta la captura de las consultas en la consola de Astra |
| 5 | Backlog priorizado | [`backlog_final.md`](backlog_final.md) | Cubierto | — |

Documentos de apoyo:

| Archivo | Contenido |
|---|---|
| [`plan_correcciones.md`](plan_correcciones.md) | Feedback de la primera entrega y correcciones al diseño v1 |
| [`gobierno.md`](gobierno.md) | Gobierno preliminar |
| [`matriz_requisito_componente.md`](matriz_requisito_componente.md) | Trazabilidad requisitos → componentes, con los archivos reales |

---

## 2. Alcance mínimo (§6.2)

| Componente | Archivo | Estado | Evidencia |
|---|---|---|---|
| Correcciones y arquitectura | `plan_correcciones.md` · `arquitectura_v2.svg` | Cubierto | — |
| Batch a Bronze | `src/ingest/batch_masters.py` | Cubierto | 7 maestros, 4.112 de 4.112 filas |
| Streaming a Bronze | `src/ingest/stream_events.py` | Cubierto | 43.200 de 43.200 · late data y dedupe en `quality_sandbox/` |
| Silver | `src/silver/` | Cubierto | Eventos, `dim_org`, `dim_resource` y 6 features |
| Calidad | `src/quality/` | Cubierto | `quality/` · `quality_sandbox/` |
| Gold | `src/gold/marts.py` | Cubierto | 11.050 filas en `org_daily_usage_by_service` |
| Serving | `src/serving/` | Cubierto | `serving/queries_cassandra.md` · `serving/queries_astradb.md` |
| Analítica o ML | `src/gold/anomalies.py` | Cubierto | `gold/anomalies.md` · 134 anomalías |
| Idempotencia | `src/idempotency.py` | Cubierto | `idempotencia/` · 3 corridas, 0 duplicados |
| Gobierno preliminar | `gobierno.md` | Cubierto | — |
| Pruebas y reproducibilidad | README · `config/` · `tests/` | Cubierto | `ejecucion/` · 68 tests |
| Backlog final | `backlog_final.md` | Cubierto | — |

---

## 3. Criterios de aceptación (§6.4)

| Criterio | Estado | Sustento |
|---|---|---|
| Batch y streaming corren con los datos provistos | Cubierto | `ejecucion/pipeline_log.txt` |
| Reglas de calidad y quarantine con ejemplos verificables | Cubierto | `quality_sandbox/` (D20) |
| Mart FinOps en Gold y tabla Cassandra/AstraDB poblada | Cubierto | 11.050 filas en Gold, Cassandra y Astra |
| Dos consultas sobre AstraDB con resultados | Cubierto | `serving/queries_astradb.md` |
| La re-ejecución no duplica registros | Cubierto | `idempotencia/` |
| El diagrama representa lo implementado | Cubierto | `arquitectura_v2.svg` |
| El README permite ejecutar desde un entorno limpio | Parcial | Falta probarlo desde un clon limpio |

---

## 4. Checklist de entrega (§9.2)

- [x] Correcciones de la primera entrega incorporadas
- [x] Arquitectura actualizada
- [x] Tres maestros batch en Bronze
- [x] Streaming con esquema, watermark, dedupe, late data y checkpoint
- [x] Silver para eventos y al menos un maestro
- [x] Tres features calculadas
- [x] Tres reglas de calidad y quarantine
- [x] Mart `org_daily_usage_by_service` en Gold
- [x] Keyspace y tabla Cassandra/AstraDB poblada
- [x] Dos consultas CQL con resultados
- [x] Idempotencia demostrada
- [x] README Quickstart, logs y evidencias
- [x] Backlog final priorizado

Cierre del repositorio:

- [x] Sin credenciales versionadas
- [ ] Quickstart probado desde un clon limpio
- [ ] Captura de las consultas en la consola de Astra
- [ ] Tag `v2.0-entrega2`
