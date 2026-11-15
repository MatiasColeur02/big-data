# Plan de correcciones

Correcciones posteriores a la primera entrega, con prioridad, responsable, fecha objetivo y evidencia esperada. Es lo que pide la consigna como "Plan de correcciones".

---

## 1. Feedback de la primera entrega

| | |
|---|---|
| Puntaje de rúbrica | 9,40 / 10 |
| Penalización IA | 0,00 |
| Nota final | 9 / 10 |
| Devolución | "Presentaron una fundación técnica muy sólida y ordenada. Hay coherencia entre el problema, la arquitectura y la organización del repositorio." |

El feedback no marca correcciones puntuales. Las de abajo salieron de implementar el diseño v1 y medir. Cada una es una decisión nueva en [`decisions.md`](decisions.md), y la decisión que reemplaza quedó marcada como "Actualizada por Dxx".

---

## 2. Correcciones

Prioridad **Alta**: sin la corrección el pipeline da un resultado incorrecto sin error.

| # | Corrección | Prioridad | Responsable | Fecha | Evidencia | Estado |
|---|---|---|---|---|---|---|
| C1 | Bronze de eventos con file sink en `append`, no `foreachBatch` con overwrite: el overwrite pisaba los lotes anteriores | Alta | Data Lake | 09/11 | D13 · 43.200 de 43.200 en Bronze | Hecho |
| C2 | Idempotencia del stream por checkpoint, sin anti-join contra Bronze | Media | Data Lake | 09/11 | D13 · `idempotencia/` | Hecho |
| C3 | Compactación en Silver en vez de en Bronze | Media | Data Lake | 09/11 | D13, D21 · 1.440 archivos a 60 | Hecho |
| C4 | `TRUNCATE` antes de recargar el Top-N: con upsert, un total distinto creaba una fila nueva | Alta | Arquitectura | 09/11 | D14 · `check_astra.txt` | Hecho |
| C5 | Ventanas de días contra `as_of_date` y no contra la fecha actual | Alta | Procesamiento | 09/11 | D15 · P2 = 5 filas | Hecho |
| C6 | Sesión de Spark en UTC: en UTC−3 el 12,6 % de los eventos caía en el día anterior | Alta | Procesamiento | 09/11 | D15 | Hecho |
| C7 | `event_date` en Quarantine, para controlar O4 por fecha | Media | Datos y calidad | 09/11 | D16 · 60 de 60 fechas | Hecho |
| C8 | Dos perfiles, local con Docker y cloud con Colab + Astra, en lugar de Colab + Drive | Alta | Arquitectura | 10/11 | D17, D22 · `cloud/` | Hecho |
| C9 | Trigger `availableNow` para el pipeline | Media | Data Lake | 09/11 | D18 | Hecho |
| C10 | `value` nulo pasa a Silver marcado, sin imputar | Alta | Datos y calidad | 09/11 | D19 | Hecho |
| C11 | Sandbox con fixtures para mostrar las reglas y el late data | Media | Datos y calidad | 09/11 | D20 · `quality_sandbox/` | Hecho |
| C12 | Features en Silver; Gold solo publica | Baja | Procesamiento | 09/11 | D21 | Hecho |
| C13 | CSV con `escape='"'` y `FAILFAST`: `resources.csv` corría columnas sin error | Alta | Data Lake | 09/11 | `tests/test_batch_masters.py` | Hecho |
| C14 | Diagrama v2 con solo lo implementado | Media | Arquitectura | 12/11 | `arquitectura_v2.svg` | Hecho |
| C15 | Matriz requisito-componente con los archivos reales | Baja | Documento y repo | 12/11 | `matriz_requisito_componente.md` | Hecho |
| C16 | README con el estado actual y el Quickstart | Media | Documento y repo | 12/11 | `README.md` | Hecho |
| C17 | Decisiones abiertas cerradas: AstraDB y Docker, anomalías, sin SCD2 | Baja | Arquitectura | 10/11 | `decisions.md` | Hecho |
