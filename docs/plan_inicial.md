# Plan inicial

Supuestos, riesgos con sus mitigaciones, estimación de esfuerzo y próximos pasos. Es lo que pide la consigna como "Plan inicial", y cubre los puntos 10 y 11 del alcance obligatorio.

---

## 1. Supuestos

| # | Supuesto | Si no se cumple |
|---|---|---|
| 1 | El dataset no cambia entre hoy y el 07/12 | Re-correr el notebook de perfilado y revisar D4, D6 y D7, que dependen de cifras medidas |
| 2 | `credits` nulo en `billing_monthly` significa cero, no "desconocido" | Cambia el cálculo de revenue de la consulta 4 |
| 3 | Los tickets sin `resolved_at` están abiertos, no perdidos | Cambia la tasa de resolución y el mart de Soporte |
| 4 | Los tipos de cambio de ARS (~0,0015) son reales y no errores de carga | Corregirlos inflaría el revenue argentino por ~650 |
| 5 | Los `resource_id` de los eventos siempre existen en `resources.csv` | Hoy se cumple (400 de 400); igual se implementa un left join con marca de huérfano |
| 6 | La proyección de volumen de las 5V es ilustrativa, no una medición | Se declara como supuesto en el documento; no sostiene ninguna decisión por sí sola |

---

## 2. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Mitigación |
|---|---|---|---|---|
| R1 | Un watermark corto descarta el 93 % de los eventos, sin error ni log | Alta | Crítico | D6: watermark de 60 días, con la simulación documentada |
| R2 | El esfuerzo del equipo se desvía hacia la implementación y el documento de diseño queda incompleto | Media | Crítico | Esta entrega es de diseño; la implementación es alcance de la 2.ª |
| R3 | `value` declarado como `DoubleType` pierde el 3,03 % de los datos en silencio | Alta | Alto | D4: leer como string en Bronze y castear en Silver |
| R4 | El revenue queda distorsionado ±15 % por el tipo de cambio de las facturas en USD | Alta | Alto | D7: forzar el FX a 1,0 para USD, conservando el original |
| R5 | El diagrama deja de coincidir con lo que dice el documento | Media | Alto | Control cruzado antes de congelar; el diagrama se actualiza junto con el texto |
| R6 | Colab se desconecta y se pierde el trabajo | Media | Medio | D2: el Data Lake vive en Drive, no en el disco de Colab |
| R7 | Límites del tier gratuito de AstraDB en la segunda entrega | Baja | Medio | Probar la conexión antes del 16/11; Cassandra en contenedor como plan B |
| R8 | Los archivos Parquet quedan demasiado chicos y degradan la lectura | Media | Bajo | D3: no particionar por servicio; compactar Bronze |

R1, R3 y R4 no son hipotéticos: se detectaron midiendo el dataset y están documentados con la cifra exacta en `decisions.md`.

---

## 3. Estimación de esfuerzo, roles y recursos

### Roles

| Área | De qué responde | Artefacto principal |
|---|---|---|
| Arquitectura | Patrón, diagrama, capacidades transversales, matriz | `arquitectura_v1.svg`, `matriz_requisito_componente.md` |
| Datos y calidad | Inventario, perfil de fuentes, reglas de calidad, diccionario | `evidence/profiling_landing.md`, §3 del diseño |
| Data Lake | Zonas, particionamiento, naming, retención, metadatos, promoción | §5 del diseño |
| Procesamiento | Flujos batch y streaming, esquemas explícitos, lógica MapReduce | §§6 y 7 del diseño |
| Documento y repo | Estructura, README, convenciones, redacción e integración final | `diseno_v1.md`, este plan |

### Esfuerzo

Estimado en **tallas relativas** y no en horas: a esta altura del proyecto el esfuerzo se conoce
mejor en términos de peso comparado entre bloques que en un número de horas que todavía no se
puede predecir.

- **Alto** · varias sesiones, con discusión de equipo
- **Medio** · una o dos sesiones
- **Bajo** · una sesión

Bloques de la primera entrega. Los de las entregas siguientes están en la sección 4.

| Bloque de trabajo | Esfuerzo | Estado |
|---|---|---|
| Lectura de la consigna y del material de clase | Medio | hecho |
| Perfilado del dataset y validación de hallazgos | Medio | hecho |
| Decisiones de arquitectura y diseño del Data Lake | **Alto** | hecho |
| Diagrama de arquitectura v1 | Medio | hecho |
| Matriz de trazabilidad | Medio | hecho |
| Interpretación del problema, usuarios y objetivos | Bajo | hecho |
| Metadatos de las zonas del Data Lake | Bajo | hecho |
| Flujos batch y streaming, y lógica MapReduce | Medio | hecho |
| Redacción e integración del documento de diseño | **Alto** | hecho |
| Revisión contra el checklist y ensayo de defensa | Bajo | pendiente |

### Recursos

| Recurso | Para qué | Costo | Estado |
|---|---|---|---|
| Google Colab | Ejecución de PySpark en `local[*]` | gratuito | disponible |
| Google Drive | Data Lake persistente entre sesiones de Colab | gratuito | disponible |
| GitHub | Repositorio versionado, privado, con el docente invitado | gratuito | creado |
| AstraDB | Serving en Cassandra (2.ª entrega) | tier gratuito | a validar antes del 16/11 |
| PySpark 3.5.x | Motor de procesamiento | — | fijado en D2 |

No hay costos de infraestructura: todo el alcance del proyecto entra en los tiers gratuitos.

---

## 4. Próximos pasos

Bloques de trabajo posteriores a esta entrega, con la instancia en la que se evalúan y su esfuerzo
en las mismas tallas. El alcance sale de la consigna (§5.6, §6.2 y §7.2) y los componentes, de la
parte B de `matriz_requisito_componente.md`.

| # | Bloque de trabajo | Entrega | Esfuerzo | Componente o referencia |
|---|---|---|---|---|
| 1 | Plan de correcciones a partir del feedback: prioridad, responsable, fecha objetivo y evidencia esperada | Posterior a la 1.ª | Bajo | Consigna §5.6 |
| 2 | Validar la conexión con AstraDB y cerrar la decisión de serving | 2.ª · 16/11 | Bajo | R7 · decisiones abiertas |
| 3 | Ingesta batch de al menos tres maestros a Bronze | 2.ª · 16/11 | Medio | `src/ingest/batch_masters.py` |
| 4 | Ingesta streaming de eventos a Bronze, con watermark, deduplicación y checkpoint | 2.ª · 16/11 | **Alto** | `src/ingest/stream_events.py` |
| 5 | Silver de eventos y de al menos un maestro, con joins y tres features | 2.ª · 16/11 | **Alto** | `src/silver/conform_events.py`, `src/silver/features.py` |
| 6 | Reglas de calidad y Quarantine con muestras | 2.ª · 16/11 | Medio | `src/quality/rules.py` |
| 7 | Mart `org_daily_usage_by_service` en Gold | 2.ª · 16/11 | Medio | `src/gold/marts.py` |
| 8 | Keyspace, tabla query-first, carga desde Spark y dos consultas CQL | 2.ª · 16/11 | Medio | `src/serving/load_astra.py` |
| 9 | Componente analítico; la opción preferida es anomalías de costo | 2.ª · 16/11 | Medio | `src/gold/anomalies.py` · D9 |
| 10 | Demostración de idempotencia, pruebas, Quickstart y evidencias de ejecución | 2.ª · 16/11 | Medio | `tests/`, `evidence/`, README |
| 11 | Gobierno preliminar y backlog final priorizado | 2.ª · 16/11 | Bajo | Consigna §6.2 |
| 12 | Maestros restantes en Bronze y Silver | Final · 07/12 | Medio | `src/ingest/batch_masters.py` |
| 13 | Marts restantes y sus tablas de serving, para responder P3 a P5 | Final · 07/12 | **Alto** | `src/gold/marts.py`, `src/serving/load_astra.py` |
| 14 | Gobierno, linaje, seguridad y observabilidad completos; diccionario de datos | Final · 07/12 | Medio | D11 |
| 15 | Presentación ejecutiva, video y defensa oral | Final · 07/12 | Medio | Consigna §7.5 |
