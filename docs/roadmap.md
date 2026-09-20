# Roadmap · Primera entrega

**Fecha límite: lunes 28/09/2026 · 18:30 h** · Hoy: 20/09 · Quedan 8 días.

## Qué hay que entregar

La consigna (§5.3 y checklist §9.1) pide cinco artefactos:

| Artefacto | Archivo |
|---|---|
| Documento de diseño | `docs/diseno_v1.md` → PDF |
| Repositorio versionado | este repo, tag `v1.0-entrega1` |
| Diagrama de arquitectura v1 | `docs/arquitectura_v1.png` |
| Matriz requisito-componente | `docs/matriz_requisito_componente.md` |
| Plan inicial (supuestos, riesgos, esfuerzo) | sección del documento de diseño |
| Evidencia de exploración de datos | `notebooks/01_profiling_landing.ipynb` + `evidence/` |

**Esta entrega es de diseño, no de implementación.** La consigna lo dice explícitamente: *"no se espera una implementación profunda ni over-engineering en esta etapa"*. No se escriben jobs de Spark, no se crea el keyspace, no se escribe Parquet. Lo único que se ejecuta es el notebook de perfilado.

---

## Los pasos, en orden

### 1 · Entender el caso y las fuentes
Leer la consigna completa y correr el notebook de perfilado sobre las 8 fuentes. De acá salen los números que después sostienen todo: cuántas filas, qué está roto, qué dominios hay, cómo se distribuyen los eventos. **Sin esto no se puede decidir nada**, porque cada decisión de arquitectura tiene que justificarse con un dato, no con una intuición.

Salida: `evidence/profiling_landing.md`.

### 2 · Definir el problema y los objetivos
Quiénes son los usuarios (FinOps, Soporte, Producto), qué preguntas tienen que poder responder y cómo se mide el éxito. Las preguntas se derivan directo de las 5 consultas obligatorias de la consigna §7.4: si el diseño las responde, el problema está bien planteado.

### 3 · Justificar Big Data con las 5V
Con los números del paso 1 más una proyección a escala real declarada como supuesto. El error a evitar: decir "hay mucho dato" sobre un dataset de 13 MB. El argumento honesto es que el dataset es una muestra de enseñanza y la arquitectura se diseña para la trayectoria del caso.

### 4 · Elegir el patrón arquitectónico
Batch, Lambda, Kappa o híbrido, con justificación. Esto condiciona todo lo que sigue, así que se decide antes de dibujar nada.

### 5 · Diseñar el Data Lake
Zonas (Landing/Bronze/Silver/Gold + Quarantine), formato, particionamiento, naming, retención y reglas de promoción entre zonas. El particionamiento hay que justificarlo con números de filas y tamaños de archivo, no con "por fecha porque sí".

### 6 · Diseñar los flujos batch y streaming
Qué fuente entra por dónde, con qué esquema, qué configuración de streaming y qué reglas de calidad. Acá se definen los esquemas explícitos y el tratamiento de la evolución v1→v2.

### 7 · Escribir la lógica MapReduce
Expresar el flujo batch principal como map / combiner / shuffle / reduce y mostrar su equivalencia con el `groupBy().agg()` de Spark. Es un punto propio de la consigna (§5.2.9) y de la rúbrica (10 %).

### 8 · Dibujar el diagrama de arquitectura v1
Recién acá, cuando las decisiones ya están tomadas. Fuentes, ingesta, zonas, procesamiento, serving, consumo y capacidades transversales. **El diagrama tiene que coincidir con lo que dice el documento**: es el error de consistencia más fácil de cometer y el más fácil de detectar por el docente.

### 9 · Armar la matriz requisito-componente
Cada requisito de la consigna §4.4 mapeado a un componente propuesto, más la relación entre las 5V y las decisiones de arquitectura.

### 10 · Listar supuestos, riesgos y decisiones abiertas
Incluyendo estimación de esfuerzo. Las decisiones que todavía no están tomadas se declaran como abiertas: la consigna las pide explícitamente (§5.2.10), y esconderlas es peor que admitirlas.

### 11 · Redactar el documento de diseño
Integrar los pasos 2 a 10 en un solo documento conciso y visual. La consigna pide que "permita una revisión rápida", así que tablas y diagramas antes que párrafos largos.

### 12 · Control contra el checklist y congelamiento
Verificar los 12 ítems del checklist §9.1 uno por uno, cada uno contra un archivo concreto. Después congelar el repo, exportar el PDF, tagear y ensayar la defensa.
