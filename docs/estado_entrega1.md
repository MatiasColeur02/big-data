# Trazabilidad de la primera entrega

**Fecha límite: lunes 05/10/2026 · 18:30 h** (postergada desde el 28/09) · Última revisión: 03/10/2026

Relación entre lo que pide la consigna para la primera evaluación y el archivo del repositorio que lo cubre.

Estados: **Cubierto** · **Parcial** · **Pendiente**

---

## 1. Artefactos de entrega (§5.3)

| # | Artefacto | Archivo | Estado | Observación |
|---|---|---|---|---|
| 1 | Documento de diseño | [`diseno_v1.md`](diseno_v1.md), exportado a PDF | Parcial | Redactadas las secciones 2 y 7 |
| 2 | Repositorio | [`../README.md`](../README.md) y estructura del repositorio | Cubierto | — |
| 3 | Diagrama de arquitectura v1 | [`arquitectura_v1.svg`](arquitectura_v1.svg), exportado a PNG | Parcial | PNG sin exportar |
| 4 | Matriz requisito-componente | [`matriz_requisito_componente.md`](matriz_requisito_componente.md) | Parcial | Sin la columna de objetivos medibles |
| 5 | Plan inicial | [`plan_inicial.md`](plan_inicial.md) | Parcial | Sin la sección de próximos pasos |

Documentos de apoyo:

| Archivo | Contenido |
|---|---|
| [`decisions.md`](decisions.md) | Decisiones técnicas D1 a D10, con su justificación y evidencia |
| [`../evidence/profiling_landing.md`](../evidence/profiling_landing.md) | Perfilado del dataset: evidencia de exploración (§5.2, punto 12) |
| [`../notebooks/01_profiling_landing.ipynb`](../notebooks/01_profiling_landing.ipynb) | Notebook que produce el perfilado |

---

## 2. Alcance obligatorio (§5.2)

| # | Punto | Archivo | Estado | Observación |
|---|---|---|---|---|
| 1 | Interpretación del problema, usuarios, preguntas y objetivos medibles | `diseno_v1.md` §1 | Pendiente | — |
| 2 | Justificación de Big Data con las 5V | `diseno_v1.md` §2 · `matriz_requisito_componente.md` parte C | Cubierto | — |
| 3 | Inventario y perfil de fuentes | `../evidence/profiling_landing.md` · `diseno_v1.md` §3 | Parcial | Medido en la evidencia; sin la tabla por fuente en el documento |
| 4 | Diagrama de arquitectura de alto nivel | `arquitectura_v1.svg` | Cubierto | — |
| 5 | Selección justificada del patrón | `decisions.md` D1 · `diseno_v1.md` §4 | Parcial | Justificado en D1; sin incorporar al documento |
| 6 | Mapeo de requisitos a componentes y relación 5V ↔ decisiones | `matriz_requisito_componente.md` | Cubierto | — |
| 7 | Diseño del Data Lake | `decisions.md` D3 y D10 · `diseno_v1.md` §5 | Parcial | Metadatos sin definir; sin incorporar al documento |
| 8 | Flujos batch y streaming, con herramientas específicas | `decisions.md` D2, D4, D5, D6 y D7 · `diseno_v1.md` §6 | Parcial | Decidido; sin incorporar al documento |
| 9 | Flujo batch expresado con lógica MapReduce | `diseno_v1.md` §7 | Cubierto | — |
| 10 | Supuestos, riesgos, mitigaciones y decisiones abiertas | `plan_inicial.md` §§1-2 · `decisions.md` | Cubierto | — |
| 11 | Estimación de esfuerzo, roles y recursos | `plan_inicial.md` §3 | Cubierto | Esfuerzo expresado en tallas relativas |
| 12 | Repositorio inicial con README, convenciones y evidencia de exploración | repositorio · `../evidence/` | Cubierto | — |

---

## 3. Criterios de aceptación (§5.4)

| Criterio | Estado | Sustento |
|---|---|---|
| El problema, los usuarios y los criterios de éxito están formulados sin ambigüedad | Pendiente | `diseno_v1.md` §1 |
| La arquitectura responde a los requisitos y distingue claramente batch y streaming | Cubierto | `arquitectura_v1.svg` · `decisions.md` D1 |
| Las zonas del Data Lake, formatos y particiones son coherentes con los datos provistos | Cubierto | `decisions.md` D3 y D10 |
| El flujo MapReduce muestra cómo se resolvería el procesamiento batch del caso | Cubierto | `diseno_v1.md` §7 |
| Los supuestos y riesgos son realistas y tienen mitigaciones propuestas | Cubierto | `plan_inicial.md` §§1-2 |
| El repositorio y la documentación permiten continuar sin rehacer la fundación | Cubierto | repositorio |

---

## 4. Checklist de entrega (§9.1)

- [ ] Documento de diseño disponible en el canal de entrega
- [x] Repositorio accesible y versionado
- [ ] Interpretación del caso y objetivos medibles
- [x] Análisis 5V
- [ ] Inventario y perfil de fuentes
- [ ] Arquitectura v1 y patrón justificado
- [ ] Diseño Landing/Bronze/Silver/Gold
- [ ] Flujos batch y streaming
- [x] Lógica MapReduce o equivalente
- [ ] Matriz requisito-componente
- [x] Supuestos, riesgos, mitigaciones y estimación de esfuerzo
- [x] Evidencia mínima de lectura/exploración de datos

Cierre del repositorio:

- [x] Sin credenciales, tokens ni datos sensibles versionados
- [ ] Documento de diseño exportado a PDF
- [ ] Tag `v1.0-entrega1`

---

## 5. Después de la entrega (§5.6)

Con el feedback de la primera evaluación se versiona un plan de correcciones con prioridad, responsable, fecha objetivo y evidencia esperada. Ese plan alimenta la arquitectura actualizada de la segunda entrega (16/11/2026).
