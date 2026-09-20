# Estado de la primera entrega

**Fecha límite: lunes 28/09/2026 · 18:30 h** · Última revisión: 20/09/2026

Tablero de control: qué pide la consigna para esta instancia, dónde está cada cosa y qué falta.

🟢 listo · 🟡 empezado, falta terminar · 🔴 no arrancado

---

## 1. Los cinco artefactos de entrega (§5.3)

Cada artefacto de la consigna, en un archivo.

| # | Artefacto | Archivo | Estado | Qué falta |
|---|---|---|---|---|
| 1 | **Documento de diseño** | [`diseno_v1.md`](diseno_v1.md) → exportar a `.pdf` | 🔴 | Ocho de sus diez secciones |
| 2 | **Repositorio** | [`../README.md`](../README.md) + estructura del repo | 🟢 | — |
| 3 | **Diagrama de arquitectura v1** | [`arquitectura_v1.svg`](arquitectura_v1.svg) · `.png` | 🟢 | — |
| 4 | **Matriz requisito-componente** | [`matriz_requisito_componente.md`](matriz_requisito_componente.md) | 🟢 | Los objetivos medibles, cuando exista el punto 1 |
| 5 | **Plan inicial** | [`plan_inicial.md`](plan_inicial.md) | 🟡 | Las horas de la estimación de esfuerzo |

**Archivos de apoyo**, que no son artefactos en sí pero sostienen a los cinco:

| Archivo | Para qué sirve |
|---|---|
| [`decisions.md`](decisions.md) | Las 10 decisiones técnicas con su justificación y evidencia |
| [`roadmap.md`](roadmap.md) | Qué hay que entregar y en qué orden hacerlo |
| [`../evidence/profiling_landing.md`](../evidence/profiling_landing.md) | Perfilado del dataset · la evidencia de exploración que pide §5.2.12 |
| [`../notebooks/01_profiling_landing.ipynb`](../notebooks/01_profiling_landing.ipynb) | El notebook que genera esa evidencia |

---

## 2. Resumen

**Casi todo lo que falta es redacción, no decisión.** Las diez decisiones técnicas están tomadas, el perfilado está corrido, el diagrama está hecho y la matriz está completa. Lo que queda es volcar eso al documento de diseño.

Queda **un solo hueco de contenido real**: la **interpretación del problema** —usuarios, preguntas principales y objetivos medibles—, que es el punto 1 del alcance y no tiene nada escrito. Es además lo primero que se lee del documento y cae en la dimensión de la rúbrica que pesa 15 %.

Hay también un hueco chico de diseño: **metadatos** de las zonas del Data Lake, que aparece en el punto 7 y ninguna decisión cubre.

---

## 3. El alcance obligatorio, punto por punto (§5.2)

| # | Punto | Dónde está | Estado | Qué falta |
|---|---|---|---|---|
| 1 | Interpretación del problema, usuarios, preguntas y objetivos medibles | — | 🔴 | Todo |
| 2 | Justificación de Big Data con las 5V | `diseno_v1.md` §2 · `matriz` parte C | 🟡 | Las tablas están; falta redactar el argumento |
| 3 | Inventario y perfil de fuentes: grano, frecuencia, tipos, calidad, trazabilidad y riesgos | `../evidence/profiling_landing.md` | 🟡 | Los números están medidos; falta la tabla por fuente con grano y frecuencia |
| 4 | Diagrama de arquitectura de alto nivel | `arquitectura_v1.svg` | 🟢 | — |
| 5 | Selección justificada del patrón | `decisions.md` D1 | 🟡 | Decidido y argumentado; falta pasarlo al documento |
| 6 | Mapeo de requisitos a componentes y relación 5V ↔ decisiones | `matriz_requisito_componente.md` | 🟢 | — |
| 7 | Data Lake: zonas, formatos, particiones, naming, retención, metadatos y promoción | `decisions.md` D3 y D10 | 🟡 | **Metadatos** no está cubierto. Falta redactar el resto |
| 8 | Flujos batch y streaming, con herramientas específicas | `decisions.md` D2, D4, D6, D7 | 🟡 | Decidido; falta redactar el recorrido |
| 9 | Flujo batch expresado con lógica MapReduce | `diseno_v1.md` §7 | 🟡 | Borrador completo; falta revisarlo y cerrarlo |
| 10 | Supuestos, riesgos, mitigaciones y decisiones abiertas | `plan_inicial.md` §§1-2 · `decisions.md` | 🟢 | — |
| 11 | Estimación de esfuerzo, roles y recursos | `plan_inicial.md` §3 | 🟡 | Roles y recursos listos; faltan las horas |
| 12 | Repositorio inicial con README, convenciones y evidencia de exploración | repo + `../evidence/` | 🟢 | — |

**Cuatro verdes, siete amarillos, un rojo.** Los amarillos son casi todos "está decidido pero no escrito".

---

## 4. Criterios de aceptación (§5.4)

| Criterio | Estado | Comentario |
|---|---|---|
| El problema, los usuarios y los criterios de éxito están formulados sin ambigüedad | 🔴 | Depende del punto 1 |
| La arquitectura responde a los requisitos y distingue claramente batch y streaming | 🟢 | El diagrama separa las dos ramas por color |
| Las zonas del Data Lake, formatos y particiones son coherentes con los datos provistos | 🟢 | D3 justifica el particionado con números medidos |
| El flujo MapReduce muestra cómo se resolvería el procesamiento batch del caso | 🟡 | Borrador en el documento |
| Los supuestos y riesgos son realistas y tienen mitigaciones propuestas | 🟢 | En `plan_inicial.md` |
| El repositorio y la documentación permiten continuar sin rehacer la fundación | 🟢 | — |

---

## 5. Cuánto pesa cada cosa (§5.5)

| Dimensión | Peso | Dónde estamos |
|---|---|---|
| Arquitectura: coherencia, responsabilidades, flujos y decisiones | **25 %** | 🟢 fuerte |
| Data Lake: zonas, formatos, particiones, metadatos y retención | **20 %** | 🟡 falta metadatos y redacción |
| Comprensión y justificación: problema, usuarios, preguntas, 5V y criterios de éxito | **15 %** | 🔴 **el punto débil** |
| Datos: inventario, perfil, calidad inicial y trazabilidad | **15 %** | 🟡 medido, falta redactar |
| Procesamiento batch: lógica y correspondencia con MapReduce | 10 % | 🟡 borrador |
| Ingeniería y documentación: repo, claridad, versionado, supuestos | 10 % | 🟢 |
| Defensa y respuesta al feedback | 5 % | — |

El único rojo cae justo en el 15 % de "comprensión y justificación", que además es la primera sección del documento: si queda floja, tiñe la lectura de lo que viene después, que es la parte fuerte.

---

## 6. Lo que falta, en orden

1. **Escribir la interpretación del problema.** Usuarios, qué pregunta se hace cada uno y objetivos medibles. Las preguntas salen de las cinco consultas obligatorias de §7.4.
2. **Redactar el documento de diseño** volcando lo ya decidido a las secciones 3 a 8. Es copiar y ordenar, no pensar de nuevo.
3. **Definir los metadatos** de cada zona del Data Lake.
4. **Completar las horas** de la estimación en `plan_inicial.md`.
5. **Revisar el borrador de MapReduce** y cerrarlo.
6. **Exportar a PDF**, verificar contra el checklist de abajo y tagear `v1.0-entrega1`.

---

## 7. Checklist final (§9.1)

Se tilda antes de congelar, cada ítem contra un archivo concreto.

- [ ] Documento de diseño disponible en el canal de entrega
- [ ] Repositorio accesible y versionado
- [ ] Interpretación del caso y objetivos medibles
- [ ] Análisis 5V
- [ ] Inventario y perfil de fuentes
- [ ] Arquitectura v1 y patrón justificado
- [ ] Diseño Landing/Bronze/Silver/Gold
- [ ] Flujos batch y streaming
- [ ] Lógica MapReduce o equivalente
- [ ] Matriz requisito-componente
- [ ] Supuestos, riesgos, mitigaciones y estimación de esfuerzo
- [ ] Evidencia mínima de lectura/exploración de datos

### Higiene del repositorio

- [ ] Borrar `../evidence/PENDIENTE.md`, que ya no aplica
- [ ] Sacar `.idea/` y los `.DS_Store` del índice de git: `git rm -r --cached .idea` y `git rm --cached '**/.DS_Store'`
- [ ] Verificar que no haya credenciales commiteadas
- [ ] Tag `v1.0-entrega1` una vez congelado

---

## Después del 28/09

La clase del 28 se usa para feedback. Con eso hay que versionar un **plan de correcciones** con prioridad, responsable, fecha objetivo y evidencia esperada, que es lo que alimenta la segunda entrega del 16/11. Lo pide §5.6.
