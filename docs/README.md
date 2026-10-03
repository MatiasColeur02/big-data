# docs/

| Archivo | Qué es |
|---|---|
| **[`estado_entrega1.md`](estado_entrega1.md)** | **Tablero de control**: qué pide la consigna, qué está hecho y qué falta |
| [`roadmap.md`](roadmap.md) | Qué hay que entregar y en qué orden hacerlo |
| [`decisions.md`](decisions.md) | Las 10 decisiones técnicas, con su justificación y evidencia |
| [`diseno_v1.md`](diseno_v1.md) | **Artefacto 1** · Documento de diseño que se entrega el 05/10 (se exporta a PDF) |
| [`plan_inicial.md`](plan_inicial.md) | **Artefacto 5** · Supuestos, riesgos, esfuerzo, roles, recursos y próximos pasos |
| [`matriz_requisito_componente.md`](matriz_requisito_componente.md) | **Artefacto 4** · Trazabilidad preguntas / requisitos / 5V → componentes |
| `arquitectura_v1.svg` · `.png` | **Artefacto 3** · Diagrama de arquitectura v1 |

El diagrama se edita en el `.svg`, que es texto plano y versiona bien en git; draw.io lo importa
si prefieren editarlo ahí. Para regenerar el PNG desde la raíz del repo:

```bash
python3 -c "import cairosvg; cairosvg.svg2png(url='docs/arquitectura_v1.svg', write_to='docs/arquitectura_v1.png', scale=2, background_color='white')"
```
