# docs/

## Segunda entrega · 16/11/2026

| Archivo | Qué es |
|---|---|
| [`estado_entrega2.md`](estado_entrega2.md) | Trazabilidad: qué pide la consigna para la segunda entrega y qué archivo lo cubre |
| `arquitectura_v2.svg` · `.png` | **Artefacto 2** · Diagrama de arquitectura v2 |
| [`plan_correcciones.md`](plan_correcciones.md) | Feedback de la primera entrega y correcciones |
| [`gobierno.md`](gobierno.md) | Gobierno preliminar |
| [`backlog_final.md`](backlog_final.md) | **Artefacto 5** · Backlog para la entrega final |
| [`decisions.md`](decisions.md) | **Artefacto 3** · Las 22 decisiones técnicas, con su justificación y evidencia |
| [`matriz_requisito_componente.md`](matriz_requisito_componente.md) | Trazabilidad preguntas / requisitos / 5V → componentes |

## Primera entrega · 05/10/2026

| Archivo | Qué es |
|---|---|
| [`estado_entrega1.md`](estado_entrega1.md) | Trazabilidad de la primera entrega |
| [`diseno_v1.md`](diseno_v1.md) | **Artefacto 1** · Documento de diseño, exportado a [`diseno_v1.pdf`](diseno_v1.pdf) |
| [`plan_inicial.md`](plan_inicial.md) | **Artefacto 5** · Supuestos, riesgos, esfuerzo, roles, recursos y próximos pasos |
| `arquitectura_v1.svg` · `.png` | **Artefacto 3** · Diagrama de arquitectura v1 |

El diagrama se edita en el `.svg`, que es texto plano y versiona bien en git; draw.io lo importa
si prefieren editarlo ahí. Para regenerar un PNG desde la raíz del repo (cambiar `v2` por la versión que corresponda):

```bash
python3 -c "import cairosvg; cairosvg.svg2png(url='docs/arquitectura_v2.svg', write_to='docs/arquitectura_v2.png', scale=2, background_color='white')"
```
