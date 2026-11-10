# Sandbox de calidad y late data (D20)

Generado por `python -m src.quality.sandbox`: 5 archivos reales + `data/fixtures/`.

| Paso | Resultado |
|---|---|
| Corrida 1 del stream | 1807 filas leídas, 1 duplicado(s) descartado(s), watermark `2025-07-02T23:51:00.000Z` |
| Corrida 2 (tardíos) | 2 filas leídas, 1 descartada(s) por watermark |
| Bronze | 1807 filas, 1806 `event_id` distintos |
| Calidad | 1804 válidos, 3 en Quarantine |
| Bloqueos por regla | {'event_id_null': 1, 'event_id_duplicate': 0, 'value_not_castable': 2} |
| Marcas por regla | {'cost_negative': 9, 'unit_imputed': 78, 'value_null': 45} |
| Fechas sin balance (O4) | 0 |

Muestras: [`quarantine_samples.md`](quarantine_samples.md) · conteos: [`quality_report.json`](quality_report.json)
