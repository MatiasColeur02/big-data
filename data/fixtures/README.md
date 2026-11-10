# data/fixtures/

Eventos sintéticos para demostrar las reglas de calidad y el manejo de late data (D20). Están
fuera de Landing, que es inmutable (§3.2), y nunca se mezclan con el lake principal: se procesan
en `data/sandbox/` con `python -m src.quality.sandbox`.

| Archivo | `event_id` | Caso | Resultado esperado |
|---|---|---|---|
| `fixtures_calidad.jsonl` | `null` | `event_id` nulo | Quarantine `event_id_null` |
| | `evt_nwlh5boz2guf` | Repite un evento real de `events_part_0000.jsonl` | Bronze lo descarta en la deduplicación del stream |
| | `fx_value_texto` | `value = "abc"` | Quarantine `value_not_castable` |
| | `fx_value_coma` | `value = "1,5"` | Quarantine `value_not_castable` |
| | `fx_costo_negativo` | `cost_usd_increment = -5.0` | Silver con `cost_negative` y `cost_anomaly = true` |
| | `fx_unit_nula` | `unit` nula con `value` | Silver con `unit = hours` imputada y `unit_raw` nula |
| | `fx_value_nulo` | `value` nulo | Silver con `value_null`, costo conservado (D19) |
| `fixtures_tardios.jsonl` | `fx_tarde_dentro` | Evento del 15/07, llega en una corrida posterior | Entra a Bronze (dentro del watermark) |
| | `fx_tarde_fuera` | Evento del 15/06, llega en una corrida posterior | Descartado: `numRowsDroppedByWatermark = 1` |

Hay un solo evento con `event_id` nulo porque la deduplicación agrupa los nulos como una misma
clave (verificado en `tests/test_stream_events.py`).
