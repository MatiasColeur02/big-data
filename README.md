# Cloud Provider Analytics

Proyecto Integrador · **Big Data · ITBA · 2C 2026** · Prof. Diego Mosquera

Pipeline de ETL, streaming y serving para analítica de FinOps, Soporte y Producto sobre datos de un proveedor de nube. PySpark + Structured Streaming + Parquet + Cassandra/AstraDB.

| | |
|---|---|
| Entrega actual | **Primera entrega · 28/09/2026** — diseño y fundación de datos |
| Estado | Diseño. Todavía no hay pipeline ejecutable (es el alcance de la entrega 2) |
| Equipo | 5 integrantes |
---

## Estructura

```
.
├── README.md
├── config/                   ← configuración externalizada, sin credenciales
├── data/                     ← dataset provisto por la cátedra (landing)
├── docs/                     ← todo el material de la entrega
│   ├── estado_entrega1.md    ← tablero de control
│   ├── decisions.md · roadmap.md
│   ├── arquitectura_v1.svg / .png
│   ├── diseno_v1.md
│   └── matriz_requisito_componente.md
├── evidence/                 ← logs, capturas y salidas de cada entrega
├── infra/                    ← scripts de entorno (entrega 2+)
├── notebooks/                ← exploración y perfilado
├── src/                      ← ingesta, procesamiento y serving (entrega 2+)
│   ├── common/               ← conformance compartido entre batch y streaming
│   ├── ingest/  quality/  silver/  gold/  serving/
└── tests/
```

---

## Arquitectura en una línea

**Patrón Lambda.**

```
                    ┌──────────────────┐
  usage_events ────►│ Structured       │──┐
  (120 JSONL)       │ Streaming        │  │
                    └──────────────────┘  │   ┌────────┐   ┌────────┐   ┌───────────┐
                                          ├──►│ BRONZE │──►│ SILVER │──►│   GOLD    │──► Cassandra
                    ┌──────────────────┐  │   │Parquet │   │Parquet │   │  5 marts  │    AstraDB
  7 CSV maestros ──►│ Batch (PySpark)  │──┘   └────────┘   └────────┘   └───────────┘
                    └──────────────────┘            │
                                                    ▼
                                              QUARANTINE
```

---

## Reproducir el perfilado

```bash
git clone <url-del-repo>
pip install pandas
jupyter notebook notebooks/01_profiling_landing.ipynb
```

El notebook lee **solo** desde `data/datalake/landing/`, no escribe nada. Su salida está versionada en `evidence/profiling_landing.md`.

---

## Dataset

Dataset sintético provisto por la cátedra: **47.312 filas, ~13 MB**.

| Fuente | Filas | Grano |
|---|---|---|
| `usage_events_stream/*.jsonl` | 43.200 (120 × 360) | 1 evento de uso |
| `marketing_touches.csv` | 1.500 | 1 interacción |
| `support_tickets.csv` | 1.000 | 1 ticket |
| `users.csv` | 800 | 1 usuario |
| `resources.csv` | 400 | 1 recurso |
| `billing_monthly.csv` | 240 | 1 factura (org × mes) |
| `nps_surveys.csv` | 92 | 1 encuesta |
| `customers_orgs.csv` | 80 | 1 organización |