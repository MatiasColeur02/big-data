# Cloud Provider Analytics

Proyecto Integrador · **Big Data · ITBA · 2C 2026** · Prof. Diego Mosquera

Pipeline de ETL, streaming y serving para analítica de FinOps, Soporte y Producto sobre datos de un proveedor de nube. PySpark + Structured Streaming + Parquet + Cassandra/AstraDB.

| | |
|---|---|
| Entrega actual | **Primera entrega · 05/10/2026** (postergada desde el 28/09) — diseño y fundación de datos |
| Estado | Diseño. Todavía no hay pipeline ejecutable (es el alcance de la entrega 2) |
| Equipo | Valentina Marti Reta · Nicanor Porto · Matías Coleur · Federico Etchegorry · Julieta Techenski |
---

## Estructura

```
.
├── README.md
├── config/                   ← configuración externalizada, sin credenciales
├── data/                     ← dataset provisto por la cátedra (landing)
├── docs/                     ← todo el material de la entrega
│   ├── estado_entrega1.md    ← trazabilidad consigna → archivos
│   ├── decisions.md
│   ├── arquitectura_v1.svg / .png
│   ├── diseno_v1.md / .pdf
│   ├── matriz_requisito_componente.md
│   └── plan_inicial.md
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

Requiere **Python 3.9 o superior**. En macOS conviene usar un entorno virtual: el Python del
sistema es gestionado por el SO y `pip install` directo falla con `externally-managed-environment`.

```bash
git clone <url-del-repo>
cd tp-big-data

python3 -m venv .venv         
source .venv/bin/activate    

pip install --upgrade pip
pip install pandas tabulate jupyter

jupyter notebook notebooks/01_profiling_landing.ipynb
```

Para salir del entorno, `deactivate`. Para volver a entrar en otra sesión, solo
`source .venv/bin/activate`. La carpeta `.venv/` está en `.gitignore`, así que no se versiona.

En Windows el único cambio es la activación: `.venv\Scripts\activate`.

**Dependencias:** `pandas` para el perfilado, `tabulate` porque el notebook exporta las tablas con
`.to_markdown()`, y `jupyter` para ejecutarlo.

El notebook lee **solo** desde `data/datalake/landing/`, no escribe nada ahí y tarda menos de un
minuto. Su salida se escribe en `evidence/profiling_landing.md`, que está versionada.

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