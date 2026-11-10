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

## Entorno de ejecución

Solo hace falta **Docker Desktop**: Python 3.11, Java 17, PySpark 3.5.9 y Cassandra 4.1 vienen en
contenedores (D17).

```bash
git clone <url-del-repo>
cd big-data
make up      # o: docker compose up -d --build  → Jupyter en http://localhost:8888
make check   # verifica versiones y la conexión Spark ↔ Cassandra
```

Todos los comandos, la alternativa sin Docker y la solución de problemas están en
[`infra/README.md`](infra/README.md).

---

## Perfil cloud: Colab + AstraDB

El mismo código corre contra **AstraDB** cambiando solo la config (D17, D22). Necesita una base
Astra con el keyspace `cloud_analytics` (se crea desde la consola de Astra, no por CQL), un token
`AstraCS:...` y el secure connect bundle (`.zip`). **Ni el token ni el bundle van al repo.**

**En Google Colab**

1. Abrir `notebooks/02_pipeline.ipynb` en Colab (*File → Open notebook → GitHub*).
2. En el panel de Secrets (ícono de llave), crear `ASTRA_DB_TOKEN` con el token y activar
   *Notebook access*. Si no está, la celda lo pide por prompt sin mostrarlo.
3. Correr la celda **Setup de Colab**: pide el bundle (se guarda en `/content`, fuera del repo),
   instala Java 17 y las dependencias fijadas, clona el repo y deja `PIPELINE_PROFILE=cloud`.
4. *Runtime → Run all*. El lake queda en el disco de la sesión y se regenera desde Landing (D22).
5. Antes de commitear una copia del notebook: *Edit → Clear all outputs* (`tests/test_notebooks.py`
   falla si quedan salidas).

**Desde Docker, sin Colab**

```bash
cp .env.example .env                       # completar ASTRA_DB_TOKEN
cp ~/Downloads/secure-connect-*.zip secure-connect-cloud-analytics.zip   # raíz del repo, ignorado
make check-astra                           # escritura, lectura, upsert y TRUNCATE en Astra
make pipeline PROFILE=cloud                # pipeline completo con serving en Astra
make idempotency PROFILE=cloud             # prueba O6 → evidence/entrega2/idempotencia/astradb.md
```

Evidencia del perfil cloud: [`evidence/entrega2/cloud/`](evidence/entrega2/cloud/).

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
