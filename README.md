# Cloud Provider Analytics

Proyecto Integrador · **Big Data · ITBA · 2C 2026** · Prof. Diego Mosquera

Pipeline de ETL, streaming y serving para analítica de FinOps, Soporte y Producto sobre datos de un proveedor de nube. PySpark + Structured Streaming + Parquet + Cassandra/AstraDB.

|                     |                                                                                                                                                                                      |
|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Entrega actual      | **Segunda entrega · 16/11/2026** · implementación técnica                                                                                                                            |
| Estado              | Pipeline ejecutable de punta a punta: Landing → Bronze → Silver → Gold → Cassandra/AstraDB, con P1 y P2 respondidas. Detalle en [`docs/estado_entrega2.md`](docs/estado_entrega2.md) |
| Entregas anteriores | Primera · 05/10/2026 · diseño (tag `v1.0-entrega1`, nota 9/10)                                                                                                                       |
| Equipo              | Valentina Marti Reta · Nicanor Porto · Matías Coleur · Federico Etchegorry · Julieta Techenski                                                                                       |
---

## Estructura

```
.
├── README.md
├── config/                   ← configuración externalizada, sin credenciales
├── data/                     ← dataset provisto por la cátedra (landing)
├── docs/                     ← todo el material de la entrega
│   ├── estado_entrega2.md    ← trazabilidad consigna → archivos
│   ├── arquitectura_v2.svg / .png  ← lo implementado
│   ├── decisions.md          ← D1–D22
│   ├── gobierno.md           ← controles, metadatos, linaje, acceso, observabilidad
│   ├── plan_correcciones.md  ← feedback de la 1.ª entrega → cambios
│   ├── backlog_final.md      ← obligatorio / deseable / fuera de alcance
│   ├── matriz_requisito_componente.md
│   └── (1.ª entrega) estado_entrega1.md · diseno_v1 · arquitectura_v1 · plan_inicial.md
├── evidence/entrega2/        ← calidad, quarantine, gold, serving, idempotencia, cloud
├── infra/                    ← Dockerfiles, chequeos de entorno y emulador de Colab
├── notebooks/                ← 01 perfilado · 02 pipeline completo
├── src/
│   ├── common/               ← config, sesión de Spark, esquemas, registro de corridas
│   ├── ingest/  quality/  silver/  gold/  serving/
│   ├── pipeline.py           ← orquesta las 6 etapas
│   └── idempotency.py        ← prueba O6
├── tests/                    ← pytest (68 casos)
├── docker-compose.yml  ·  Makefile  ·  requirements.txt
```

---

## Arquitectura

**Patrón Lambda.** Diagrama completo, con solo lo implementado: [`docs/arquitectura_v2.png`](docs/arquitectura_v2.png).

```
                    ┌──────────────────┐
  usage_events ────►│ Structured       │──┐
  (120 JSONL)       │ Streaming        │  │
                    └──────────────────┘  │   ┌────────┐   ┌────────┐   ┌───────────┐
                                          ├──►│ BRONZE │──►│ SILVER │──►│   GOLD    │──► Cassandra
                    ┌──────────────────┐  │   │Parquet │   │Parquet │   │  3 marts  │    AstraDB
  7 CSV maestros ──►│ Batch (PySpark)  │──┘   └────────┘   └────────┘   └───────────┘
                    └──────────────────┘            │
                                                    ▼
                                              QUARANTINE
```

---

## Quickstart (perfil local)

Solo hace falta **Docker Desktop**, con al menos 4 GB de RAM. Python 3.11, Java 17, PySpark 3.5.9 y
Cassandra 4.1 vienen en contenedores (D17). El perfil local no necesita cuenta ni token.

```bash
# 1. Clonar
git clone <url-del-repo>
cd big-data

# 2. Levantar Spark + Cassandra (la primera vez construye la imagen: unos minutos)
make up            # Jupyter en http://localhost:8888 · Spark UI en http://localhost:4040
make check         # versiones + escritura, lectura, upsert y TRUNCATE en Cassandra → "Entorno listo."

# 3. Correr el pipeline completo desde cero (~1 minuto)
make clean         # borra zonas generadas y checkpoints; Landing no se toca
make pipeline      # Bronze → calidad → Silver → Gold → Cassandra, con tabla de estado por etapa

# 4. Ver los resultados
make queries       # P1 y P2 con cqlsh y con el driver → evidence/entrega2/serving/
make cqlsh         # consola CQL interactiva (USE cloud_analytics;)

# 5. Pruebas
make test          # pytest dentro del contenedor
make idempotency   # 3 corridas seguidas → evidence/entrega2/idempotencia/cassandra.md

# 6. Apagar
make down          # conserva los datos de Cassandra
make reset         # borra también los datos de Cassandra
```

Salida esperada de `make pipeline`: las 6 etapas en `ok`, 43.200 eventos en Bronze y Silver, 0 en
Quarantine, y 11.050 y 262 filas en Gold y en Cassandra.

También se puede correr por etapa (`make bronze`, `make quality`, `make silver`, `make gold`,
`make serving`) o desde `notebooks/02_pipeline.ipynb`. Los comandos equivalentes en
`docker compose`, la alternativa sin Docker y los problemas conocidos están en
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
cd big-data

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

---

## Logs y errores

Cada etapa escribe en consola con hora y nivel, y cada job agrega una línea a
`data/datalake/_metadata/runs/runs.jsonl` con filas leídas, escritas y rechazadas, duración y estado.
Si una etapa falla, el pipeline se detiene ahí, la registra como `failed` y muestra el error. Los
errores de config, de variables de entorno faltantes y de conexión con Cassandra o Astra traen un
mensaje que dice qué revisar. Ejemplo de corrida: [`evidence/entrega2/ejecucion/`](evidence/entrega2/ejecucion/).

---

## Limitaciones

- Gold tiene 3 de los 5 marts y el serving responde P1 y P2. El resto está en [`docs/backlog_final.md`](docs/backlog_final.md).
- Quarantine queda vacía en el lake principal porque el dataset no viola las reglas que bloquean; las reglas se prueban con fixtures (D20).
- El watermark es de 60 días porque cada archivo trae los 60 días mezclados (D6).
- Corre en un solo nodo (`local[*]`).
