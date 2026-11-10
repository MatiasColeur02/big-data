# config/

| Archivo | Versionado | Qué es |
|---|---|---|
| `pipeline.base.yaml` | Sí | Todo lo común: rutas del lake, Spark, streaming, calidad, anomalías y serving |
| `pipeline.local.example.yaml` | Sí | Perfil local (Docker Compose o venv + Cassandra en Docker) |
| `pipeline.cloud.example.yaml` | Sí | Perfil cloud (Colab + AstraDB) |
| `pipeline.local.yaml`, `pipeline.cloud.yaml` | **No** | Copias editables de las plantillas, con prioridad sobre ellas |

`src/common/config.py` carga `pipeline.base.yaml` y le combina encima el perfil. El perfil se
elige con `PIPELINE_PROFILE=local|cloud` (por defecto `local`). Para cada perfil usa
`pipeline.<perfil>.yaml` si existe y, si no, la plantilla `.example.yaml`: el perfil local corre
sin copiar nada.

**Credenciales:** solo por variables de entorno (`ASTRA_DB_TOKEN`). El cargador rechaza cualquier
clave `token`, `password` o `secret` con un valor escrito en el YAML. Nunca commitear tokens ni el
secure connect bundle de AstraDB: la consigna lo prohíbe (§8.2) y es un criterio de aceptación de
la entrega final.
