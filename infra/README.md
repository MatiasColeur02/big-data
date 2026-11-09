# infra/

Entorno local completo con Docker Compose (D17). Lo único que hay que instalar es
**Docker Desktop** (o Docker Engine + Compose v2): Python, Java, PySpark y Cassandra vienen en
los contenedores, con las versiones fijas.

| Servicio | Imagen | Qué hace |
|---|---|---|
| `cassandra` | `cassandra:4.1.12` | Serving. Puerto `127.0.0.1:9042`. Datos en el volumen `cassandra_data` |
| `cassandra-init` | `cassandra:4.1.12` | Cuando Cassandra está sana, aplica `src/serving/cql/01_keyspace.cql` y `02_tables.cql`, y termina |
| `spark` | `cloud-analytics-spark:3.5.9` (de `infra/spark/Dockerfile`) | Python 3.11 + Java 17 + PySpark 3.5.9 + `spark-cassandra-connector` 3.5.1. Corre JupyterLab y sirve para comandos sueltos |

El repo se monta en `/workspace` dentro de `spark`: los cambios en el código se ven al instante y
las zonas que genera el pipeline quedan en `data/datalake/` del repo (ignoradas por git).

## Uso

| Acción | Con `make` | Sin `make` (Windows) |
|---|---|---|
| Levantar todo y abrir Jupyter en <http://localhost:8888> | `make up` | `docker compose up -d --build` |
| Verificar el entorno | `make check` | `docker compose run --rm spark python infra/check_env.py` |
| Correr los tests | `make test` | `docker compose run --rm spark pytest` |
| Terminal en el contenedor de Spark | `make shell` | `docker compose run --rm spark bash` |
| `cqlsh` contra Cassandra | `make cqlsh` | `docker compose exec cassandra cqlsh` |
| Ver logs | `make logs` | `docker compose logs -f` |
| Apagar (conserva los datos de Cassandra) | `make down` | `docker compose down` |
| Apagar y borrar los datos de Cassandra | `make reset` | `docker compose down -v` |

La primera vez `make up` tarda unos minutos: construye la imagen (~1,1 GB) y Cassandra demora
~1 minuto en quedar healthy. Después, `up` tarda segundos.

`make check` verifica los bloqueantes B2 y B3: versiones de Python, Java y PySpark, escritura y
lectura con el connector (dos veces, para comprobar que el upsert no duplica) y `TRUNCATE` con
`cassandra-driver`. Salida esperada:

```
Versiones
  OK     Python 3.11.x
  OK     Java 17
  OK     PySpark 3.5.9
Cassandra
  OK     cassandra-driver conectado a cassandra:9042, keyspace `cloud_analytics` presente
  OK     SparkSession con com.datastax.spark:spark-cassandra-connector_2.12:3.5.1
  OK     connector: 2 escrituras de 3 filas, 3 leídas (upsert por clave primaria)
  OK     cassandra-driver: TRUNCATE deja la tabla vacía (D14)
Entorno listo.
```

## Problemas conocidos

| Síntoma | Causa | Solución |
|---|---|---|
| `apt-get` falla con `At least one invalid signature was encountered` al construir | El disco virtual de Docker Desktop está lleno | `docker system df` y liberar espacio (`docker builder prune`, `docker image prune`) o agrandar el disco en Settings → Resources |
| `cassandra` no queda healthy o se reinicia | Poca memoria para Docker (usa ~1,8 GB) | Darle al menos 4 GB a Docker Desktop |
| `port is already allocated` en 8888, 4040 o 9042 | Otro Jupyter, Spark o Cassandra corriendo en el host | Cerrarlo o cambiar el puerto del lado izquierdo en `docker-compose.yml` |
| `no hay conexión con Cassandra` en `make check` | Cassandra todavía arrancando | Esperar a que `docker compose ps` la muestre `healthy` |

## Sin Docker (venv)

Funciona igual si se tiene **Python 3.11 y Java 17** en el host. Con Python 3.12+ o Java 21
PySpark 3.5 no está soportado oficialmente.

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d cassandra cassandra-init   # Cassandra sigue en Docker
python infra/check_env.py                        # CASSANDRA_HOST vale localhost por defecto
```
