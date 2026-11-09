# Entorno de ejecución del pipeline: Python 3.11 + Java 17 + PySpark 3.5 (D17).
# Base bookworm y no la etiqueta por defecto: Debian 13 (trixie) ya no trae OpenJDK 17.
FROM python:3.11-slim-bookworm

ARG SPARK_CASSANDRA_PACKAGE=com.datastax.spark:spark-cassandra-connector_2.12:3.5.1

# procps: los scripts de arranque de Spark usan `ps`.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/java-17

ENV JAVA_HOME=/usr/lib/jvm/java-17 \
    TZ=UTC \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    SPARK_CASSANDRA_PACKAGE=${SPARK_CASSANDRA_PACKAGE} \
    SPARK_IVY_DIR=/opt/ivy

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Resuelve el connector y sus dependencias al construir la imagen, para que las corridas
# no dependan de Maven Central.
RUN python -c "from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]') \
.config('spark.jars.packages', '${SPARK_CASSANDRA_PACKAGE}') \
.config('spark.jars.ivy', '${SPARK_IVY_DIR}') \
.getOrCreate().stop()"

# El repo se monta acá. PYTHONPATH permite `import src...` desde notebooks/ y desde tests/.
WORKDIR /workspace
ENV PYTHONPATH=/workspace

EXPOSE 8888 4040
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--ServerApp.root_dir=/workspace", "--IdentityProvider.token=", "--ServerApp.password="]
