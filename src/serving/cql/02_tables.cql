-- Tablas del serving, modeladas query-first: una tabla por consulta (D8).
-- Idempotente (IF NOT EXISTS). Corre igual en los dos perfiles (D17):
--   Docker   lo aplica cassandra-init al levantar, y load_cassandra.py antes de cada carga
--   AstraDB  lo aplica load_cassandra.py con cassandra-driver, o se pega en la consola CQL
-- El keyspace se crea en 01_keyspace.cql (Docker) o desde la consola (Astra).
USE cloud_analytics;

-- P1 · Costos y requests diarios por organización y servicio en un rango de fechas.
-- Grano del mart de Gold: (org_id, usage_date, service). org_id como partition key porque la
-- consulta filtra por organización (~138 filas por partición); usage_date DESC primero en el
-- clustering para resolver el rango de fechas con un slice contiguo. La clave no incluye
-- métricas, así que la recarga es un upsert puro (D14).
CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
    org_id              text,
    usage_date          date,
    service             text,
    daily_cost_usd      double,
    requests            double,
    cpu_hours           double,
    storage_gb_hours    double,
    genai_tokens        bigint,
    carbon_kg           double,
    events              bigint,
    value_null_events   bigint,
    cost_anomaly_events bigint,
    processed_ts        timestamp,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);

-- P2 · Top-N servicios por costo acumulado en los últimos 14 días para una organización.
-- Cassandra no ordena por una columna agregada: Spark pre-agrega la ventana [as_of − 13, as_of]
-- (D15) y el total va como clustering key DESC, así `LIMIT N` devuelve el Top-N ya ordenado (D8).
-- Como el total es parte de la clave, la recarga hace TRUNCATE antes de escribir (D14).
CREATE TABLE IF NOT EXISTS org_service_cost_14d (
    org_id         text,
    total_cost_usd double,
    service        text,
    total_requests double,
    active_days    int,
    rank           int,
    window_start   date,
    as_of_date     date,
    processed_ts   timestamp,
    PRIMARY KEY ((org_id), total_cost_usd, service)
) WITH CLUSTERING ORDER BY (total_cost_usd DESC, service ASC);
