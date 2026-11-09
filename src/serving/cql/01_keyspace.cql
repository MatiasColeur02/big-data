-- Keyspace del serving local (Cassandra en Docker). Lo aplica el servicio cassandra-init.
-- En AstraDB el keyspace se crea desde la consola con el mismo nombre: Astra no permite
-- CREATE KEYSPACE por CQL (D17). SimpleStrategy con RF=1 porque el clúster local tiene un nodo.
CREATE KEYSPACE IF NOT EXISTS cloud_analytics
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
