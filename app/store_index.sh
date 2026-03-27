#!/bin/bash
set -e

source /app/venv/bin/activate

echo "Checking Cassandra..."
python - <<'PY'
from cassandra.cluster import Cluster
cluster = Cluster(["cassandra-server"])
session = cluster.connect()
print("Connected to Cassandra")
session.shutdown()
cluster.shutdown()
PY

echo "Loading index from HDFS into Cassandra..."
python /app/store_index.py

echo "Done loading index into Cassandra."