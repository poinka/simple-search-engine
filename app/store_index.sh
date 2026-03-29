#!/bin/bash
set -euo pipefail

source /app/.venv/bin/activate

echo "Waiting for Cassandra..."
for i in $(seq 1 30); do
  if python - <<'PY'
from cassandra.cluster import Cluster
cluster = Cluster(["cassandra-server"])
session = cluster.connect()
session.shutdown()
cluster.shutdown()
print("Connected to Cassandra")
PY
  then
    break
  fi
  echo "Cassandra is not ready yet. Retry ${i}/30..."
  sleep 5
done

echo "Loading index from HDFS into Cassandra..."
python /app/store_index.py

echo "Done loading index into Cassandra."