#!/bin/bash
set -euo pipefail

INDEXER_VENV=/app/.venv-indexer
export CASSANDRA_HOST=${CASSANDRA_HOST:-cassandra-server}

if [ ! -f "${INDEXER_VENV}/bin/activate" ]; then
  echo "Indexer virtual environment not found at ${INDEXER_VENV}"
  exit 1
fi

source "${INDEXER_VENV}/bin/activate"

echo "Waiting for Cassandra at ${CASSANDRA_HOST}..."
for i in $(seq 1 30); do
  if python - <<'PY'
import os
from cassandra.cluster import Cluster

host = os.environ.get("CASSANDRA_HOST", "cassandra-server")
cluster = Cluster([host])
session = cluster.connect()
session.shutdown()
cluster.shutdown()
print(f"Connected to Cassandra at {host}")
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