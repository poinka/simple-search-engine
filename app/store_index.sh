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
CONNECTED=0

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
    CONNECTED=1
    break
  fi

  echo "Cassandra is not ready yet. Retry ${i}/30..."
  sleep 5
done

if [ "${CONNECTED}" -ne 1 ]; then
  echo "Cassandra did not become ready in time."
  exit 1
fi

echo "Loading index from HDFS into Cassandra..."
python /app/app.py

echo "Done loading index into Cassandra."