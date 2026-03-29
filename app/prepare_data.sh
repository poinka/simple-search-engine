#!/bin/bash
set -euo pipefail

source /app/.venv/bin/activate
export PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python
unset PYSPARK_PYTHON

PARQUET_FILE=$(find /app -maxdepth 1 -type f -name "*.parquet" | sort | head -n 1 || true)
if [ -z "${PARQUET_FILE}" ]; then
  echo "No parquet file found in /app. Put one parquet shard in app/ before running."
  exit 1
fi

export PARQUET_FILE
PARQUET_BASENAME=$(basename "${PARQUET_FILE}")

mkdir -p /app/data
rm -rf /app/data/*

hdfs dfs -rm -r -f /parquet || true
hdfs dfs -mkdir -p /parquet
hdfs dfs -put -f "${PARQUET_FILE}" "/parquet/${PARQUET_BASENAME}"

python /app/prepare_data.py

hdfs dfs -rm -r -f /data || true
hdfs dfs -rm -r -f /input/data || true
hdfs dfs -mkdir -p /data
hdfs dfs -mkdir -p /input

find /app/data -type f -name "*.txt" -print0 | while IFS= read -r -d '' file; do
    hdfs dfs -put -f "$file" /data/
done

python /app/build_input_data.py

echo "Parquet in HDFS:"
hdfs dfs -ls /parquet

echo "Documents in HDFS /data:"
hdfs dfs -ls /data | head -20

echo "Prepared input in HDFS /input/data:"
hdfs dfs -ls /input/data
hdfs dfs -cat /input/data/part-* | head -5 || true