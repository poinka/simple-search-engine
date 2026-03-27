#!/bin/bash
set -e

source /app/venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

mkdir -p /app/data
rm -rf /app/data/*

python /app/prepare_data.py

hdfs dfs -rm -r -f /data || true
hdfs dfs -rm -r -f /input/data || true
hdfs dfs -mkdir -p /data
hdfs dfs -mkdir -p /input

find /app/data -type f -name "*.txt" -print0 | while IFS= read -r -d '' file; do
    hdfs dfs -put -f "$file" /data/
done

python /app/build_input_data.py

hdfs dfs -ls /data | head -20
hdfs dfs -ls /input/data
hdfs dfs -cat /input/data/part-* | head -5