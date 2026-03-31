#!/bin/bash
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo 'Usage: bash /app/search.sh "your query here"'
  exit 1
fi

QUERY="$*"
QUERY_VENV=/app/.venv-query
export CASSANDRA_HOST=${CASSANDRA_HOST:-cassandra-server}

if [ ! -x "${QUERY_VENV}/bin/python" ]; then
  echo "Query python not found at ${QUERY_VENV}/bin/python"
  exit 1
fi

spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.yarn.archive=hdfs:///spark/spark-jars.zip \
  --conf spark.pyspark.driver.python="${QUERY_VENV}/bin/python" \
  --conf spark.pyspark.python=/usr/bin/python3 \
  --conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3 \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3 \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=1 \
  --conf spark.yarn.am.cores=1 \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  --conf spark.executor.memoryOverhead=128 \
  --conf spark.yarn.am.memory=512m \
  --conf spark.yarn.am.memoryOverhead=128 \
  --conf spark.default.parallelism=1 \
  --conf spark.sql.shuffle.partitions=1 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.shuffle.service.enabled=false \
  --conf spark.ui.showConsoleProgress=false \
  /app/query.py "$QUERY"