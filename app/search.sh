#!/bin/bash
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo 'Usage: bash /app/search.sh "your query here"'
  exit 1
fi

QUERY="$*"
QUERY_VENV=/app/.venv-query
export CASSANDRA_HOST=${CASSANDRA_HOST:-cassandra-server}

if [ ! -f "${QUERY_VENV}/bin/activate" ]; then
  echo "Query virtual environment not found at ${QUERY_VENV}"
  exit 1
fi

source "${QUERY_VENV}/bin/activate"

export PYSPARK_DRIVER_PYTHON="${QUERY_VENV}/bin/python"
unset PYSPARK_PYTHON

spark-submit \
  --master yarn \
  --deploy-mode client \
  --archives hdfs:///user/root/.venv-query.tar.gz#.venv \
  --conf spark.yarn.jars=hdfs:///apps/spark/jars/* \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
  --conf spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python \
  --conf spark.yarn.appMasterEnv.CASSANDRA_HOST="${CASSANDRA_HOST}" \
  --conf spark.executorEnv.CASSANDRA_HOST="${CASSANDRA_HOST}" \
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
  --conf spark.ui.showConsoleProgress=false \
  /app/query.py "$QUERY"