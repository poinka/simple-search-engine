#!/bin/bash
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo 'Usage: bash /app/search.sh "your query here"'
  exit 1
fi

QUERY="$*"

source /app/.venv/bin/activate

export PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python
export PYSPARK_PYTHON=./.venv/bin/python
export CASSANDRA_HOST=cassandra-server

spark-submit \
  --master yarn \
  --deploy-mode client \
  --archives hdfs:///user/root/.venv-query.tar.gz#.venv \
  --conf spark.yarn.archive=hdfs:///apps/spark/spark-jars.zip \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
  --conf spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python \
  --conf spark.yarn.appMasterEnv.CASSANDRA_HOST=cassandra-server \
  --conf spark.executorEnv.CASSANDRA_HOST=cassandra-server \
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