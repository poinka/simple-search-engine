#!/bin/bash
set -euo pipefail

INPUT_PATH=${1:-/input/data}
TMP_PATH=/tmp/indexer_job
OUTPUT_PATH=${TMP_PATH}/output
INDEXER_PATH=/indexer

echo "Creating index from input path: ${INPUT_PATH}"

# Clean previous temporary and final outputs
hdfs dfs -rm -r -f "${TMP_PATH}" || true
hdfs dfs -rm -r -f "${INDEXER_PATH}" || true

# Run Hadoop Streaming job
mapred streaming \
  -D mapreduce.job.name="simple-search-indexer" \
  -D mapreduce.job.reduces=1 \
  -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "${INPUT_PATH}" \
  -output "${OUTPUT_PATH}"

# Create final index folders
hdfs dfs -mkdir -p "${INDEXER_PATH}/vocabulary"
hdfs dfs -mkdir -p "${INDEXER_PATH}/index"
hdfs dfs -mkdir -p "${INDEXER_PATH}/docs"
hdfs dfs -mkdir -p "${INDEXER_PATH}/stats"

# Split reducer output into separate logical datasets
hdfs dfs -cat "${OUTPUT_PATH}"/part-* | awk -F '\t' '$1=="V"{print $2 "\t" $3}' | hdfs dfs -put - "${INDEXER_PATH}/vocabulary/part-00000"
hdfs dfs -cat "${OUTPUT_PATH}"/part-* | awk -F '\t' '$1=="T"{print $2 "\t" $3 "\t" $4}' | hdfs dfs -put - "${INDEXER_PATH}/index/part-00000"
hdfs dfs -cat "${OUTPUT_PATH}"/part-* | awk -F '\t' '$1=="D"{print $2 "\t" $3 "\t" $4}' | hdfs dfs -put - "${INDEXER_PATH}/docs/part-00000"
hdfs dfs -cat "${OUTPUT_PATH}"/part-* | awk -F '\t' '$1=="S"{print $2 "\t" $3}' | hdfs dfs -put - "${INDEXER_PATH}/stats/part-00000"

echo
echo "Created index folders:"
hdfs dfs -ls "${INDEXER_PATH}"

echo
echo "Vocabulary sample:"
hdfs dfs -cat "${INDEXER_PATH}/vocabulary/part-00000" | head -5 || true

echo
echo "Index sample:"
hdfs dfs -cat "${INDEXER_PATH}/index/part-00000" | head -5 || true

echo
echo "Docs sample:"
hdfs dfs -cat "${INDEXER_PATH}/docs/part-00000" | head -5 || true

echo
echo "Stats:"
hdfs dfs -cat "${INDEXER_PATH}/stats/part-00000" || true