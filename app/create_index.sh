#!/bin/bash
set -e

INPUT_PATH=${1:-/input/data}
TMP_PATH=/tmp/indexer_job
INDEXER_PATH=/indexer
OUTPUT_PATH=$TMP_PATH/output

STREAMING_JAR=$(find /usr/local/hadoop -name "hadoop-streaming*.jar" | head -n 1)

if [ -z "$STREAMING_JAR" ]; then
  echo "Could not find hadoop-streaming jar"
  exit 1
fi

echo "Using streaming jar: $STREAMING_JAR"

hdfs dfs -rm -r -f $TMP_PATH || true
hdfs dfs -rm -r -f $INDEXER_PATH || true

hdfs dfs -mkdir -p $TMP_PATH

mapred streaming \
  -D mapreduce.job.name="simple-search-indexer" \
  -D mapreduce.job.reduces=1 \
  -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input $INPUT_PATH \
  -output $OUTPUT_PATH

hdfs dfs -mkdir -p $INDEXER_PATH

# Split reducer output into index/docs/stats
hdfs dfs -cat $OUTPUT_PATH/part-* | awk -F '\t' '$1=="T"{print substr($0,3)}' | hdfs dfs -put - $INDEXER_PATH/index.tsv
hdfs dfs -cat $OUTPUT_PATH/part-* | awk -F '\t' '$1=="D"{print substr($0,3)}' | hdfs dfs -put - $INDEXER_PATH/docs.tsv
hdfs dfs -cat $OUTPUT_PATH/part-* | awk -F '\t' '$1=="S"{print substr($0,3)}' | hdfs dfs -put - $INDEXER_PATH/stats.tsv

echo "Created index files:"
hdfs dfs -ls $INDEXER_PATH

echo "Sample index:"
hdfs dfs -cat $INDEXER_PATH/index.tsv | head -5

echo "Document stats:"
hdfs dfs -cat $INDEXER_PATH/docs.tsv | head -5

echo "Corpus stats:"
hdfs dfs -cat $INDEXER_PATH/stats.tsv