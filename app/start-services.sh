#!/bin/bash
set -euo pipefail

# This should run only on the master node
printf "cluster-slave-1\n" > "$HADOOP_HOME/etc/hadoop/workers"
printf "cluster-slave-1\n" > "$HADOOP_HOME/etc/hadoop/slaves"

echo "Workers configured as:"
cat "$HADOOP_HOME/etc/hadoop/workers"
cat "$HADOOP_HOME/etc/hadoop/slaves"

# Start Hadoop services
"$HADOOP_HOME/sbin/start-dfs.sh" || true
"$HADOOP_HOME/sbin/start-yarn.sh" || true
mapred --daemon start historyserver || true

echo
echo "Java processes:"
jps -lm || true

echo
echo "HDFS report:"
hdfs dfsadmin -report || true

# Leave safe mode if needed
hdfs dfsadmin -safemode leave || true

# Prepare HDFS directories
hdfs dfs -mkdir -p /apps/spark/jars
hdfs dfs -mkdir -p /user/root

# Refresh Spark jars in HDFS so repeated runs do not fail
hdfs dfs -rm -r -f /apps/spark/jars/* || true
hdfs dfs -put -f /usr/local/spark/jars/* /apps/spark/jars/

# Permissions
hdfs dfs -chmod -R 755 /apps/spark/jars || true
hdfs dfs -chmod 755 /user/root || true

echo
echo "Scala version:"
scala -version || true

echo
echo "Java processes after setup:"
jps -lm || true

echo
echo "Spark jars in HDFS:"
hdfs dfs -ls /apps/spark/jars | head -20 || true