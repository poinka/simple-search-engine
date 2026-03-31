#!/bin/bash
set -euo pipefail

printf "cluster-slave-1\n" > "$HADOOP_HOME/etc/hadoop/workers"
printf "cluster-slave-1\n" > "$HADOOP_HOME/etc/hadoop/slaves"

echo "Workers configured as:"
cat "$HADOOP_HOME/etc/hadoop/workers"
cat "$HADOOP_HOME/etc/hadoop/slaves"

"$HADOOP_HOME/sbin/start-dfs.sh"
"$HADOOP_HOME/sbin/start-yarn.sh"
mapred --daemon start historyserver

echo
echo "Master Java processes:"
jps -lm

echo
echo "Worker Java processes:"
ssh cluster-slave-1 "jps -lm" || true

echo
echo "Waiting for YARN worker registration..."
REGISTERED=0
for i in $(seq 1 24); do
  if yarn node -list 2>/dev/null | grep -q "RUNNING"; then
    REGISTERED=1
    echo "YARN worker node is registered."
    break
  fi
  echo "Retry ${i}/24: YARN worker not registered yet..."
  sleep 5
done

if [ "$REGISTERED" -ne 1 ]; then
  echo "No RUNNING YARN NodeManager found."
  echo
  echo "Current YARN nodes:"
  yarn node -list || true
  exit 1
fi

echo
echo "HDFS report:"
hdfs dfsadmin -report || true

hdfs dfsadmin -safemode leave || true

hdfs dfs -mkdir -p /spark
hdfs dfs -mkdir -p /user/root

rm -f /tmp/spark-jars.zip
cd /usr/local/spark/jars
zip -qr /tmp/spark-jars.zip .

hdfs dfs -rm -f /spark/spark-jars.zip || true
hdfs dfs -put -f /tmp/spark-jars.zip /spark/spark-jars.zip

hdfs dfs -chmod 644 /spark/spark-jars.zip || true
hdfs dfs -chmod 755 /user/root || true

echo
echo "Scala version:"
scala -version || true

echo
echo "YARN nodes:"
yarn node -list || true

echo
echo "Spark archive in HDFS:"
hdfs dfs -ls /spark || true