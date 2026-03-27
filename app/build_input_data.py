import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("build-input-data")
    .master("local[*]")
    .getOrCreate()
)

sc = spark.sparkContext
rdd = sc.wholeTextFiles("hdfs:///data/*.txt")

def convert(item):
    path, text = item
    filename = os.path.basename(path)
    if filename.endswith(".txt"):
        filename = filename[:-4]

    if "_" in filename:
        doc_id, doc_title = filename.split("_", 1)
    else:
        doc_id, doc_title = filename, "untitled"

    doc_title = doc_title.replace("\t", " ").replace("\n", " ").strip()
    text = text.replace("\t", " ").replace("\n", " ").strip()

    return f"{doc_id}\t{doc_title}\t{text}"

rdd.map(convert).coalesce(1).saveAsTextFile("hdfs:///input/data")
spark.stop()