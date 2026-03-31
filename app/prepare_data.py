#!/usr/bin/env python3
import glob
import os
import re
import unicodedata

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, trim

N_DOCS = 1000
DOCS_DIR = "/app/data"
METADATA_HDFS_PATH = "hdfs:///tmp/search_engine_metadata/doc_titles"


def detect_local_parquet() -> str:
    env_path = os.environ.get("PARQUET_FILE")
    if env_path and os.path.exists(env_path):
        return env_path

    candidates = sorted(glob.glob("/app/*.parquet"))
    if not candidates:
        raise FileNotFoundError(
            "No parquet file found in /app. Put one parquet shard in app/ before running."
        )
    return candidates[0]


def build_hdfs_parquet_path(local_parquet_path: str) -> str:
    basename = os.path.basename(local_parquet_path)
    return f"hdfs:///parquet/{basename}"


def safe_filename_title(title: str) -> str:
    title = str(title)
    title = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
    title = sanitize_filename(title)
    title = title.replace(" ", "_")
    title = re.sub(r"_+", "_", title)
    title = re.sub(r"[^A-Za-z0-9._()-]", "_", title)
    return title[:150] if title else "untitled"


def clean_field(value: str) -> str:
    return str(value).replace("\t", " ").replace("\n", " ").strip()


def hdfs_delete_if_exists(spark: SparkSession, path: str) -> None:
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI.create(path), conf
    )
    hdfs_path = jvm.org.apache.hadoop.fs.Path(path)
    if fs.exists(hdfs_path):
        fs.delete(hdfs_path, True)


spark = (
    SparkSession.builder
    .appName("data preparation")
    .master("local[*]")
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .getOrCreate()
)

os.makedirs(DOCS_DIR, exist_ok=True)

local_parquet_path = detect_local_parquet()
hdfs_parquet_path = build_hdfs_parquet_path(local_parquet_path)

df = spark.read.parquet(hdfs_parquet_path)

selected_df = (
    df.select("id", "title", "text")
      .withColumn("title", trim(col("title")))
      .withColumn("text", trim(col("text")))
      .filter(col("id").isNotNull())
      .filter(col("title").isNotNull())
      .filter(col("text").isNotNull())
      .filter(length(col("title")) > 0)
      .filter(length(col("text")) > 0)
      .orderBy("id")
      .limit(N_DOCS)
)

# Save documents as individual text files in DOCS_DIR with filename format: {doc_id}_{sanitised_title}.txt
for row in selected_df.toLocalIterator():
    doc_id = str(row["id"])
    original_title = str(row["title"])
    safe_title = safe_filename_title(original_title)
    filename = f"{DOCS_DIR}/{doc_id}_{safe_title}.txt"

    with open(filename, "w", encoding="utf-8") as f:
        f.write(row["text"])

# Store metadata (doc_id and title) in HDFS for later use when loading into Cassandra
hdfs_delete_if_exists(spark, METADATA_HDFS_PATH)

(
    selected_df.rdd
    .map(lambda row: f"{row['id']}\t{clean_field(row['title'])}")
    .coalesce(1)
    .saveAsTextFile(METADATA_HDFS_PATH)
)

print(f"Read parquet from: {hdfs_parquet_path}")
print(f"Prepared {N_DOCS} documents in {DOCS_DIR}")
print(f"Stored title metadata in: {METADATA_HDFS_PATH}")

spark.stop()