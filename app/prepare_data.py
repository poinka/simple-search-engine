#!/usr/bin/env python3
import glob
import os
import re
import unicodedata

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, trim

N_DOCS = 1000


def detect_parquet() -> str:
    env_path = os.environ.get("PARQUET_FILE")
    if env_path and os.path.exists(env_path):
        return env_path

    candidates = sorted(glob.glob("/app/*.parquet"))
    if not candidates:
        raise FileNotFoundError("No parquet file found in /app. Put one parquet shard in app/ before running.")
    return candidates[0]


def safe_ascii_title(title: str) -> str:
    title = str(title)
    title = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
    title = sanitize_filename(title)
    title = title.replace(" ", "_")
    title = re.sub(r"_+", "_", title)
    title = re.sub(r"[^A-Za-z0-9._()-]", "_", title)
    return title[:150] if title else "untitled"


spark = (
    SparkSession.builder
    .appName("data preparation")
    .master("local[*]")
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .getOrCreate()
)

os.makedirs("/app/data", exist_ok=True)

parquet_path = detect_parquet()
df = spark.read.parquet(f"file://{parquet_path}")

df = (
    df.select("id", "title", "text")
      .withColumn("title", trim(col("title")))
      .withColumn("text", trim(col("text")))
      .filter(col("id").isNotNull())
      .filter(col("title").isNotNull())
      .filter(col("text").isNotNull())
      .filter(length(col("title")) > 0)
      .filter(length(col("text")) > 0)
      .limit(N_DOCS)
)

for row in df.toLocalIterator():
    filename = f"/app/data/{row['id']}_{safe_ascii_title(row['title'])}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(row["text"])

spark.stop()