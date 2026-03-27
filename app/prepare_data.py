import os
import re
import unicodedata
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, trim

spark = SparkSession.builder \
    .appName("data preparation") \
    .master("local[*]") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

os.makedirs("/app/data", exist_ok=True)

df = spark.read.parquet("file:///app/b.parquet")

df = (
    df.select("id", "title", "text")
      .withColumn("title", trim(col("title")))
      .withColumn("text", trim(col("text")))
      .filter(col("id").isNotNull())
      .filter(col("title").isNotNull())
      .filter(col("text").isNotNull())
      .filter(length(col("title")) > 0)
      .filter(length(col("text")) > 0)
      .limit(1000)
)

rows = df.collect()

def safe_ascii_title(title: str) -> str:
    title = str(title)
    title = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
    title = sanitize_filename(title)
    title = title.replace(" ", "_")
    title = re.sub(r"_+", "_", title)
    title = re.sub(r"[^A-Za-z0-9._()-]", "_", title)
    return title[:150] if title else "untitled"

for row in rows:
    filename = f"/app/data/{row['id']}_{safe_ascii_title(row['title'])}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(row["text"])

spark.stop()