#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession

DOCS_GLOB = "hdfs:///data/*.txt"
TITLES_GLOB = "hdfs:///tmp/search_engine_metadata/doc_titles/part-*"
OUTPUT_PATH = "hdfs:///input/data"


def clean_field(value: str) -> str:
    return str(value).replace("\t", " ").replace("\n", " ").strip()


spark = (
    SparkSession.builder
    .appName("build-input-data")
    .master("local[*]")
    .getOrCreate()
)

sc = spark.sparkContext


def parse_doc(item):
    path, text = item
    filename = os.path.basename(path)
    if filename.endswith(".txt"):
        filename = filename[:-4]

    if "_" in filename:
        doc_id, fallback_title = filename.split("_", 1)
    else:
        doc_id, fallback_title = filename, "untitled"

    return doc_id, (clean_field(fallback_title), clean_field(text))


def parse_title(line):
    parts = line.split("\t", 1)
    if len(parts) != 2:
        return None
    doc_id, original_title = parts
    return doc_id, clean_field(original_title)


docs_rdd = sc.wholeTextFiles(DOCS_GLOB).map(parse_doc)
titles_rdd = (
    sc.textFile(TITLES_GLOB)
      .map(parse_title)
      .filter(lambda x: x is not None)
)

joined_rdd = docs_rdd.leftOuterJoin(titles_rdd)

output_rdd = joined_rdd.map(
    lambda item: (
        f"{item[0]}\t"
        f"{item[1][1] if item[1][1] is not None else item[1][0][0]}\t"
        f"{item[1][0][1]}"
    )
)

output_rdd.coalesce(1).saveAsTextFile(OUTPUT_PATH)

print(f"Built input data in: {OUTPUT_PATH}")
spark.stop()