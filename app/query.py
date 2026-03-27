#!/usr/bin/env python3
import sys
import math
import re
from collections import defaultdict

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

KEYSPACE = "search_engine"
CASSANDRA_HOST = "cassandra-server"
TOKEN_RE = re.compile(r"[a-z0-9]+")
K1 = 1.2
B = 0.75


def tokenize(text: str):
    return TOKEN_RE.findall(text.lower())


def bm25(tf: int, df: int, dl: int, avgdl: float, N: int):
    idf = math.log(1.0 + (N - df + 0.5) / (df + 0.5))
    denom = tf + K1 * (1 - B + B * dl / avgdl)
    return idf * (tf * (K1 + 1)) / denom


def get_stat(session, name: str):
    row = session.execute(
        "SELECT value FROM stats WHERE name = %s",
        (name,)
    ).one()
    if row is None:
        raise RuntimeError(f"Missing stat: {name}")
    return float(row.value)


def main():
    query = " ".join(sys.argv[1:]).strip()
    if not query:
        print("Usage: python query.py <search query>")
        sys.exit(1)

    query_terms = tokenize(query)
    if not query_terms:
        print("No valid query terms.")
        sys.exit(0)

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)

    N = int(get_stat(session, "N"))
    avgdl = float(get_stat(session, "AVGDL"))

    all_postings = []
    for term in query_terms:
        rows = session.execute(
            "SELECT term, doc_id, tf, doc_len, title, df FROM postings WHERE term = %s",
            (term,)
        )
        for row in rows:
            all_postings.append(
                (row.term, row.doc_id, int(row.tf), int(row.doc_len), row.title, int(row.df))
            )

    if not all_postings:
        print("No matching documents found.")
        session.shutdown()
        cluster.shutdown()
        sys.exit(0)

    spark = (
        SparkSession.builder
        .appName("search-query")
        .master("local[*]")
        .getOrCreate()
    )
    sc = spark.sparkContext

    rdd = sc.parallelize(all_postings)

    scored = (
        rdd.map(lambda x: (
            x[1],  # doc_id
            (bm25(x[2], x[5], x[3], avgdl, N), x[4])  # score, title
        ))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1]))
        .map(lambda x: (x[0], x[1][1], x[1][0]))  # doc_id, title, score
        .takeOrdered(10, key=lambda x: -x[2])
    )

    print(f"Query: {query}")
    print("Top 10 results:")
    for rank, (doc_id, title, score) in enumerate(scored, start=1):
        print(f"{rank}\t{doc_id}\t{title}\t{score:.6f}")

    spark.stop()
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()