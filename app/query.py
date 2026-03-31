#!/usr/bin/env python3
import math
import os
import re
import sys
from collections import Counter
from typing import Dict, List, Tuple

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

KEYSPACE = "search_engine"
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra-server")
TOKEN_RE = re.compile(r"[a-z0-9]+")
K1 = 1.2
B = 0.75


def read_query() -> str:
    if len(sys.argv) > 1:
        return " ".join(sys.argv[1:]).strip()
    return sys.stdin.readline().strip()


def tokenize(text: str) -> List[str]:
    return TOKEN_RE.findall(text.lower())


def bm25(tf: int, df: int, dl: int, avgdl: float, n_docs: int) -> float:
    idf = math.log(1.0 + (n_docs - df + 0.5) / (df + 0.5))
    denom = tf + K1 * (1.0 - B + B * dl / avgdl)
    return idf * (tf * (K1 + 1.0)) / denom


def get_stat(session, name: str) -> float:
    row = session.execute("SELECT value FROM stats WHERE name = %s", (name,)).one()
    if row is None:
        raise RuntimeError(f"Missing stat in Cassandra: {name}")
    return float(row.value)


def load_term_dfs(session, query_terms: List[str]) -> List[Tuple[str, int]]:
    stmt = session.prepare("SELECT df FROM vocabulary WHERE term = ?")
    found = []
    for term in query_terms:
        row = session.execute(stmt, (term,)).one()
        if row is not None:
            found.append((term, int(row.df)))
    return found


def load_postings(session, term_dfs: List[Tuple[str, int]]):
    stmt = session.prepare(
        "SELECT doc_id, tf, doc_len, title FROM index_data WHERE term = ?"
    )
    postings = []
    for term, df in term_dfs:
        rows = session.execute(stmt, (term,))
        for row in rows:
            postings.append(
                (
                    term,
                    str(row.doc_id),
                    str(row.title),
                    int(row.tf),
                    int(row.doc_len),
                    int(df),
                )
            )
    return postings


def main():
    query = read_query()
    if not query:
        print("Empty query.")
        sys.exit(1)

    query_tokens = tokenize(query)
    if not query_tokens:
        print("No valid query terms.")
        sys.exit(0)

    query_tf: Dict[str, int] = Counter(query_tokens)
    unique_terms = list(query_tf.keys())

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)

    try:
        n_docs = int(get_stat(session, "N"))
        avgdl = float(get_stat(session, "AVGDL"))
        term_dfs = load_term_dfs(session, unique_terms)

        if not term_dfs:
            print(f"Query: {query}")
            print("Top 10 results:")
            print("No matching documents found.")
            return

        postings = load_postings(session, term_dfs)
    finally:
        session.shutdown()
        cluster.shutdown()

    spark = SparkSession.builder.appName("search-query").getOrCreate()
    sc = spark.sparkContext

    try:
        num_slices = max(1, min(8, len(postings))) if postings else 1
        postings_rdd = sc.parallelize(postings, numSlices=num_slices)

        scored_rdd = postings_rdd.map(
            lambda x: (
                x[1],  # doc_id
                (
                    bm25(
                        tf=x[3],
                        df=x[5],
                        dl=x[4],
                        avgdl=avgdl,
                        n_docs=n_docs,
                    ) * query_tf[x[0]],
                    x[2],  # title
                ),
            )
        )

        top_docs = (
            scored_rdd
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1]))
            .map(lambda x: (x[0], x[1][1], x[1][0]))
            .takeOrdered(10, key=lambda x: -x[2])
        )

        print(f"Query: {query}")
        print("Top 10 results:")
        if not top_docs:
            print("No matching documents found.")
            return

        for rank, (doc_id, title, score) in enumerate(top_docs, start=1):
            print(f"{rank}\t{doc_id}\t{title}\t{score:.6f}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()