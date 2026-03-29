#!/usr/bin/env python3
import subprocess
from cassandra.cluster import Cluster

KEYSPACE = "search_engine"
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra-server")


def hdfs_cat(path: str):
    proc = subprocess.Popen(
        ["hdfs", "dfs", "-cat", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    for line in proc.stdout:
        yield line.rstrip("\n")

    stderr = proc.stderr.read()
    ret = proc.wait()
    if ret != 0:
        raise RuntimeError(f"hdfs dfs -cat failed for {path}: {stderr}")


def create_schema(session):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(KEYSPACE)

    session.execute("""
        CREATE TABLE IF NOT EXISTS docs (
            doc_id text PRIMARY KEY,
            title text,
            doc_len int
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            df int
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS index_data (
            term text,
            doc_id text,
            tf int,
            doc_len int,
            title text,
            PRIMARY KEY (term, doc_id)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            name text PRIMARY KEY,
            value double
        )
    """)

    session.execute("TRUNCATE docs")
    session.execute("TRUNCATE vocabulary")
    session.execute("TRUNCATE index_data")
    session.execute("TRUNCATE stats")


def load_docs(session):
    prepared = session.prepare(
        "INSERT INTO docs (doc_id, title, doc_len) VALUES (?, ?, ?)"
    )

    count = 0
    for line in hdfs_cat("/indexer/docs/part-*"):
        parts = line.split("\t")
        if len(parts) != 3:
            continue

        doc_id, title, doc_len = parts
        session.execute(prepared, (doc_id, title, int(doc_len)))
        count += 1

    print(f"Inserted docs total: {count}")


def load_vocabulary(session):
    prepared = session.prepare(
        "INSERT INTO vocabulary (term, df) VALUES (?, ?)"
    )

    count = 0
    for line in hdfs_cat("/indexer/vocabulary/part-*"):
        parts = line.split("\t")
        if len(parts) != 2:
            continue

        term, df = parts
        session.execute(prepared, (term, int(df)))
        count += 1

    print(f"Inserted vocabulary terms total: {count}")


def load_stats(session):
    prepared = session.prepare(
        "INSERT INTO stats (name, value) VALUES (?, ?)"
    )

    count = 0
    for line in hdfs_cat("/indexer/stats/part-*"):
        parts = line.split("\t")
        if len(parts) != 2:
            continue

        name, value = parts
        session.execute(prepared, (name, float(value)))
        count += 1

    print(f"Inserted stats total: {count}")


def load_index(session):
    prepared = session.prepare(
        "INSERT INTO index_data (term, doc_id, tf, doc_len, title) VALUES (?, ?, ?, ?, ?)"
    )

    terms_count = 0
    postings_count = 0

    for line in hdfs_cat("/indexer/index/part-*"):
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue

        term, df_str, postings_str = parts

        if postings_str.strip():
            postings = postings_str.split("|")
        else:
            postings = []

        for posting in postings:
            try:
                doc_id, tf, doc_len, title = posting.split(":", 3)
                session.execute(
                    prepared,
                    (term, doc_id, int(tf), int(doc_len), title)
                )
                postings_count += 1
            except Exception as e:
                print(f"Skipping malformed posting for term={term}: {posting} ({e})")

        terms_count += 1

    print(f"Inserted index terms total: {terms_count}")
    print(f"Inserted postings total: {postings_count}")


def main():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()

    create_schema(session)
    load_docs(session)
    load_vocabulary(session)
    load_stats(session)
    load_index(session)

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()