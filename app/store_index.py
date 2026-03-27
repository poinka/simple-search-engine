import subprocess
from cassandra.cluster import Cluster

KEYSPACE = "search_engine"
CASSANDRA_HOST = "cassandra-server"


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
        CREATE TABLE IF NOT EXISTS postings (
            term text,
            doc_id text,
            tf int,
            doc_len int,
            title text,
            df int,
            PRIMARY KEY (term, doc_id)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            name text PRIMARY KEY,
            value double
        )
    """)


def load_docs(session):
    prepared = session.prepare(
        "INSERT INTO docs (doc_id, title, doc_len) VALUES (?, ?, ?)"
    )

    count = 0
    for line in hdfs_cat("/indexer/docs.tsv"):
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        doc_id, title, doc_len = parts
        session.execute(prepared, (doc_id, title, int(doc_len)))
        count += 1
        if count % 200 == 0:
            print(f"Inserted docs: {count}")
    print(f"Inserted docs total: {count}")


def load_stats(session):
    prepared = session.prepare(
        "INSERT INTO stats (name, value) VALUES (?, ?)"
    )

    count = 0
    for line in hdfs_cat("/indexer/stats.tsv"):
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        name, value = parts
        session.execute(prepared, (name, float(value)))
        count += 1
    print(f"Inserted stats total: {count}")


def load_postings(session):
    prepared = session.prepare(
        "INSERT INTO postings (term, doc_id, tf, doc_len, title, df) VALUES (?, ?, ?, ?, ?, ?)"
    )

    terms_count = 0
    postings_count = 0

    for line in hdfs_cat("/indexer/index.tsv"):
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue

        term, df_str, postings_str = parts
        df = int(df_str)

        if postings_str.strip():
            postings = postings_str.split("|")
        else:
            postings = []

        for posting in postings:
            try:
                doc_id, tf, doc_len, title = posting.split(":", 3)
                session.execute(
                    prepared,
                    (term, doc_id, int(tf), int(doc_len), title, df)
                )
                postings_count += 1
            except Exception as e:
                print(f"Skipping malformed posting for term={term}: {posting} ({e})")

        terms_count += 1
        if terms_count % 1000 == 0:
            print(f"Inserted terms: {terms_count}, postings: {postings_count}")

    print(f"Inserted terms total: {terms_count}")
    print(f"Inserted postings total: {postings_count}")


def main():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()

    create_schema(session)
    load_docs(session)
    load_stats(session)
    load_postings(session)

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()