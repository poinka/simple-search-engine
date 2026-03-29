#!/usr/bin/env python3
import sys

current_term = None
postings = []

total_docs = 0
total_doc_len = 0


def flush_term():
    global current_term, postings
    if current_term is None:
        return

    df = len(postings)
    postings_str = "|".join(postings)

    print(f"V\t{current_term}\t{df}")
    print(f"T\t{current_term}\t{df}\t{postings_str}")

    current_term = None
    postings = []


for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t")
    key = parts[0]

    if key == "!DOC":
        if len(parts) != 4:
            continue

        _, doc_id, title, doc_len = parts
        try:
            doc_len = int(doc_len)
        except ValueError:
            continue

        total_docs += 1
        total_doc_len += doc_len

        print(f"D\t{doc_id}\t{title}\t{doc_len}")
        continue

    if len(parts) != 5:
        continue

    term, doc_id, title, tf, doc_len = parts

    try:
        tf = int(tf)
        doc_len = int(doc_len)
    except ValueError:
        continue

    if current_term is None:
        current_term = term

    if term != current_term:
        flush_term()
        current_term = term

    safe_title = title.replace("|", " ").replace(":", " ")
    postings.append(f"{doc_id}:{tf}:{doc_len}:{safe_title}")

flush_term()

if total_docs > 0:
    avgdl = total_doc_len / total_docs
    print(f"S\tN\t{total_docs}")
    print(f"S\tAVGDL\t{avgdl}")