import re
import sys
from collections import Counter

TOKEN_RE = re.compile(r"[a-z0-9]+")

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id, title, text = parts
    title = title.strip()
    text = text.strip().lower()

    tokens = TOKEN_RE.findall(text)
    doc_len = len(tokens)

    if doc_len == 0:
        continue

    tf_counter = Counter(tokens)

    print(f"!DOC\t{doc_id}\t{title}\t{doc_len}")

    for term, tf in tf_counter.items():
        print(f"{term}\t{doc_id}\t{title}\t{tf}\t{doc_len}")