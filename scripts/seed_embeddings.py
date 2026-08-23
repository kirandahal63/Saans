"""
Saans - one-time setup script: chunks api/knowledge_base.md, embeds every
chunk, and loads it into Supabase's guidance_embeddings table.

Run this once after applying sql/supabase_setup.sql:

    export SUPABASE_DB_URL="postgresql://postgres:[password]@[host]:5432/postgres"
    pip install -r api/requirements.txt
    python scripts/seed_embeddings.py

Re-run any time you edit knowledge_base.md -- it clears and re-embeds
the whole table, so it's always safe to re-run.
"""

import os
import re
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).parent.parent / "api"))
from embeddings import embed  # noqa: E402

DATABASE_URL = os.environ["SUPABASE_DB_URL"]
KB_PATH = Path(__file__).parent.parent / "api" / "knowledge_base.md"


def load_chunks() -> list[dict]:
    raw = KB_PATH.read_text(encoding="utf-8")
    sections = re.split(r"\n(?=# )", raw.strip())
    chunks = []
    for section in sections:
        lines = section.strip().splitlines()
        if not lines:
            continue
        heading = lines[0].lstrip("# ").strip()
        body = " ".join(lines[1:]).strip()
        chunks.append({"heading": heading, "content": body})
    return chunks


def main() -> None:
    chunks = load_chunks()
    print(f"Loaded {len(chunks)} chunks from {KB_PATH.name}")

    print("Computing embeddings (first run downloads the model, ~130MB)...")
    vectors = embed([f"{c['heading']}. {c['content']}" for c in chunks])

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DELETE FROM guidance_embeddings")
        for chunk, vec in zip(chunks, vectors):
            vec_literal = "[" + ",".join(str(x) for x in vec) + "]"
            cur.execute(
                "INSERT INTO guidance_embeddings (heading, content, embedding) VALUES (%s, %s, %s::vector)",
                (chunk["heading"], chunk["content"], vec_literal),
            )
    conn.close()
    print(f"Loaded {len(chunks)} embeddings into Supabase. Done.")


if __name__ == "__main__":
    main()
