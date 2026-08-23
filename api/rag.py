"""
Saans - retrieval layer, now backed by a real vector database
(Postgres + pgvector, hosted on Supabase) instead of the prototype's
in-process TF-IDF.

Run scripts/seed_embeddings.py once (after applying sql/supabase_setup.sql)
to populate the guidance_embeddings table before this will return results.
"""

import os

import psycopg2

from embeddings import embed_one

DATABASE_URL = os.environ["SUPABASE_DB_URL"]  # postgres connection string, see .env.example


def search(query: str, top_k: int = 3) -> list[dict]:
    """Embed the query and return the top_k nearest guidance chunks by
    cosine distance, using pgvector's <=> operator."""
    query_vec = embed_one(query)
    vec_literal = "[" + ",".join(str(x) for x in query_vec) + "]"

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT heading, content, embedding <=> %s::vector AS distance
                FROM guidance_embeddings
                ORDER BY distance ASC
                LIMIT %s
                """,
                (vec_literal, top_k),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    return [{"heading": r[0], "text": r[1], "distance": float(r[2])} for r in rows]
