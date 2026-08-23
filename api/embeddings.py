"""
Saans - embedding helper, shared by scripts/seed_embeddings.py and api/rag.py.

Uses `fastembed` (ONNX-runtime based) instead of sentence-transformers/torch
-- same embedding quality for this use case, but a fraction of the Docker
image size and startup time, which matters when you're rebuilding
containers repeatedly during development.

Model: BAAI/bge-small-en-v1.5, 384 dimensions. If you change this, update
the `vector(384)` column definition in sql/supabase_setup.sql to match.
"""

from fastembed import TextEmbedding

_model = None


def get_model() -> TextEmbedding:
    global _model
    if _model is None:
        _model = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
    return _model


def embed(texts: list[str]) -> list[list[float]]:
    """Embed a list of strings, returns one 384-dim vector per input."""
    model = get_model()
    return [vec.tolist() for vec in model.embed(texts)]


def embed_one(text: str) -> list[float]:
    return embed([text])[0]
