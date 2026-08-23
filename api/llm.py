"""
Saans - LLM call layer.

Supports three providers behind one function, switched by the
LLM_PROVIDER env var:

  - "gemini" (default) -- Google AI Studio, genuinely free, no credit
    card required. Best default for a student project.
  - "groq"   -- also free, no credit card, extremely fast (Llama models).
    Good fallback if you hit Gemini's daily limit during a demo.
  - "anthropic" -- Claude, paid. Only use this if you have a funded key.

All three are called with plain `requests` calls rather than each
provider's SDK, so there's nothing extra to install and the code stays
easy to read/compare across providers -- useful for understanding what
"calling an LLM API" actually looks like under the hood.
"""

import os

import requests

LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "gemini").lower()

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")
GOOGLE_MODEL = os.environ.get("GOOGLE_MODEL", "gemini-2.5-flash")

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_MODEL = os.environ.get("GROQ_MODEL", "openai/gpt-oss-120b")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5")


def _call_gemini(system_prompt: str, user_content: str) -> str:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GOOGLE_MODEL}:generateContent"
    resp = requests.post(
        url,
        params={"key": GOOGLE_API_KEY},
        json={
            "system_instruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"parts": [{"text": user_content}]}],
            "generationConfig": {"maxOutputTokens": 300},
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["candidates"][0]["content"]["parts"][0]["text"].strip()


def _call_groq(system_prompt: str, user_content: str) -> str:
    resp = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
        json={
            "model": GROQ_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            "max_tokens": 300,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


def _call_anthropic(system_prompt: str, user_content: str) -> str:
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": ANTHROPIC_MODEL,
            "max_tokens": 300,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_content}],
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return "".join(block["text"] for block in data["content"] if block["type"] == "text").strip()


def is_configured() -> bool:
    return {
        "gemini": bool(GOOGLE_API_KEY),
        "groq": bool(GROQ_API_KEY),
        "anthropic": bool(ANTHROPIC_API_KEY),
    }.get(LLM_PROVIDER, False)


def generate_answer(system_prompt: str, user_content: str) -> str:
    """Dispatch to whichever provider is configured. Raises if the
    selected provider has no key set -- callers should check
    is_configured() first and fall back gracefully (see api/main.py)."""
    if LLM_PROVIDER == "groq":
        return _call_groq(system_prompt, user_content)
    if LLM_PROVIDER == "anthropic":
        return _call_anthropic(system_prompt, user_content)
    return _call_gemini(system_prompt, user_content)  # default
