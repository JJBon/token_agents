# prompts/prompts.py

import os
from typing import Optional, Any

from ragas.prompt import PydanticPrompt  # kept in case you use it elsewhere
from pydantic import BaseModel, Field

try:
    from langfuse import Langfuse
except ImportError:
    Langfuse = None


class SimplePrompt:
    """
    Minimal stand-in for a Langfuse prompt object.
    Only provides a `.prompt` attribute, which is what the rest of
    your code actually needs (e.g. query_agent_system_prompt.prompt).
    """
    def __init__(self, prompt: str):
        self.prompt = prompt


def _fallback_query_agent_prompt() -> SimplePrompt:
    """
    Local fallback for the query agent system prompt.
    """
    return SimplePrompt(
        (
            "You are a helpful query agent. "
            "You answer user questions clearly and concisely. "
            "If the user provides feedback in the form "
            "'feedback: ...', you should adapt future answers accordingly."
        )
    )


def _fallback_supervisor_prompt() -> SimplePrompt:
    """
    Local fallback for the BI supervisor system prompt.
    """
    return SimplePrompt(
        (
            "You are a BI supervisor agent. "
            "You coordinate and oversee other agents, ensuring that "
            "their answers are consistent, factually correct, and aligned "
            "with the user's goals."
        )
    )


def _get_langfuse_client() -> Optional["Langfuse"]:
    """
    Safely create a Langfuse client only if everything is configured.
    If anything is missing or fails, return None so we can fall back.
    """
    if Langfuse is None:
        return None

    public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
    secret_key = os.getenv("LANGFUSE_SECRET_KEY")

    if not public_key or not secret_key:
        # This is exactly the situation during Docker build
        return None

    try:
        return Langfuse(public_key=public_key, secret_key=secret_key)
    except Exception as e:
        print(f"[prompts] Langfuse init failed, using fallback prompts. Error: {e}")
        return None


def _safe_get_prompt(
    lf: Optional["Langfuse"],
    name: str,
    label: str,
    fallback: Any,
) -> Any:
    """
    Try to fetch a prompt from Langfuse; if anything goes wrong, return fallback.
    """
    if lf is None:
        return fallback

    try:
        return lf.get_prompt(name, label=label)
    except Exception as e:
        print(
            f"[prompts] Langfuse get_prompt('{name}', label='{label}') failed, "
            f"using fallback. Error: {e}"
        )
        return fallback


# Initialize client once at import time, but only if safely configured
_lf = _get_langfuse_client()

query_agent_system_prompt = _safe_get_prompt(
    _lf,
    "semantic/system-prompts/query-agent",
    label="latest",
    fallback=_fallback_query_agent_prompt(),
)

supervisor_agent_system_prompt = _safe_get_prompt(
    _lf,
    "semantic/system-prompts/bi-supervisor",
    label="latest",
    fallback=_fallback_supervisor_prompt(),
)
