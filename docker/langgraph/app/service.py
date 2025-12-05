# service.py
from __future__ import annotations

import asyncio
import time
import logging
from typing import Any, Dict, List, Optional

import bentoml
from langchain_core.messages import HumanMessage, BaseMessage
from langfuse.langchain import CallbackHandler

from agents.conversation_agent.graph import build_graph  # <-- use your feedback graph

# Langfuse callback handler
lf_handler = CallbackHandler()

# Logging
logger = logging.getLogger("feedback_agent_service")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def _build_graph_sync():
    """
    Handle both sync and async build_graph implementations.
    """
    g = build_graph()
    if asyncio.iscoroutine(g):
        return asyncio.run(g)
    return g


def _extract_last_user_message_from_openai(messages: List[Dict[str, Any]]) -> str:
    """
    Extract last user message from an OpenAI-style 'messages' array.
    """
    for m in reversed(messages):
        if m.get("role") == "user":
            content = m.get("content")
            if isinstance(content, str):
                return content.strip()
            if isinstance(content, list):
                parts: List[str] = []
                for p in content:
                    if (
                        isinstance(p, dict)
                        and p.get("type") == "text"
                        and isinstance(p.get("text"), str)
                    ):
                        parts.append(p["text"])
                if parts:
                    return "\n".join(parts).strip()
    return ""


@bentoml.service(
    name="feedback_agent_service",
    traffic={"timeout": 600},
)
class FeedbackAgentService:
    """
    BentoML Service that wraps your query_agent feedback graph and exposes:

    - POST /invoke
    - POST /v1/chat/completions  (OpenAI compatible, non-streaming)
    """

    def __init__(self) -> None:
        logger.info("Initializing FeedbackAgentService, building graph...")
        self.graph = _build_graph_sync()
        logger.info("Graph built and ready.")

    # ------------------------------------------------------------------
    # Simple /invoke API (for quick tests)
    # ------------------------------------------------------------------
    @bentoml.api
    def invoke(self, message: str, session_id: Optional[str] = None) -> Dict[str, Any]:
        """
        POST /invoke?message=...&session_id=...

        or JSON:
        {
          "message": "hello",
          "session_id": "chat-123"
        }
        """
        user_message = (message or "").strip()
        thread_id = session_id or "default"

        logger.info("[/invoke] message=%r session_id=%r", user_message, thread_id)

        if not user_message:
            return {"ok": False, "error": "Missing 'message'."}

        state = {"messages": [HumanMessage(content=user_message)]}
        config = {
            "configurable": {"thread_id": thread_id},  # for MemorySaver
            "callbacks": [lf_handler],                 # Langfuse
            "tags": ["query_agent", "invoke"],
            "metadata": {"entrypoint": "invoke"},
        }

        try:
            result_state = self.graph.invoke(state, config=config)
        except Exception as e:
            logger.exception("Graph.invoke failed in /invoke")
            return {
                "ok": False,
                "thread_id": thread_id,
                "response": f"Graph error: {e}",
            }

        messages: List[BaseMessage] = result_state.get("messages", [])  # type: ignore
        text = ""
        if messages:
            last = messages[-1]
            text = last.content if isinstance(last.content, str) else str(last.content)

        logger.info("[/invoke] response=%r", text)

        return {
            "ok": True,
            "thread_id": thread_id,
            "response": text,
        }

    # ------------------------------------------------------------------
    # OpenAI-style /v1/chat/completions (non-streaming)
    # ------------------------------------------------------------------
    @bentoml.api(route="/v1/chat/completions")
    def chat_completions(
        self,
        model: str,
        messages: List[Dict[str, Any]],
        user: Optional[str] = None,
        session_id: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        top_p: Optional[float] = None,
        stream: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        OpenAI-compatible (non-streaming) endpoint.

        Expected JSON:

        {
          "model": "feedback-agent",
          "messages": [...],
          "user": "chat-123",
          "stream": false
        }
        """
        logger.info(
            "[/v1/chat/completions] model=%r user=%r session_id=%r",
            model,
            user,
            session_id,
        )

        model_name = model or "feedback-agent"
        thread_id = user or session_id or "default"

        user_message = _extract_last_user_message_from_openai(messages)
        if not user_message:
            logger.warning("[/v1/chat/completions] No user message found in messages.")
            now = int(time.time())
            text = "No user message found in 'messages'."
            return {
                "id": f"chatcmpl-{now}",
                "object": "chat.completion",
                "created": now,
                "model": model_name,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": text},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                },
            }

        state = {"messages": [HumanMessage(content=user_message)]}
        config = {
            "thread_id": thread_id,
            "callbacks": [lf_handler],
            "tags": ["query_agent", "chat_completions"],
            "metadata": {"entrypoint": "chat_completions"},
        }

        try:
            result_state = self.graph.invoke(state, config=config)
        except Exception as e:
            logger.exception("Graph.invoke failed in /v1/chat/completions")
            text = f"Graph error: {e}"
        else:
            result_messages: List[BaseMessage] = result_state.get("messages", [])  # type: ignore
            text = ""
            if result_messages:
                last = result_messages[-1]
                text = last.content if isinstance(last.content, str) else str(last.content)

        logger.info("[/v1/chat/completions] final text=%r", text)

        now = int(time.time())

        return {
            "id": f"chatcmpl-{now}",
            "object": "chat.completion",
            "created": now,
            "model": model_name,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": text},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        }
