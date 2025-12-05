from typing import TypedDict, Annotated, Optional, List
import os

from langchain_core.messages import (
    BaseMessage,
    HumanMessage,
    AIMessage,
    SystemMessage,
)
from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, START, END, add_messages
from langgraph.checkpoint.memory import MemorySaver

from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI

from servers.src.variables import OLLAMA_MODEL


# ---- State definition ----

class AgentState(TypedDict):
    # Conversation history (LangGraph will append messages automatically)
    messages: Annotated[List[BaseMessage], add_messages]
    # Persistent user feedback, e.g. "be concise", "answer in spanish"
    feedback: Optional[str]


# ---- LLM factory ----

def build_llm():
    """
    Decide which backend to use and return a chat model instance.

    Backends:

    - LLM_BACKEND=litellm:
        Uses ChatOpenAI configured to talk to a LiteLLM proxy.

        ENV:
          - LITELLM_MODEL_NAME: model name LiteLLM exposes
          - LITELLM_BASE_URL:   LiteLLM HTTP endpoint (e.g. http://litellm:4000)
          - LITELLM_API_KEY:    any API key LiteLLM expects (or "sk-noop")

    - (default):
        Uses local Ollama via ChatOllama with OLLAMA_MODEL.
    """
    backend = os.getenv("LLM_BACKEND", "litellm").lower()

    if backend == "litellm":
        litellm_model = os.getenv("LITELLM_MODEL_NAME", "bedrock-claude-haiku")
        base_url = os.getenv("LITELLM_BASE_URL", "http://litellm:4000")
        api_key = os.getenv("LITELLM_API_KEY", "sk-noop")

        return ChatOpenAI(
            model=litellm_model,
            base_url=base_url,
            api_key=api_key,
            temperature=0.0,
            timeout=60,
        )

    # Default: Ollama
    return ChatOllama(model=OLLAMA_MODEL)


# ---- Routing function (NOT a node) ----

def route(state: AgentState) -> str:
    """
    Decide where to go next based on the latest user message.

    If the last Human message starts with 'feedback:', route to 'store_feedback',
    otherwise route to 'assistant'.
    """
    messages = state.get("messages") or []
    if not messages:
        return "assistant"

    last = messages[-1]

    if isinstance(last, HumanMessage) and isinstance(last.content, str):
        text = last.content.strip()
        if text.lower().startswith("feedback:"):
            return "store_feedback"

    return "assistant"


# ---- Nodes ----

def store_feedback_node(state: AgentState) -> AgentState:
    """
    Extract feedback from the last message and acknowledge it.
    Example user message: 'feedback: answer in spanish and be concise'
    """
    last = state["messages"][-1]
    text = str(last.content).strip()
    feedback_text = text[len("feedback:"):].strip()

    ack = AIMessage(
        content=(
            f"Got your feedback: '{feedback_text}'. "
            "I’ll adapt my future responses accordingly."
        )
    )

    # Update state.feedback and append an acknowledgement
    return {
        "feedback": feedback_text,
        "messages": [ack],
    }


def build_graph():
    """
    Build a simple graph with:
      - route() called from START to pick 'assistant' vs 'store_feedback'
      - store_feedback_node -> updates feedback + acknowledges
      - assistant_node -> answers using feedback
    """

    llm = build_llm()  # choose LiteLLM vs Ollama here

    # NOTE: accept `config` so LangGraph can pass callbacks, configurable, etc.
    def assistant_node(state: AgentState, config: RunnableConfig) -> AgentState:
        """
        Main assistant behavior.
        Uses any stored feedback as part of the system prompt.
        """
        system_content = "You are a helpful assistant."

        if state.get("feedback"):
            system_content += (
                " The user has given you this persistent feedback:"
                f" '{state['feedback']}'. Please adapt your responses to follow it."
            )

        system_msg = SystemMessage(content=system_content)

        # Use full history + system message
        messages = [system_msg] + state["messages"]

        # VERY IMPORTANT: pass `config` so callbacks (Langfuse) are applied.
        response = llm.invoke(messages, config=config)

        return {"messages": [response]}

    graph = StateGraph(AgentState)

    # Nodes
    graph.add_node("assistant", assistant_node)
    graph.add_node("store_feedback", store_feedback_node)

    # Routing from START based on route()
    graph.add_conditional_edges(
        START,
        route,
        {
            "assistant": "assistant",
            "store_feedback": "store_feedback",
        },
    )

    # End of flow for each path
    graph.add_edge("assistant", END)
    graph.add_edge("store_feedback", END)

    # In-memory checkpointing, keyed by configurable.thread_id
    checkpointer = MemorySaver()

    return graph.compile(checkpointer=checkpointer)
