import asyncio
import hashlib
import uuid
import json
from typing import Annotated, TypedDict, List, Optional, Literal

import boto3
from pydantic import BaseModel, Field, ConfigDict
from langchain_aws import ChatBedrockConverse
from langchain_core.messages import HumanMessage, BaseMessage, AIMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableConfig

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages

from prompts.prompts import supervisor_agent_system_prompt
from agents.query_agent.graph import get_query_graph_tool 
from langfuse.langchain import CallbackHandler
lf_handler = CallbackHandler()


# === LLM for supervisor judgment ===
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
supervisor_llm = ChatBedrockConverse(
    model="anthropic.claude-3-haiku-20240307-v1:0",
    provider="anthropic",
    temperature=0,
    client=bedrock,
)

# === Inference schema for LLM judgment ===
class InferredRoute(BaseModel):
    model_config = ConfigDict(extra="ignore")
    next: Literal["FINISH", "query_agent"]
    inferred_tools: List[str] = Field(default_factory=list)
    violations: List[str] = Field(default_factory=list)
    feedback: str
    has_data: bool
    has_insight: bool
    signature: str  # raw signature before normalization
    reasoning: Optional[str] = None

    @classmethod
    def normalize_signature(cls, sig: str) -> str:
        return hashlib.sha256(sig.strip().encode("utf-8")).hexdigest()

# === Supervisor prompt (includes system instructions) ===
system_text = f"{supervisor_agent_system_prompt.prompt}"

prompt = ChatPromptTemplate.from_messages([("system", system_text), ("placeholder", "{messages}")])
supervisor_chain = prompt | supervisor_llm.with_structured_output(InferredRoute)

# === State definition ===
class State(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]
    agent_calls: int
    last_signature: Optional[str]
    next: Optional[str]

# === Helpers ===
def get_text(msg: BaseMessage) -> str:
    c = getattr(msg, "content", "")
    if isinstance(c, list):
        return " ".join(str(x) for x in c)
    return str(c or "")

def sanitize_history(history: List[BaseMessage]) -> List[BaseMessage]:
    # If the last message is an AIMessage from previous LLM (which could prefill), drop it for clean inference
    if history and isinstance(history[-1], AIMessage):
        return history[:-1]
    return history

def build_raw_signature(route: InferredRoute) -> str:
    tools = ",".join(route.inferred_tools) or "none"
    data_flag = "yes" if route.has_data else "no"
    insight_flag = "yes" if route.has_insight else "no"
    decision = route.next
    return f"inferred={tools};data={data_flag};insight={insight_flag};decision={decision}"

# === Supervisor node ===
async def supervisor_node(state: State, config: RunnableConfig):
    calls = state.get("agent_calls", 0)
    history = list(state["messages"])
    sanitized = sanitize_history(history)

    # Run structured judgment
    try:
        route: InferredRoute = await supervisor_chain.ainvoke({"messages": sanitized}, config=config)
    except Exception as e:
        # Fallback: if parsing failed, decide based on deterministic presence
        latest_agent = next((m for m in reversed(history) if getattr(m, "name", "") == "query_agent"), None)
        content = get_text(latest_agent) if latest_agent else ""
        has_data = "data:" in content.lower()
        has_insight = "insight:" in content.lower()
        route = InferredRoute(
            next="FINISH" if (has_data and has_insight) or calls >= 2 else "query_agent",
            inferred_tools=[],
            violations=[],
            feedback=f"Structured parse failed: {e}. Falling back to basic heuristics.",
            has_data=has_data,
            has_insight=has_insight,
            signature="fallback",
            reasoning="Parsing error fallback.",
        )

    raw_sig = build_raw_signature(route)
    normalized = InferredRoute.normalize_signature(route.signature or raw_sig)

    # Assemble supervisor summary message (include raw signature for traceability)
    summary_lines = [
        f"Feedback: {route.feedback.strip()}",
        f"Inferred tools: {', '.join(route.inferred_tools) or 'none'}",
        f"Violations: {', '.join(route.violations) if route.violations else 'none'}",
        f"Has data: {route.has_data}",
        f"Has insight: {route.has_insight}",
        f"Decision: {route.next}",
        f"Raw signature: {raw_sig}",
    ]
    if route.reasoning:
        summary_lines.append(f"Reasoning: {route.reasoning.strip()}")
    supervisor_msg = HumanMessage(content="\n".join(summary_lines), name="supervisor")

    merged = history + [supervisor_msg]

    # Stagnation or max attempts -> finish
    if state.get("last_signature") == normalized or calls >= 2 or (route.has_data and route.has_insight and not any(v.startswith("missing") for v in route.violations)):
        final_decision = "FINISH"
        finishing_msg = HumanMessage(content="Supervisor decided: FINISH (terminal)", name="supervisor")
        return {
            "next": "FINISH",
            "agent_calls": calls,
            "messages": merged + [finishing_msg],
            "last_signature": normalized,
        }

    # Otherwise, route to query_agent
    return {
        "next": route.next,
        "agent_calls": calls,
        "messages": merged,
        "last_signature": normalized,
    }

# === call_query_agent node ===
async def call_query_agent_node(state: State, config: RunnableConfig):
    calls = state.get("agent_calls", 0)
    history = list(state["messages"])

    # Extract user request (first human without a name)
    user_req = ""
    for m in history:
        if getattr(m, "name", None) is None:
            user_req = get_text(m)
            break

    # Build inner config preserving tracing
    inner_cfg = {}
    if isinstance(config, dict):
        inner_cfg = {**config}
    else:
        inner_cfg = {
            "thread_id": getattr(config, "thread_id", None),
            "callbacks": getattr(config, "callbacks", None),
            "tags": getattr(config, "tags", None),
            "metadata": getattr(config, "metadata", None),
        }
    # Ensure supervisor's lf_handler remains
    existing_cbs = inner_cfg.get("callbacks", [])
    if isinstance(existing_cbs, list):
        if lf_handler not in existing_cbs:
            existing_cbs.append(lf_handler)
        inner_cfg["callbacks"] = existing_cbs

    try:
        tool = await get_query_graph_tool(config=None)
        tool_out = await tool.ainvoke(
            {"user_request": user_req, "retries": 0},
            config=inner_cfg,
        )
    except Exception as e:
        err = HumanMessage(content=f"Query agent graph tool failed: {e}", name="supervisor")
        return {
            "next": "FINISH",
            "agent_calls": calls + 1,
            "messages": history + [err],
            "last_signature": state.get("last_signature"),
        }

    # Build query_agent-like message
    data = getattr(tool_out, "data", "") if hasattr(tool_out, "data") else tool_out.get("data", "")
    insight = getattr(tool_out, "insight", "") if hasattr(tool_out, "insight") else tool_out.get("insight", "")
    tools_used = getattr(tool_out, "tools_used", []) if hasattr(tool_out, "tools_used") else tool_out.get("tools_used", [])
    raw_query_result = getattr(tool_out, "raw_query_result", {}) if hasattr(tool_out, "raw_query_result") else tool_out.get("raw_query_result", {})

    agent_parts = []
    if raw_query_result:
        agent_parts.append(f"Raw Query Result:\n{json.dumps(raw_query_result, indent=2)}")
    if data:
        agent_parts.append(f"Data:\n{data}")
    if insight:
        agent_parts.append(f"Insight:\n{insight}")
    if tools_used:
        agent_parts.append(f"Tools used: {', '.join(tools_used)}")
    agent_msg = HumanMessage(content="\n\n".join(agent_parts), name="query_agent")

    return {
        "next": "supervisor",
        "agent_calls": calls + 1,
        "messages": history + [agent_msg],
        "last_signature": state.get("last_signature"),
    }

# === Graph assembly ===
memory = MemorySaver()
graph_builder = StateGraph(State)
graph_builder.add_node("supervisor", supervisor_node)
graph_builder.add_node("call_query_agent", call_query_agent_node)

# Entry: start → supervisor
graph_builder.add_edge(START, "supervisor")

# Routing: supervisor decides
def routing_fn(s: State):
    return s.get("next", None)

graph_builder.add_conditional_edges(
    "supervisor",
    routing_fn,
    {
        "query_agent": "call_query_agent",
        "FINISH": END,
    },
)

# After call_query_agent go back to supervisor
graph_builder.add_edge("call_query_agent", "supervisor")

graph = graph_builder.compile(checkpointer=memory)
graph.name = "SupervisorAgentGraph"

# === Example run ===
async def main():
    result = await graph.ainvoke(
        {
            "messages": [HumanMessage(content="fetch bitcoin data , aggregate by week . Summarize results")],
            "agent_calls": 0,
            "last_signature": None,
            "next": None,
        },
        config={
            "thread_id": f"supervisor-{uuid.uuid4()}",
            "callbacks": [lf_handler],
            "tags": ["supervisor_flow"],
            "metadata": {"entrypoint": "supervisor"},
        },
    )
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
