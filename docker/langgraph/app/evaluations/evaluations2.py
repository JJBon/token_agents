import asyncio
import json
import logging
import uuid
from typing import List, Dict, Optional

from ragas.metrics import ToolCallAccuracy
from ragas.dataset_schema import MultiTurnSample
from ragas.messages import ToolCall, HumanMessage as RagasHuman, AIMessage as RagasAI, ToolMessage as RagasTool

# Import graph-related dependencies for integration mode
try:
    from coin_gecko import build_graph, lf_handler
    from tools.dbt_tools import fetch_metrics_tool, create_query_tool, fetch_query_result_tool
    from agents.dbt_agents import query_llm
    from langchain_core.messages import HumanMessage, SystemMessage
    GRAPH_AVAILABLE = True
except ImportError:
    GRAPH_AVAILABLE = False

logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------
# Helper Functions
# ---------------------------------------------------

def normalize_content(content):
    """Convert list-based content to a string."""
    if isinstance(content, list):
        texts = [c.get("text", "") for c in content if isinstance(c, dict) and "text" in c]
        return " ".join(texts).strip()
    return str(content)


def normalize_tool_calls(tool_calls: Optional[List[Dict]]):
    """Ensure tool calls have required fields like 'id'."""
    normalized = []
    for tc in (tool_calls or []):
        tc_copy = dict(tc)
        tc_copy.setdefault("id", f"tool_{uuid.uuid4()}")
        normalized.append(tc_copy)
    return normalized


def convert_message(raw_msg: Dict):
    """Convert raw message into a Ragas-compatible object."""
    msg_type = raw_msg.get("type", "human")

    if msg_type.lower() in ["human", "user"]:
        return RagasHuman(content=normalize_content(raw_msg.get("content", "")))

    if msg_type.lower() in ["ai", "assistant"]:
        tool_calls = [
            ToolCall(name=tc["name"], args=tc.get("args", {}))
            for tc in normalize_tool_calls(raw_msg.get("tool_calls", []))
        ]
        return RagasAI(content=normalize_content(raw_msg.get("content", "")), tool_calls=tool_calls)

    if msg_type.lower() in ["tool"]:
        return RagasTool(content=normalize_content(raw_msg.get("content", "")))

    return RagasHuman(content=normalize_content(raw_msg.get("content", "")))


def debug_ragas_messages(ragas_messages: List):
    """Debug print of ragas messages."""
    print("==== DEBUG RAGAS MESSAGES ====")
    for m in ragas_messages:
        if isinstance(m, RagasAI):
            print(f"AIMessage: {m.content} | tool_calls: {[tc.name for tc in (m.tool_calls or [])]}")
        else:
            print(f"{m.__class__.__name__}: {m.content}")
    print("==============================\n")


async def evaluate_messages(messages: List):
    """Evaluate tool call accuracy for a list of Ragas messages."""
    debug_ragas_messages(messages)

    reference_tool_calls = [
        ToolCall(name="fetch_metrics", args={}),
        ToolCall(name="create_query", args={"metrics": ["market_cap_growth_rate"]}),
    ]
    print("==== EXPECTED TOOL CALLS ====")
    for ref in reference_tool_calls:
        print(f"- {ref.name} with args {ref.args}")

    sample = MultiTurnSample(
        user_input=messages,
        reference_tool_calls=reference_tool_calls,
    )

    scorer = ToolCallAccuracy()
    raw_score = await scorer.multi_turn_ascore(sample)
    score = min(1.0, raw_score)
    print("\nTool Call Accuracy:", round(score, 3))
    return score


# ---------------------------------------------------
# Mocked Unit Test
# ---------------------------------------------------
async def unit_test(validate_only=False):
    raw_messages = [
        {"type": "human", "content": "fetch Bitcoin market cap growth rate"},
        {
            "type": "ai",
            "content": [
                {"type": "text", "text": "Okay, let's try to fetch the Bitcoin market cap growth rate using the dbt semantic layer."}
            ],
            "tool_calls": [{"name": "fetch_metrics", "args": {}}],
        },
        {"type": "tool", "content": json.dumps({"metrics": ["market_cap_growth_rate"]})},
        {
            "type": "ai",
            "content": "Based on the available metrics, it looks like the 'market_cap_growth_rate' metric would be the most relevant to fetch the Bitcoin market cap growth rate.",
            "tool_calls": [{"name": "create_query", "args": {"metrics": ["market_cap_growth_rate"]}}],
        },
        {"type": "tool", "content": json.dumps({"status": "CREATED", "query": {"metrics": ["market_cap_growth_rate"]}})},
    ]

    messages = [convert_message(msg) for msg in raw_messages]

    if validate_only:
        debug_ragas_messages(messages)
        print("Validation only mode. No scoring performed.")
        return

    await evaluate_messages(messages)


# ---------------------------------------------------
# Integration Test
# ---------------------------------------------------
async def integration_test():
    if not GRAPH_AVAILABLE:
        print("Graph dependencies not available. Integration test skipped.")
        return

    print("Running integration test with real graph invocation...\n")
    tools = [fetch_metrics_tool, create_query_tool, fetch_query_result_tool]
    graph = build_graph(tools=tools, llm=query_llm)

    result = await graph.ainvoke(
        {"messages": [
            HumanMessage(content="fetch Bitcoin market cap growth rate"),
            SystemMessage(content="You are a DBT agent. First call fetch_metrics tool, then create_query, then fetch_query_result tools. Restrict total number of tool call to 5 times."),
        ]},
        config={"callbacks": [lf_handler]}
    )

    raw_messages = []
    for msg in result["messages"]:
        raw_messages.append({
            "type": msg.__class__.__name__.replace("Message", "").lower(),
            "content": getattr(msg, "content", ""),
            "tool_calls": getattr(msg, "tool_calls", []),
        })

    messages = [convert_message(m) for m in raw_messages]
    await evaluate_messages(messages)


# ---------------------------------------------------
# Main Entry
# ---------------------------------------------------
if __name__ == "__main__":
    import sys
    validate_only_flag = "--validate-only" in sys.argv
    integration_flag = "--integration" in sys.argv

    if integration_flag:
        asyncio.run(integration_test())
    else:
        asyncio.run(unit_test(validate_only=validate_only_flag))
