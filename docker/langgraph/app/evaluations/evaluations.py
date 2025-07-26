import asyncio
import json
import logging

from coin_gecko import build_graph, lf_handler
from ragas.integrations.langgraph import convert_to_ragas_messages
from ragas.metrics import ToolCallAccuracy
from ragas.dataset_schema import MultiTurnSample
from ragas.messages import ToolCall, HumanMessage as RagasHuman, AIMessage as RagasAI, ToolMessage as RagasTool

from langchain_core.messages import HumanMessage, AIMessage, ToolMessage, SystemMessage
from tools.dbt_tools import fetch_metrics_tool, create_query_tool, fetch_query_result_tool
from agents.dbt_agents import query_llm

logging.basicConfig(level=logging.DEBUG)

tools = [fetch_metrics_tool, create_query_tool, fetch_query_result_tool]
graph = build_graph(tools=tools, llm=query_llm)


# -----------------------
# Flatten AI content
# -----------------------
def flatten_ai_content(msg: AIMessage):
    """
    Convert AIMessage content (which may be a list of segments) into a simple string
    while preserving tool_calls.
    """
    if isinstance(msg, AIMessage) and isinstance(msg.content, list):
        text_parts = [seg.get("text", "") for seg in msg.content if seg.get("type") == "text"]
        msg.content = " ".join(text_parts).strip()
    return msg


# -----------------------
# Convert LangChain messages to RAGAS messages
# -----------------------
def to_ragas_message_list(messages):
    """
    Converts a list of LangChain messages into RAGAS messages
    while preserving tool_call relationships.
    """
    ragas_msgs = []
    pending_tool_calls = None

    for m in messages:
        if isinstance(m, HumanMessage):
            ragas_msgs.append(RagasHuman(content=m.content))

        elif isinstance(m, AIMessage):
            # Flatten content
            m = flatten_ai_content(m)
            tool_calls = [
                ToolCall(name=tc["name"], args=tc.get("args", {}))
                for tc in getattr(m, "tool_calls", [])
            ] if getattr(m, "tool_calls", None) else None

            ragas_msgs.append(RagasAI(content=m.content, tool_calls=tool_calls))
            pending_tool_calls = bool(tool_calls)

        elif isinstance(m, ToolMessage):
            if not pending_tool_calls:
                logging.warning(
                    f"ToolMessage {m} does not follow an AIMessage with tool_calls."
                )
            ragas_msgs.append(RagasTool(content=m.content))
            pending_tool_calls = False

        elif isinstance(m, SystemMessage):
            # Skip SystemMessage for RAGAS evaluation
            continue

        else:
            logging.warning(f"Skipping unsupported message type: {m}")

    return ragas_msgs

def debug_ragas_messages(ragas_msgs):
    """Prints RAGAS messages in JSON-friendly format with tool_calls info."""
    formatted_msgs = []

    for m in ragas_msgs:
        if isinstance(m, RagasHuman):
            formatted_msgs.append({
                "type": "human",
                "content": m.content,
                "metadata": m.metadata
            })
        elif isinstance(m, RagasAI):
            formatted_msgs.append({
                "type": "ai",
                "content": m.content,
                "tool_calls": [
                    {"name": tc.name, "args": tc.args}
                    for tc in (m.tool_calls or [])
                ],
                "metadata": m.metadata
            })
        elif isinstance(m, RagasTool):
            formatted_msgs.append({
                "type": "tool",
                "content": m.content,
                "metadata": m.metadata
            })
        else:
            formatted_msgs.append({"type": str(type(m)), "content": str(m)})

    print("==== DEBUG RAGAS MESSAGES ====")
    print(json.dumps(formatted_msgs, indent=2))
    print("==============================")

# -----------------------
# Evaluation
# -----------------------
async def test_evaluation():
    logging.info("Starting evaluation...")

    # Simulate user request
    result = await graph.ainvoke(
        {"messages": [
            HumanMessage(content="fetch Bitcoin market cap growth rate"),
            SystemMessage(content="You are a DBT agent. First call fetch_metrics tool, "
                                  "then create_query, then fetch_query_result tools. "
                                  "Restrict total number of tool call to 5 times."),
        ]},
        config={"callbacks": [lf_handler]}
    )

    messages = result["messages"]
    for msg in messages:
        logging.debug(f"message is of type {type(msg)} -> content: {getattr(msg, 'content', '')}")

    ragas_msgs = to_ragas_message_list(messages)

    
    debug_ragas_messages(ragas_msgs)


    # Create MultiTurnSample
    sample = MultiTurnSample(
        user_input=ragas_msgs,
        reference_tool_calls=[
            ToolCall(name="fetch_metrics", args={}),
            ToolCall(name="create_query", args={"metrics": ["bitcoin_market_cap_growth_rate"]}),
            ToolCall(name="fetch_query_result", args={"metrics": ["bitcoin_market_cap_growth_rate"]}),
        ],
    )

    scorer = ToolCallAccuracy()
    score = await scorer.multi_turn_ascore(sample)
    print("Tool Call Accuracy:", score)


if __name__ == "__main__":
    asyncio.run(test_evaluation())
