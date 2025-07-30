import asyncio
import json
import logging

from langfuse import get_client
from ragas.integrations.langgraph import convert_to_ragas_messages
from ragas.messages import ToolCall
from prompts.prompts import query_agent_system_prompt

from base_evaluator2 import evaluate_messages, convert_message
from agents.dbt_agents import query_llm
from tools.dbt_tools import (
    fetch_metrics_tool,
    create_query_tool,
    fetch_query_result_tool,
    search_dimension_values_tool
)

from coin_gecko import build_graph, lf_handler

# -----------------------
# Logging Setup
# -----------------------
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# -----------------------
# Langfuse Client
# -----------------------
lf = get_client()

# -----------------------
# Tools and Graph
# -----------------------
tools = [fetch_metrics_tool, create_query_tool, fetch_query_result_tool, search_dimension_values_tool]
graph = build_graph(tools, query_llm)


# -----------------------
# Dataset Evaluation
# -----------------------
async def evaluate_dataset(dataset_name: str):
    dataset = lf.get_dataset(dataset_name)
    items = dataset.items

    print(f"Evaluating dataset: {dataset.name}, total items: {len(items)}")

    for item in items:
        user_input = item.input
        expected_output = item.expected_output

        print(f"Running test for input: {user_input}")

        # Run the agent graph
        result = await graph.ainvoke(
            {
                "messages": [
                    {"role": "system", "content": query_agent_system_prompt.prompt},
                    {"role": "user", "content": user_input}
                ]
            },
            config={"callbacks": [lf_handler]}
        )

        # # Convert messages to Ragas messages
        ragas_msgs = [convert_message(m) for m in result["messages"]]

        # # Reference tool calls
        expected_tools = [
            ToolCall(name=t["name"], args=t.get("args", {}))
            for t in expected_output.get("expected_tools", [])
        ]

        # # Final assistant output
        final_output = str(result["messages"][-1].content)

        # # Evaluate messages with Ragas-based evaluator
        scores = await evaluate_messages(ragas_msgs, expected_tools, final_output)

        print(f"Scores for '{user_input}': {json.dumps(scores, indent=2)}")

        # # Log evaluation result to Langfuse
        lf.create_event(
            name="tool-call-evaluation",
            input=user_input,
            output={
                "messages": [str(m) for m in result["messages"]],
                "evaluation_scores": scores
            }
        )

    print("Evaluation completed.")


# -----------------------
# Entry Point
# -----------------------
if __name__ == "__main__":
    asyncio.run(evaluate_dataset("Fetch Bitcoin Weekly Data"))
