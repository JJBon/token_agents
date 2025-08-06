from tools.dbt_tools import fetch_metrics_tool, create_query_tool, fetch_query_result_tool, search_dimension_values_tool
import asyncio
from coin_gecko import  lf_handler, lf
from evaluations.utils.runner import run_scenario_langraph
from agents.query_agent.graph import build_graph 
import uuid
# ----------------------- 
# Dataset Evaluation
# -----------------------

graph = build_graph()

async def evaluate_dataset(dataset_name: str):
    session_id = f"{dataset_name}_tests_{uuid.uuid4()}"
    dataset = lf.get_dataset(dataset_name)
    items = dataset.items

    print(f"Evaluating dataset: {dataset.name}, total items: {len(items)}")
    print(f"Using session_id: {session_id} for all traces")

    all_scores = []

    for item in items:
        user_input = item.input
        print(f"Running test for input: {user_input}")

        # Run scenario with Langfuse config
        tool_score = await run_scenario_langraph(
            item,
            simulate=False,
            graph=graph,
            config={
                "callbacks": [lf_handler],
                "configurable": {
                    "thread_id": str(uuid.uuid4())
                },
                "metadata": {
                    "langfuse_session_id": session_id
                },
                "tags": ["evaluation"]
            }
        )

        all_scores.append(tool_score)

        # Log per-trace scores
        try:
            trace_id = lf_handler.last_trace_id
            for key, value in tool_score.items():
                lf.create_score(trace_id=trace_id, name=key, value=value)
        except Exception as e:
            print(f"Warning: Unable to log score to Langfuse: {e}")

    # 🧮 Compute average scores
    if all_scores:
        avg_scores = {
            key: round(sum(s[key] for s in all_scores) / len(all_scores), 4)
            for key in all_scores[0]
        }

        print("\n✅ Average Scores:")
        for key, value in avg_scores.items():
            print(f"{key}: {value}")

        # 📝 Log to session-level metadata
        try:
            for key, value in avg_scores.items():
                lf.create_score(
                    name=key,
                    value=value,
                    session_id=session_id,
                )
        except Exception as e:
            print(f"Warning: Failed to log session-level scores: {e}")

    print(f"\n✅ Evaluation complete for dataset: {dataset_name}")
    print(f"Langfuse session ID: {session_id}")
  


# -----------------------
# Entry Point
# -----------------------
if __name__ == "__main__":
    asyncio.run(evaluate_dataset("query_inputs"))
