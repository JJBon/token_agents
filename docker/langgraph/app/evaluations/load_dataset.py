from tools.dbt_tools import fetch_metrics_tool, create_query_tool, fetch_query_result_tool, search_dimension_values_tool
import asyncio
from coin_gecko import  lf_handler, lf
from app.evaluations.utils.runner import run_scenario_langraph
from agents.query_agent.graph import graph 


# -----------------------
# Dataset Evaluation
# -----------------------
async def evaluate_dataset(dataset_name: str):
    dataset = lf.get_dataset(dataset_name)
    items = dataset.items

    print(f"Evaluating dataset: {dataset.name}, total items: {len(items)}")

    for item in items:
        user_input = item.input

        print(f"Running test for input: {user_input}")

        # Run the agent graph
        tool_score = await run_scenario_langraph(item,simulate=False,graph=graph)

        try:
            trace_id = lf_handler.last_trace_id
            lf.create_score(
                trace_id=trace_id,
                name="general_score",
                value=tool_score["general_score"]
            )
            lf.create_score(
                trace_id=trace_id,
                name="tool_accuracy",
                value=tool_score["tool_accuracy"]
            )
            lf.create_score(
                trace_id=trace_id,
                name="final_output_score",
                value=tool_score["final_output_score"]
            )
        except Exception as e:
            print(f"Warning: Unable to log score to Langfuse: {e}")

    print(f"Scenario '{item.input}' Tool Call Accuracy:", tool_score)
    return tool_score




    print("Evaluation completed.")


# -----------------------
# Entry Point
# -----------------------
if __name__ == "__main__":
    asyncio.run(evaluate_dataset("query_inputs"))
