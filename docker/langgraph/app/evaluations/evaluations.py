from tests.utils.runner import load_scenario , run_scenario
from agents.dbt_agents import query_llm
from tools.dbt_tools import fetch_metrics_tool, create_query_tool, fetch_query_result_tool, search_dimension_values_tool
from coin_gecko import build_graph, lf
from langfuse import observe 
import asyncio



tools = [fetch_metrics_tool, create_query_tool, fetch_query_result_tool, search_dimension_values_tool]
graph = build_graph(tools=tools, llm=query_llm)


@observe()
async def evaluate_scenario_from_yaml(path="/app/tests/scenarios/complex_bitcoin.yml"):
    scenario = load_scenario(path)
    tool_score = await run_scenario(scenario, graph=graph,simulate=False)
    
    # Attempt to log scores for current trace
    try:
        trace_id = lf.get_current_trace_id()
        lf.create_score(
            trace_id=trace_id,
            name=f"{scenario['input']}_general_score",
            value=tool_score["general_score"]
        )
        lf.create_score(
            trace_id=trace_id,
            name=f"{scenario['input']}_tool_accuracy",
            value=tool_score["tool_accuracy"]
        )
        lf.create_score(
            trace_id=trace_id,
            name=f"{scenario['input']}_final_output_scorey",
            value=tool_score["final_output_score"]
        )
    except Exception as e:
        print(f"Warning: Unable to log score to Langfuse: {e}")

    print(f"Scenario '{scenario['input']}' Tool Call Accuracy:", tool_score)
    return tool_score


if __name__ == "__main__":
    asyncio.run(evaluate_scenario_from_yaml())
