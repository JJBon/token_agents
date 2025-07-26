from langchain_core.tools import StructuredTool
import json
from .dbt_client import dbt_client,CreateQueryInput


# -----------------------
# Define Tools
# -----------------------
def fetch_metrics() -> str:
    result = dbt_client.fetchMetrics()
    return json.dumps(result, indent=2)


def create_query(**kwargs) -> str:
    try:
        query_input = CreateQueryInput(**kwargs)
        created = dbt_client.createQuery(query_input)
        return json.dumps(created.model_dump(), indent=2)
    except Exception as e:
        return json.dumps({"status": "ERROR", "error": str(e)})


def fetch_query_result(**kwargs) -> str:
    try:
        query_input = CreateQueryInput(**kwargs)
        results = dbt_client.run_query_from_dict(query_input)
        return json.dumps(results.model_dump(), indent=2)
    except Exception as e:
        return json.dumps({"status": "ERROR", "error": str(e)})


# -----------------------
# Structured Tools
# -----------------------
fetch_metrics_tool = StructuredTool.from_function(
    func=fetch_metrics,
    name="fetch_metrics",
    description="Fetch available metrics from the dbt semantic layer."
)

create_query_tool = StructuredTool.from_function(
    func=create_query,
    name="create_query",
    description="Create and validate a metrics query.",
    args_schema=CreateQueryInput
)

fetch_query_result_tool = StructuredTool.from_function(
    func=fetch_query_result,
    name="fetch_query_result",
    description="Run a metrics query and return results.",
    args_schema=CreateQueryInput
)
