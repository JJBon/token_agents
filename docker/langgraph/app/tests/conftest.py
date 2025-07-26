import pytest
from coin_gecko import build_graph
from tools.dbt_tools import fetch_metrics_tool, create_query_tool, fetch_query_result_tool
from agents.dbt_agents import query_llm

@pytest.fixture
def graph():
    tools = [fetch_metrics_tool, create_query_tool, fetch_query_result_tool]
    return build_graph(tools=tools, llm=query_llm)
