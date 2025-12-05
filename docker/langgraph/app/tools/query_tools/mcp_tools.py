# tools/query_tools/mcp_tools.py

from __future__ import annotations
import json, os
from typing import Any, Dict, List, Optional
from mcp.server.fastmcp import FastMCP

# Import your models
from tools.query_tools.dbt_client import (
    dbt_client,
    CreateQueryInput, GroupByField, WhereCondition, FilterField
)

MCP_TRANSPORT = os.getenv("MCP_TRANSPORT", "streamable-http").strip()
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8001"))

mcp = FastMCP("dbt-semantic", host=MCP_HOST, port=MCP_PORT)

@mcp.tool()
def fetch_metrics() -> str:
    """List available metrics and dimensions (JSON)."""
    return json.dumps(dbt_client.fetchMetrics(), indent=2)

@mcp.tool()
def search_dimension_values(dimension: str, query: str, max_results: int = 20) -> str:
    """Search cached dimension values (BTC→Bitcoin)."""
    try:
        result = dbt_client.search_dimension_values(dimension=dimension, query=query, max_results=max_results)
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"status": "ERROR", "error": str(e)})

# ❗ KEY CHANGE: expand arguments so FastMCP emits a real JSON Schema
@mcp.tool()
def create_query(
    metrics: List[str],
    group_by: Optional[List[GroupByField]] = None,
    order_by: Optional[List[str]] = None,         # e.g. ["average_price_usd desc"]
    limit: Optional[int] = 10,
    where: Optional[WhereCondition] = None         # object with {"conditions":[...], "logic":"AND|OR"}
) -> str:
    """
    Validate a query against cached metadata. Returns CreateQueryResponse (JSON).
    """
    try:
        qi = CreateQueryInput(
            metrics=metrics, group_by=group_by, order_by=order_by, limit=limit, where=where
        )
        created = dbt_client.createQuery(qi)
        return created.model_dump_json(indent=2)
    except Exception as e:
        # Put a helpful hint for the model
        return json.dumps({
            "status": "ERROR",
            "error": str(e),
            "hint": "order_by is list[str] like ['metric desc']; "
                    "group_by is list[GroupByField]; "
                    "where is {'conditions':[FilterField|WhereCondition], 'logic':'AND'|'OR'}"
        }, indent=2)

@mcp.tool()
def fetch_query_result(
    metrics: List[str],
    group_by: Optional[List[GroupByField]] = None,
    order_by: Optional[List[str]] = None,
    limit: Optional[int] = 10,
    where: Optional[WhereCondition] = None
) -> str:
    """
    Execute a MetricFlow query. Returns FetchResultsResponse (JSON) with a Markdown table.
    """
    try:
        qi = CreateQueryInput(
            metrics=metrics, group_by=group_by, order_by=order_by, limit=limit, where=where
        )
        results = dbt_client.run_query_from_dict(qi)
        return results.model_dump_json(indent=2)
    except Exception as e:
        return json.dumps({"status": "ERROR", "error": str(e)}, indent=2)

if __name__ == "__main__":
    if MCP_TRANSPORT == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(transport="streamable-http")  # served at /mcp
