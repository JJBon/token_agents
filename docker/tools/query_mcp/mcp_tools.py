# tools/query_tools/mcp_tools.py

from __future__ import annotations
import json, os
from typing import Any, Dict, List, Optional
from mcp.server.fastmcp import FastMCP
from aws_creds import aws_env_from_id_token

import contextvars


ID_TOKEN_CTX = contextvars.ContextVar("id_token", default=None)

# Import your models
from dbt_client import (
    dbt_client,
    CreateQueryInput, GroupByField, WhereCondition, FilterField
)

MCP_TRANSPORT = os.getenv("MCP_TRANSPORT", "streamable-http").strip()
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8001"))

mcp = FastMCP("dbt-semantic", host=MCP_HOST, port=MCP_PORT)

try:
    from fastapi import Request
    @mcp.http.middleware("http")
    async def capture_id_token(request: Request, call_next):
        auth = request.headers.get("authorization", "")
        if isinstance(auth, str) and auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
            ID_TOKEN_CTX.set(token)
        return await call_next(request)
except Exception:
    pass  # stdio transport won't have HTTP middleware — you'll pass token another way if needed

def _user_env() -> dict:
    tok = ID_TOKEN_CTX.get()
    if not tok:
        return {}  # fallback to server creds (not recommended for prod)
    return aws_env_from_id_token(tok)

@mcp.tool()
def fetch_metrics() -> str:
    env = _user_env()
    """List available metrics and dimensions (JSON)."""
    return json.dumps(dbt_client.fetchMetrics(), indent=2)

@mcp.tool()
def search_dimension_values(dimension: str, query: Optional[str] = None, max_results: int = 20, metric: Optional[str] = None) -> str:
    """Search cached dimension values (BTC→Bitcoin). If query is empty, list up to max_results values."""
    env = _user_env()
    try:
        res = dbt_client.search_dimension_values(dimension=dimension, query=query, max_results=max_results, metric=metric)
        return json.dumps(res, indent=2)
    except Exception as e:
        # last-resort fallback so the tool is always useful
        fallback = {
            "dimension": dimension,
            "query": query,
            "matches": dbt_client._fallback_dimension_values(dimension, max_results),
            "status": "FALLBACK",
            "error": str(e),
        }
        return json.dumps(fallback, indent=2)

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
    env = _user_env()
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
    env = _user_env()
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
