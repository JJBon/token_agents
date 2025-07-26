import os
import logging
from fastmcp import FastMCP
import sys
import json
import os
import logging
import uuid
import subprocess
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import re
import os, logging
from fastmcp.tools.tool import ToolResult
from typing import List, Optional, Literal, Any
from enum import Enum
from pydantic import BaseModel, Field, model_validator
from fastmcp import Context

logger = logging.getLogger(__name__)
from .dbt_tools import dbt_client, CreateQueryInput, CreateQueryResponse, FetchResultsResponse

mcp = FastMCP(name="dbt-semantic-python")

#######################################
# Minimal dbtCoreClient Stub 
# with File-based Metrics Cache (unchanged)
#######################################


@mcp.tool()
async def initialize(protocolVersion: str, capabilities: dict, clientInfo: dict) -> dict:
    logging.info(f"Initialize called with protocolVersion={protocolVersion}, clientInfo={clientInfo}")
    return {
        "serverInfo": {
            "name": "dbt-semantic-mcp",
            "version": "0.1.0",
        },
        "capabilities": {}
    }


@mcp.tool()
def get_documentation() -> str:
    """Return usage guide for all MCP methods."""
    return json.dumps({
        "tools": [
            {
                "name": "get_documentation", "description": "...", "inputSchema": {}
            },
            {
                "name": "fetch_metrics", "description": "...", "inputSchema": {}
            },
            {
                "name": "create_query",
                "description": "...",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "metrics": {"type": "array", "items": {"type": "string"}},
                        "groupBy": {"type": "array", "items": {"type": "string"}},
                        "limit": {"type": "number"},
                        "orderBy": {"type": "array", "items": {"type": "string"}}
                    },
                    "required": ["metrics"]
                }
            },
            {
                "name": "fetch_query_result",
                "description": "...",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "object",
                            "properties": {
                                "metrics": {"type": "array", "items": {"type": "string"}},
                                "groupBy": {"type": "array", "items": {"type": "string"}},
                                "limit": {"type": "number"},
                                "orderBy": {"type": "array", "items": {"type": "string"}}
                            },
                            "required": ["metrics"]
                        }
                    },
                    "required": ["query"]
                }
            }
        ]
    })

@mcp.tool()
async def fetch_metrics(ctx: Context):
    result = dbt_client.fetchMetrics()
    return result

@mcp.tool()
def create_query(input_data: CreateQueryInput) -> CreateQueryResponse:
    """
    Create and validate a metrics query. Returns STATUS and structured query.
    """
    created = dbt_client.createQuery(input_data)

    if created.status == "ERROR":
        return CreateQueryResponse(
            status="ERROR",
            query=created.query,
            error=created.error
        )

    return CreateQueryResponse(
        status="CREATED",
        query=created.query
    )


@mcp.tool()
def fetch_query_result(input_data: CreateQueryInput) -> FetchResultsResponse:
    """
    Run the given query object and return the results.
    """
    results = dbt_client.run_query_from_dict(input_data)

    if results.status == "ERROR":
        return FetchResultsResponse(
            status="ERROR",
            results="",
            error=results.error
        )

    return FetchResultsResponse(
        status="SUCCESSFUL",
        results=results.results
    )

if __name__ == "__main__":
    logging.info("Starting MCP server over stdio...")
    mcp.run(transport="stdio")