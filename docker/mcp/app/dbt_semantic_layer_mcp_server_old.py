import os
import json
import logging
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Union, Any
from enum import Enum

from fastmcp import FastMCP, Context
from fastmcp.tools.tool import ToolResult
from pydantic import BaseModel, Field

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

mcp = FastMCP(name="dbt-semantic-python")

#######################################
# ENUMS & MODELS
#######################################

class Logic(str, Enum):
    AND = "AND"
    OR = "OR"


class TimeAggregation(str, Enum):
    day = "day"
    week = "week"
    month = "month"


class DimensionType(str, Enum):
    TIME = "time"
    DIMENSION = "dimension"


class GroupByField(BaseModel):
    type: DimensionType = Field(..., description="Either 'time' or 'dimension'.")
    dimension: str = Field(..., description="The name of the dimension.")
    aggregation: Optional[TimeAggregation] = Field(
        None, description="Only used when type='time'. Example: day, week, month."
    )

    def to_expression(self) -> str:
        if self.type == DimensionType.TIME and self.aggregation:
            return f"{self.dimension}__{self.aggregation.value}"
        return self.dimension


class FilterField(BaseModel):
    type: DimensionType = Field(..., description="Type of dimension: time or regular dimension.")
    dimension: str = Field(..., description="Dimension to filter.")
    operator: str = Field(..., description="Comparison operator (=, !=, <, <=, >, >=).")
    value: str = Field(..., description="Value to compare.")
    aggregation: Optional[TimeAggregation] = Field(
        None, description="Aggregation granularity for time filters."
    )

    def to_expression(self) -> str:
        if self.type == DimensionType.TIME and self.aggregation:
            return f"{self.dimension}__{self.aggregation.value} {self.operator} {self.value}"
        elif self.type == DimensionType.DIMENSION:
            dim_expr = (
                f"{{{{ Dimension('{self.dimension}') }}}}"
                if not self.dimension.startswith("{{ Dimension(")
                else self.dimension
            )
            return f"{dim_expr} {self.operator} '{self.value}'"
        else:
            return f"{self.dimension} {self.operator} {self.value}"


class WhereCondition(BaseModel):
    conditions: List[Union["WhereCondition", FilterField]] = Field(
        ..., description="List of filters or nested where conditions."
    )
    logic: Logic = Field(Logic.AND, description="Logical operator to combine conditions.")

    def to_where_clause(self) -> str:
        parts = []
        for cond in self.conditions:
            if isinstance(cond, WhereCondition):
                parts.append(f"({cond.to_where_clause()})")
            else:
                parts.append(cond.to_expression())
        return f" {self.logic.value} ".join(parts)


class CreateQueryInput(BaseModel):
    metrics: List[str] = Field(..., description="Metrics to query.")
    group_by: Optional[List[GroupByField]] = None
    order_by: Optional[List[str]] = None
    limit: Optional[int] = 5
    where: Optional[WhereCondition] = None

    @property
    def group_by_expressions(self) -> List[str]:
        return [g.to_expression() for g in self.group_by or []]

    @property
    def where_clause(self) -> Optional[str]:
        return self.where.to_where_clause() if self.where else None


class CreateQueryResponse(BaseModel):
    status: str = Field(..., description="Status of the query creation (CREATED or ERROR).")
    query: dict = Field(..., description="The validated query structure.")
    error: Optional[str] = Field(default=None, description="Error message if status=ERROR.")


class FetchResultsResponse(BaseModel):
    status: str = Field(..., description="Status of query execution (SUCCESSFUL or ERROR).")
    results: str = Field(..., description="Formatted query results.")
    error: Optional[str] = Field(default=None, description="Error message if status=ERROR.")


#######################################
# DBT Core Client
#######################################

class DBTCoreClient:
    def __init__(self):
        self.last_query = None
        self.project_dir = os.environ["DBT_PROJECT_PATH"]
        self.manifest_path = os.path.join(self.project_dir, "target", "manifest.json")
        self.metrics_cache_file = os.path.join(self.project_dir, "target", "metrics_cache.json")
        self._metrics_cache = None
        self._cache_lock = threading.Lock()
        self._cache_loading = False

        self._try_load_metrics_from_file()
        if self._metrics_cache is None:
            self._start_background_cache_loading()

    def _try_load_metrics_from_file(self):
        if os.path.exists(self.metrics_cache_file):
            try:
                with open(self.metrics_cache_file, "r") as f:
                    data = json.load(f)
                    if "metrics" in data:
                        self._metrics_cache = data
                        logging.info(f"Loaded metrics from file: {self.metrics_cache_file}")
            except Exception as e:
                logging.error(f"Failed to load metrics from file: {e}")

    def _write_metrics_to_file(self):
        if self._metrics_cache is None:
            return
        try:
            with open(self.metrics_cache_file, "w") as f:
                json.dump(self._metrics_cache, f, indent=2)
            logging.info(f"Wrote metrics cache to file: {self.metrics_cache_file}")
        except Exception as e:
            logging.error(f"Failed to write metrics to file: {e}")

    def _start_background_cache_loading(self):
        with self._cache_lock:
            if not self._cache_loading:
                self._cache_loading = True
                thread = threading.Thread(target=self._build_metrics_cache_background, daemon=True)
                thread.start()

    def _build_metrics_cache_background(self):
        try:
            self._build_metrics_cache()
        except Exception as e:
            logging.error(f"Background cache build failed: {e}")
        finally:
            with self._cache_lock:
                self._cache_loading = False

    def _build_metrics_cache(self):
        logging.info("Building metrics cache...")
        metrics_from_ls = self._get_all_metrics_info()
        manifest_data = {}

        if os.path.exists(self.manifest_path):
            with open(self.manifest_path, "r") as f:
                manifest_data = json.load(f)
        else:
            logging.warning("No manifest.json found. Run dbt compile or dbt build first.")
        manifest_metrics = manifest_data.get("metrics", {})

        def process_metric(unique_id, metric_info):
            metric_name = metric_info.get("name", "unknown_metric")
            manifest_def = manifest_metrics.get(unique_id, {})
            description = manifest_def.get("description") or metric_info.get("description", "")
            dimensions = self._fetch_dimensions_for_metric(metric_name)
            return {"name": metric_name, "description": description, "dimensions": dimensions}

        metrics_list = []
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(process_metric, uid, info): uid for uid, info in metrics_from_ls.items()}
            for future in as_completed(futures):
                try:
                    metrics_list.append(future.result())
                except Exception as e:
                    logging.error(f"Error processing metric {futures[future]}: {e}")

        self._metrics_cache = {"metrics": metrics_list}
        self._write_metrics_to_file()

    def _get_all_metrics_info(self):
        command = ["dbt", "ls", "--resource-type", "metric", "--output", "json", "--quiet"]
        result = subprocess.run(command, cwd=self.project_dir, capture_output=True, text=True, check=True)
        metrics_map = {}
        for line in result.stdout.strip().split("\n"):
            if line.strip():
                metric_data = json.loads(line)
                unique_id = metric_data.get("unique_id")
                if unique_id:
                    metrics_map[unique_id] = metric_data
        return metrics_map

    def _fetch_dimensions_for_metric(self, metric_name: str):
        command = ["mf", "list", "dimensions", "--metrics", metric_name]
        result = subprocess.run(command, cwd=self.project_dir, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            logging.warning(f"MetricFlow command failed for metric={metric_name}: {result.stderr}")
            return []
        return [line.replace("• ", "").strip() for line in result.stdout.splitlines() if line.strip().startswith("• ")]

    def fetchMetrics(self):
        if self._metrics_cache is None:
            self._try_load_metrics_from_file()
        return self._metrics_cache or {"metrics": []}

    def createQuery(self, query_params: CreateQueryInput) -> CreateQueryResponse:
        metrics_list = query_params.metrics
        if not metrics_list:
            return CreateQueryResponse(status="ERROR", query=query_params.dict(), error="Missing required 'metrics' array.")
        query_dict = {"metrics": metrics_list}
        if query_params.group_by_expressions:
            query_dict["group_by"] = query_params.group_by_expressions
        if query_params.limit is not None:
            query_dict["limit"] = query_params.limit
        if query_params.order_by:
            query_dict["order_by"] = query_params.order_by
        if query_params.where_clause:
            query_dict["where"] = query_params.where_clause
        self.last_query = query_dict
        return CreateQueryResponse(status="CREATED", query=query_dict)

    def run_query_from_dict(self, query_dict: CreateQueryInput) -> FetchResultsResponse:
        metrics_list = query_dict.metrics
        if not metrics_list:
            return FetchResultsResponse(status="ERROR", results="", error="No metrics provided.")
        command = ["mf", "query", "--metrics", ",".join(metrics_list)]
        if query_dict.group_by_expressions:
            command.extend(["--group-by", ",".join(query_dict.group_by_expressions)])
        if query_dict.order_by:
            command.extend(["--order", ",".join(query_dict.order_by)])
        if query_dict.where_clause:
            command.extend(["--where", query_dict.where_clause])
        if query_dict.limit is not None:
            command.extend(["--limit", str(query_dict.limit)])
        result = subprocess.run(command, cwd=self.project_dir, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            return FetchResultsResponse(status="ERROR", results="", error=result.stderr)
        return FetchResultsResponse(status="SUCCESSFUL", results=result.stdout)


#######################################
# UTILS
#######################################

def normalize_input(data: Any) -> dict:
    if isinstance(data, str):
        return json.loads(data)
    elif isinstance(data, dict):
        return data
    else:
        raise ValueError(f"Unsupported input_data type: {type(data)}")


#######################################
# MCP TOOLS
#######################################

dbt_client = DBTCoreClient()

@mcp.tool()
async def initialize(protocolVersion: str, capabilities: dict, clientInfo: dict) -> dict:
    return {"serverInfo": {"name": "dbt-semantic-mcp", "version": "0.1.0"}, "capabilities": {}}


@mcp.tool()
def get_documentation() -> str:
    return json.dumps({
        "tools": [
            {"name": "get_documentation", "description": "...", "inputSchema": {}},
            {"name": "fetch_metrics", "description": "...", "inputSchema": {}},
            {
                "name": "create_query",
                "description": "Validate and build a dbt metrics query.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "metrics": {"type": "array", "items": {"type": "string"}},
                        "group_by": {"type": "array", "items": {"type": "string"}},
                        "limit": {"type": "number"},
                        "order_by": {"type": "array", "items": {"type": "string"}},
                        "where": {"type": "object"}
                    },
                    "required": ["metrics"]
                }
            },
            {
                "name": "fetch_query_result",
                "description": "Run the given query and return results.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "metrics": {"type": "array", "items": {"type": "string"}},
                        "group_by": {"type": "array", "items": {"type": "string"}},
                        "limit": {"type": "number"},
                        "order_by": {"type": "array", "items": {"type": "string"}},
                        "where": {"type": "object"}
                    },
                    "required": ["metrics"]
                }
            }
        ]
    })


@mcp.tool()
async def fetch_metrics(ctx: Context):
    return dbt_client.fetchMetrics()


@mcp.tool()
def create_query(input_data: Any) -> CreateQueryResponse:
    query_input = CreateQueryInput(**normalize_input(input_data))
    return dbt_client.createQuery(query_input)


@mcp.tool()
def fetch_query_result(input_data: Any) -> FetchResultsResponse:
    query_input = CreateQueryInput(**normalize_input(input_data))
    return dbt_client.run_query_from_dict(query_input)


if __name__ == "__main__":
    logging.info("Starting MCP server over stdio...")
    mcp.run(transport="stdio")
