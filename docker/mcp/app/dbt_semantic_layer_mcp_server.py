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



logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

mcp = FastMCP(name="dbt-semantic-python")

#######################################
# Minimal dbtCoreClient Stub 
# with File-based Metrics Cache (unchanged)
#######################################


from enum import Enum
from typing import List, Optional, Literal
from pydantic import BaseModel, Field, model_validator

from pydantic import BaseModel, Field, model_validator
from typing import List, Optional, Literal
from enum import Enum

from enum import Enum
from typing import List, Optional, Union
from pydantic import BaseModel, Field


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
    """
    Represents a single group-by field, preserving the order across time and dimension fields.
    """
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
            # Jinja-wrapped dimensions for MetricFlow
            dim_expr = (
                f"{{{{ Dimension('{self.dimension}') }}}}"
                if not self.dimension.startswith("{{ Dimension(")
                else self.dimension
            )
            return f"{dim_expr} {self.operator} '{self.value}'"
        else:
            return f"{self.dimension} {self.operator} {self.value}"


class TimeDimension(BaseModel):
    name: str = Field(..., description="Time-based dimension, e.g., metric_time__week or metric_time__month.")

    @classmethod
    def is_valid_time_dimension(cls, dim: str) -> bool:
        # Define known time grains (you can expand this list)
        time_grains = ["metric_time__day", "metric_time__week", "metric_time__month", "metric_time__quarter", "metric_time__year"]
        return dim in time_grains


class Condition(BaseModel):
    """
    Represents a single condition expression, e.g., "metric_time__week <= current_date".
    """
    expr: str


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


class DBTCoreClient:
    def __init__(self):
        #self.project_dir = os.path.join(os.path.dirname(__file__), "coindbt")
        self.last_query = None
        self.project_dir =  os.environ["DBT_PROJECT_PATH"]
        self.manifest_path = os.path.join(self.project_dir, "target", "manifest.json")
        
        # Path to store metrics JSON file
        self.metrics_cache_file = os.path.join(self.project_dir, "target", "metrics_cache.json")

        self._metrics_cache = None  # will store {"metrics": [ ... ]}
        self._cache_lock = threading.Lock()
        self._cache_loading = False

        # Attempt to load from file on init
        self._try_load_metrics_from_file()
        if self._metrics_cache is None:
            self._start_background_cache_loading()

    def _try_load_metrics_from_file(self):
        """Load metrics from a JSON file if it exists."""
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
        """Write the current metrics cache to a JSON file."""
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
                logging.info("Starting background thread to build metrics cache...")
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
        # 1) Gather all metrics from dbt ls
        metrics_from_ls = self._get_all_metrics_info()

        # 2) Load manifest.json (for better descriptions, etc.)
        manifest_data = {}
        if os.path.exists(self.manifest_path):
            with open(self.manifest_path, "r") as f:
                manifest_data = json.load(f)
        else:
            logging.warning("No manifest.json found. Run dbt compile or dbt build first.")
        manifest_metrics = manifest_data.get("metrics", {})

        # 3) Use a thread pool to concurrently fetch dimensions for each metric
        def process_metric(unique_id, metric_info):
            metric_name = metric_info.get("name", "unknown_metric")
            manifest_def = manifest_metrics.get(unique_id, {})
            description = manifest_def.get("description") or metric_info.get("description", "")
            dimensions = self._fetch_dimensions_for_metric(metric_name)
            return {
                "name": metric_name,
                "description": description,
                "dimensions": dimensions,
            }

        metrics_list = []
        with ThreadPoolExecutor() as executor:
            future_to_uid = {
                executor.submit(process_metric, uid, info): uid 
                for uid, info in metrics_from_ls.items()
            }
            for future in as_completed(future_to_uid):
                try:
                    result = future.result()
                    metrics_list.append(result)
                except Exception as e:
                    uid = future_to_uid[future]
                    logging.error(f"Error processing metric {uid}: {e}")

        # 4) Store the results in our cache and write them to file
        self._metrics_cache = {"metrics": metrics_list}
        logging.info("Metrics cache built successfully.")
        self._write_metrics_to_file()

    def _get_all_metrics_info(self):
        logging.info("Fetching metrics from dbt with `dbt ls`...")
        command = [
            "dbt",
            "ls",
            "--resource-type", "metric",
            "--output", "json",
            "--quiet"
        ]
        result = subprocess.run(
            command,
            cwd=self.project_dir,
            capture_output=True,
            text=True,
            check=True
        )
        lines = result.stdout.strip().split("\n")
        metrics_map = {}
        for line in lines:
            line = line.strip()
            if not line:
                continue
            metric_data = json.loads(line)
            unique_id = metric_data.get("unique_id")
            if unique_id:
                metrics_map[unique_id] = metric_data
        return metrics_map

    def _fetch_dimensions_for_metric(self, metric_name: str):
        command = ["mf", "list", "dimensions", "--metrics", metric_name]
        logging.info(f"Running: {command}")
        result = subprocess.run(
            command,
            cwd=self.project_dir,
            capture_output=True,
            text=True,
            check=False
        )
        if result.returncode != 0:
            logging.warning(f"MetricFlow command failed for metric={metric_name}. "
                            f"Return code={result.returncode}, stderr={result.stderr}")
            return []
        lines = result.stdout.strip().split("\n")
        dimension_list = []
        for line in lines:
            line = line.strip()
            if not line or line.startswith("✔"):
                continue
            if line.startswith("• "):
                dim_name = line.replace("• ", "").strip()
                dimension_list.append(dim_name)
        return list(set(dimension_list))

    def fetchMetrics(self):
        """Return the metrics from our in-memory or file-based cache."""
        if self._metrics_cache is None:
            logging.warning("Metrics cache not ready in memory.")
            self._try_load_metrics_from_file()

        if self._metrics_cache is None:
            logging.warning("Metrics cache still not ready. Returning empty result.")
            return {"metrics": []}

        return self._metrics_cache

    def refreshMetrics(self):
        """Forcibly re-build the metrics cache (synchronously)."""
        logging.info("Refreshing metrics cache (synchronously)...")
        with self._cache_lock:
            self._build_metrics_cache()
        return self._metrics_cache

    def _find_dimensions_for_metric(self, metric_name: str):
        """Return the valid dimensions for a given metric."""
        if not self._metrics_cache:
            return []
        for m in self._metrics_cache["metrics"]:
            if m["name"] == metric_name:
                return m["dimensions"]
        return []

    #######################################
    # createQuery returns the *structure*
    # we want to pass to fetch_query_result
    #######################################
    
    def createQuery(self, query_params: CreateQueryInput) -> CreateQueryResponse:
        """
        Validates and constructs a MetricFlow query dictionary from CreateQueryInput.
        """
        metrics_list = query_params.metrics
        group_by_fields = query_params.group_by_expressions
        where_clause = query_params.where_clause
        limit = query_params.limit
        order_by = query_params.order_by or []

        # 1. Validate presence of metrics
        if not metrics_list:
            return CreateQueryResponse(
                status="ERROR",
                query=query_params.dict(),
                error="Missing required 'metrics' array."
            )

        # 2. Validate group-by dimensions (time-based group-bys are skipped)
        invalid_dims = []
        for metric_name in metrics_list:
            valid_dims = self._find_dimensions_for_metric(metric_name)
            for gb in query_params.group_by or []:
                if gb.type == DimensionType.DIMENSION:
                    dim_name = gb.to_expression()
                    if dim_name not in valid_dims:
                        invalid_dims.append((metric_name, dim_name))

        if invalid_dims:
            error_lines = [
                f"Dimension '{dim}' not valid for metric '{metric}'. "
                f"Valid dims: {self._find_dimensions_for_metric(metric)}"
                for metric, dim in invalid_dims
            ]
            return CreateQueryResponse(
                status="ERROR",
                query=query_params.dict(),
                error="\n\n".join(error_lines)
            )

        # 3. Build query dict
        query_dict = {"metrics": metrics_list}

        if group_by_fields:
            query_dict["group_by"] = group_by_fields
        if limit is not None:
            query_dict["limit"] = limit
        if order_by:
            query_dict["order_by"] = order_by
        if where_clause:
            query_dict["where"] = where_clause

        self.last_query = query_dict
        return CreateQueryResponse(status="CREATED", query=query_dict)
    #######################################
    # Instead of referencing a stored queryId,
    # we run the query from the provided dict
    #######################################

    import re

    def parse_metricflow_table(self,raw_output: str) -> list[dict]:
        """
        Parse space-aligned MetricFlow table output like:

            metric_time__month      max_price_volatility_all_coins    min_price_volatility_all_coins ...
            --------------------    ------------------------------     -------------------------------
            2024-03-01T00:00:00     0.119452                          0.0220735
            ...

        Returns a list of dict rows, e.g.:
        [
        {
            "metric_time__month": "2024-03-01T00:00:00",
            "max_price_volatility_all_coins": "0.119452",
            "min_price_volatility_all_coins": "0.0220735",
            ...
        },
        ...
        ]
        """
        lines = raw_output.strip().split("\n")

        # 1) Strip out spinner/log lines, e.g. containing “✔” or “Success” or “Initiating query”:
        #    (Adjust as needed)
        data_lines = [
            line for line in lines
            if line and not any(sub in line for sub in ["⠋", "✔", "🖨", "Initiating query", "Success", "written query"])
        ]

        # 2) If there’s nothing left, return empty
        if not data_lines:
            return []

        # The first non-dashed line should be the header (e.g. "metric_time__month      max_price...")
        header_line = data_lines[0]

        # The second line is usually the dashed "----" line. We can skip it:
        #   --------------------  ------------------------------  ...
        #   But let's be robust in case sometimes there's no dashed line.
        #   We'll look for the first "----" line in data_lines.
        dashed_line_idx = None
        for idx, line in enumerate(data_lines):
            if re.match(r"^\s*-+\s*-+\s*", line):
                dashed_line_idx = idx
                break

        # If we found a dashed line, the data lines start after that line
        data_start_idx = dashed_line_idx + 1 if dashed_line_idx is not None else 1

        # 3) Split the header line on 2+ spaces to get column names
        #    e.g. "metric_time__month      max_price_volatility_all_coins" -> columns
        header_cols = re.split(r"\s{2,}", header_line.strip())

        # 4) For each subsequent line, split on 2+ spaces and map to the corresponding column
        table_rows = []
        for line in data_lines[data_start_idx:]:
            # If it's another dashed line or blank, skip
            if re.match(r"^\s*-+\s*$", line):
                continue

            cols = re.split(r"\s{2,}", line.strip())

            # If the line doesn't have the same number of columns as the header,
            # skip or handle gracefully
            if len(cols) != len(header_cols):
                continue

            row_dict = {}
            for col_name, value in zip(header_cols, cols):
                row_dict[col_name] = value

            table_rows.append(row_dict)

        return table_rows
    
    def run_query_from_dict(self, query_dict: CreateQueryInput) -> FetchResultsResponse:
        """
        Executes a MetricFlow query using the given CreateQueryInput.
        Returns a FetchResultsResponse with status, formatted results, and optional error.
        """
        metrics_list = query_dict.metrics
        group_bys = query_dict.group_by_expressions
        limit_value = query_dict.limit
        order_by = query_dict.order_by or []
        where_clause = query_dict.where_clause

        # 1. Validate metrics
        if not metrics_list:
            return FetchResultsResponse(
                status="ERROR",
                results="No results due to missing metrics.",
                error="No metrics provided."
            )

        # 2. Build MetricFlow CLI command
        command = ["mf", "query"]
        command.extend(["--metrics", ",".join(metrics_list)])

        if group_bys:
            command.extend(["--group-by", ",".join(group_bys)])

        if order_by:
            command.extend(["--order", ",".join(order_by)])

        if where_clause:
            # Enclose WHERE clause in single quotes to avoid shell interpretation issues
            command.extend(["--where", where_clause])

        if limit_value is not None:
            command.extend(["--limit", str(limit_value)])

        logging.info(f"Running MetricFlow query: {' '.join(command)}")

        # 3. Execute the query
        try:
            result = subprocess.run(
                command,
                cwd=self.project_dir,
                capture_output=True,
                text=True,
                check=False
            )

            if result.returncode != 0:
                return FetchResultsResponse(
                    status="ERROR",
                    results="No results due to query failure.",
                    error=f"MetricFlow failed with code {result.returncode}: {result.stderr}"
                )

            parsed_rows = self.parse_metricflow_table(result.stdout)

            if not parsed_rows:
                return FetchResultsResponse(
                    status="SUCCESSFUL",
                    results="No rows returned by the query.",
                    error=None
                )

            # Format results as Markdown table
            header = list(parsed_rows[0].keys())
            table = "| " + " | ".join(header) + " |\n"
            table += "| " + " | ".join("---" for _ in header) + " |\n"
            for row in parsed_rows[:10]:  # Top 10 rows
                table += "| " + " | ".join(str(row.get(col, "")) for col in header) + " |\n"

            return FetchResultsResponse(
                status="SUCCESSFUL",
                results=f"### Query Results:\n\n{table}",
                error=None
            )

        except Exception as e:
            logging.exception("Unexpected error running MetricFlow query")
            return FetchResultsResponse(
                status="ERROR",
                results="No results due to an exception.",
                error=str(e)
            )

#######################################
# Instance
#######################################
dbt_client = DBTCoreClient()

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