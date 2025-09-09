import logging
import os
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
from typing import List, Optional, Union
from pydantic import BaseModel, Field
from enum import Enum
import difflib

import re


logger = logging.getLogger(__name__)

class DimensionSearchInput(BaseModel):
    dimension: str = Field(..., description="The dimension to search in (e.g., token_day__coin_name).")
    query: str = Field(..., description="The partial query string to match (e.g., BTC).")
    max_results: int = Field(10, description="Maximum number of results to return.")


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
        self.project_dir = os.environ["DBT_PROJECT_PATH"]
        self.manifest_path = os.path.join(self.project_dir, "target", "manifest.json")
        self.metrics_cache_file = os.path.join(self.project_dir, "target", "metrics_cache.json")
        self.dimension_values_file = os.path.join(self.project_dir, "target", "dimension_values.json")

        self._metrics_cache = None
        self._cache_lock = threading.Lock()
        self._cache_loading = False

        self._try_load_metrics_from_file()
        if self._metrics_cache is None:
            self._start_background_cache_loading()

    # ---------------------
    # Cache Management
    # ---------------------
    def _try_load_metrics_from_file(self):
        if os.path.exists(self.metrics_cache_file):
            try:
                with open(self.metrics_cache_file, "r") as f:
                    self._metrics_cache = json.load(f)
                logging.info(f"Loaded metrics+dimensions from {self.metrics_cache_file}")
            except Exception as e:
                logging.error(f"Failed to load metrics cache: {e}")

    def _write_metrics_to_file(self):
        if self._metrics_cache is None:
            return
        try:
            with open(self.metrics_cache_file, "w") as f:
                json.dump(self._metrics_cache, f, indent=2)
            logging.info(f"Saved metrics cache to {self.metrics_cache_file}")
        except Exception as e:
            logging.error(f"Failed to write metrics cache: {e}")

    def _start_background_cache_loading(self):
        with self._cache_lock:
            if not self._cache_loading:
                self._cache_loading = True
                thread = threading.Thread(target=self._build_metrics_cache_background, daemon=True)
                thread.start()

    def _build_metrics_cache_background(self):
        try:
            self._build_metrics_cache()
            self._build_dimension_values_cache()
        except Exception as e:
            logging.error(f"Background metrics cache build failed: {e}")
        finally:
            with self._cache_lock:
                self._cache_loading = False

    # ---------------------
    # Metrics/Dimensions Fetching
    # ---------------------
    def _build_metrics_cache(self):
        logging.info("Building metrics cache...")
        metrics_from_ls = self._get_all_metrics_info()

        manifest_data = {}
        if os.path.exists(self.manifest_path):
            with open(self.manifest_path, "r") as f:
                manifest_data = json.load(f)

        manifest_metrics = manifest_data.get("metrics", {})
        dimensions_map = {}
        metrics_list = []

        for uid, info in metrics_from_ls.items():
            metric_name = info.get("name", "unknown_metric")
            manifest_def = manifest_metrics.get(uid, {})
            description = manifest_def.get("description", info.get("description", ""))

            dimensions_for_metric = self._fetch_dimensions_for_metric(metric_name)

            metrics_list.append({
                "name": metric_name,
                "description": description,
                "dimensions": dimensions_for_metric
            })

            for dim in dimensions_for_metric:
                if dim not in dimensions_map:
                    dim_type = "time" if dim.startswith("metric_time") else "dimension"
                    dimensions_map[dim] = {"type": dim_type, "values": []}

        self._metrics_cache = {"metrics": metrics_list, "dimensions": dimensions_map}
        self._write_metrics_to_file()

    def _get_all_metrics_info(self):
        command = ["dbt", "ls", "--resource-type", "metric", "--output", "json", "--quiet"]
        result = subprocess.run(command, cwd=self.project_dir, capture_output=True, text=True, check=True)
        lines = result.stdout.strip().split("\n")
        return {json.loads(line)["unique_id"]: json.loads(line) for line in lines if line.strip()}

    def _fetch_dimensions_for_metric(self, metric_name: str) -> List[str]:
        command = ["mf", "list", "dimensions", "--metrics", metric_name]
        result = subprocess.run(command, cwd=self.project_dir, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            logging.warning(f"MetricFlow failed for {metric_name}: {result.stderr}")
            return []
        return [line.replace("• ", "").strip() for line in result.stdout.splitlines() if line.startswith("• ")]

    # ---------------------
    # Dimension Values (Cached + Filtered)
    # ---------------------
    def _build_dimension_values_cache(self):
        """Build and store dimension values cache for all dimensions."""
        logging.info("Building dimension values cache...")
        if not self._metrics_cache:
            self._build_metrics_cache()

        dimension_values = {}
        first_metric = self._metrics_cache["metrics"][0]["name"] if self._metrics_cache["metrics"] else None

        for dim, meta in self._metrics_cache.get("dimensions", {}).items():
            if meta["type"] == "dimension":
                values = self._fetch_dimension_values(first_metric, dim)
                dimension_values[dim] = values

        with open(self.dimension_values_file, "w") as f:
            json.dump(dimension_values, f, indent=2)
        logging.info(f"Dimension values cached to {self.dimension_values_file}")

    def _fetch_dimension_values(self, metric_name: str, dimension: str) -> List[str]:
        cmd = ["mf", "list", "dimension-values", "--metrics", metric_name, "--dimension", dimension]
        res = subprocess.run(cmd, cwd=self.project_dir, capture_output=True, text=True, check=False)
        if res.returncode != 0:
            logging.warning(f"list dimension-values failed: {res.stderr}")
            return []

        values = []
        for line in res.stdout.strip().split("\n"):
            line = line.strip()
            if not line or "Retrieving dimension values" in line or "We've found" in line or "✖" in line or "✔" in line:
                continue
            values.append(line.replace("• ", "").strip())
        return values

    def fetch_dimension_values_filtered(self, dimension: str, query: str, max_results: int = 10) -> List[str]:
        """Fetch up to max_results matching values for a dimension from cached data."""
        if not os.path.exists(self.dimension_values_file):
            logging.warning("Dimension cache not found. Rebuilding...")
            self._build_dimension_values_cache()

        with open(self.dimension_values_file, "r") as f:
            dimension_cache = json.load(f)

        all_values = dimension_cache.get(dimension, [])
        matches = difflib.get_close_matches(query, all_values, n=max_results, cutoff=0.3)
        return matches

    def search_dimension_values(self, dimension: str, query: str, max_results: int = 10) -> dict:
        """Public tool to search for dimension values similar to query."""
        matches = self.fetch_dimension_values_filtered(dimension, query, max_results)
        return {"dimension": dimension, "query": query, "matches": matches}

    # ---------------------
    # Public API
    # ---------------------
    def fetchMetrics(self):
        return self._metrics_cache or {"metrics": [], "dimensions": {}}

    def refreshMetrics(self):
        logging.info("Refreshing metrics cache...")
        with self._cache_lock:
            self._build_metrics_cache()
            self._build_dimension_values_cache()
        return self._metrics_cache

    def createQuery(self, query_params: CreateQueryInput) -> CreateQueryResponse:
        """
        Validate metrics, group_by, and where clauses using metrics+dimensions cache.
        Uses cached dimension values for validation instead of full fetch.
        """
        if self._metrics_cache is None:
            self._try_load_metrics_from_file()

        metrics = [m["name"] for m in self._metrics_cache.get("metrics", [])]
        for m in query_params.metrics:
            if m not in metrics:
                return CreateQueryResponse(
                    status="ERROR",
                    query=query_params.dict(),
                    error=f"Metric '{m}' is not defined."
                )

        dimensions = self._metrics_cache.get("dimensions", {})
        for gb in query_params.group_by or []:
            if gb.dimension not in dimensions:
                return CreateQueryResponse(
                    status="ERROR",
                    query=query_params.dict(),
                    error=f"Dimension '{gb.dimension}' is not available."
                )

        if query_params.where:
            for cond in query_params.where.conditions:
                if isinstance(cond, FilterField):
                    if cond.dimension not in dimensions:
                        return CreateQueryResponse(
                            status="ERROR",
                            query=query_params.dict(),
                            error=f"Dimension '{cond.dimension}' is not available."
                        )

                    valid_values = self.fetch_dimension_values_filtered(
                        cond.dimension, cond.value, max_results=20
                    )

                    if valid_values and cond.value not in valid_values:
                        return CreateQueryResponse(
                            status="ERROR",
                            query=query_params.dict(),
                            error=(
                                f"Invalid value '{cond.value}' for dimension '{cond.dimension}'. "
                                f"Did you mean: {valid_values[:5]} ?"
                            )
                        )

        query_dict = query_params.dict()
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


dbt_client = DBTCoreClient()