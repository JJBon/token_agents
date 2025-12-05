import logging
import os
import json
import threading
import subprocess
from typing import List, Optional, Union, Literal, Dict
from pydantic import BaseModel, Field
from enum import Enum
import difflib
import re

# ---------- Output cleaning helpers ----------
ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")  # strip ANSI
SPINNER_PREFIXES = ("⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏")
NOISE_STARTS = (
    "‼️ Warning:", "💡 Please update", "✔ Success", "Initiating query", "written query",
)
DASH_LINE_RE = re.compile(r"^\s*-{2,}(?:\s+-{2,})+\s*$")  # lines of ----  ---- ...
COL_SPLIT_RE = re.compile(r"\s{2,}")  # split on 2+ spaces

logger = logging.getLogger(__name__)

# ---------- Models ----------
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
    """Represents a single group-by field, preserving the order across time and dimension fields."""
    type: DimensionType = Field(..., description="Either 'time' or 'dimension'.")
    dimension: str = Field(..., description="The name of the dimension.")
    aggregation: Optional[TimeAggregation] = Field(
        None, description="Only used when type='time'. Example: day, week, month."
    )

    def to_expression(self) -> str:
        if self.type == DimensionType.TIME and self.aggregation:
            return f"{self.dimension}__{self.aggregation.value}"
        return self.dimension


class Grouping(BaseModel):
    """Wraps group-by fields and exposes the resolved dimension names."""
    items: List[GroupByField] = Field(default_factory=list)

    @property
    def dims(self) -> set[str]:
        return {g.to_expression() for g in self.items}

    def contains(self, dim: str) -> bool:
        return dim in self.dims


class OrderDirection(str, Enum):
    ASC = "ASC"
    DESC = "DESC"


class OrderTarget(str, Enum):
    METRIC = "metric"
    DIMENSION = "dimension"   # includes time grains like metric_time__week


class OrderByField(BaseModel):
    """
    ORDER BY spec that supports +/- shorthand and enforces dimension vs metric.
    Accepts:
      "+average_price_usd"
      "-average_market_cap_usd"
      "+token_day__coin_name"
      "-metric_time__week"
      "average_price_usd DESC"
      "token_day__coin_name ASC"
    """
    target: OrderTarget = Field(..., description="'metric' or 'dimension'")
    name: str = Field(..., description="Metric or dimension name (already grain-applied if time).")
    direction: OrderDirection = Field(OrderDirection.DESC)

    def to_expression(self) -> str:
        return f"{self.name} {self.direction.value}"

    def to_metricflow_token(self) -> str:
        # MetricFlow CLI expects +/- prefix, not ASC/DESC
        sign = "+" if self.direction == OrderDirection.ASC else "-"
        return f"{sign}{self.name}"

    @classmethod
    def parse(cls, raw: str, *, metrics: list[str], dimensions: dict[str, dict]) -> "OrderByField":
        s = (raw or "").strip()

        # detect +/- shorthand
        direction = None
        if s.startswith("+"):
            direction = OrderDirection.ASC
            s = s[1:].strip()
        elif s.startswith("-"):
            direction = OrderDirection.DESC
            s = s[1:].strip()

        # detect explicit ASC/DESC suffix
        parts = s.split()
        if len(parts) >= 2 and parts[-1].upper() in ("ASC", "DESC"):
            direction = OrderDirection(parts[-1].upper())
            name = " ".join(parts[:-1]).strip()
        else:
            name = s

        # default direction if none specified
        direction = direction or OrderDirection.DESC

        # classify target
        if name in metrics:
            tgt = OrderTarget.METRIC
        elif name in dimensions:  # includes metric_time__week/month etc. if present in dimensions map
            tgt = OrderTarget.DIMENSION
        else:
            # best effort: if it looks like time grain or contains '__', treat as dimension
            tgt = OrderTarget.DIMENSION if "__" in name or name.startswith("metric_time") else OrderTarget.METRIC

        return cls(target=tgt, name=name, direction=direction)


class OrderSpec(BaseModel):
    """Base class for structured order specs (not required by the CLI path, kept for extensibility)."""
    direction: OrderDirection = OrderDirection.DESC

    def to_expression(self) -> str:
        raise NotImplementedError


class MetricOrderBy(OrderSpec):
    kind: Literal["metric"] = "metric"
    name: str

    def to_expression(self) -> str:
        return f"{self.name} {self.direction.value}"


class DimensionOrderBy(OrderSpec):
    kind: Literal["dimension"] = "dimension"
    dimension: str

    def to_expression(self) -> str:
        return f"{self.dimension} {self.direction.value}"


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
        time_grains = ["metric_time__day", "metric_time__week", "metric_time__month", "metric_time__quarter", "metric_time__year"]
        return dim in time_grains


class Condition(BaseModel):
    """Represents a single condition expression, e.g., \"metric_time__week <= current_date\"."""
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
    # accept either structured OrderByField or raw strings (back-compat)
    order_by: Optional[List[Union[OrderByField, str]]] = None
    limit: Optional[int] = 5
    where: Optional[WhereCondition] = None

    @property
    def group_by_expressions(self) -> List[str]:
        return [g.to_expression() for g in self.group_by or []]

    @property
    def where_clause(self) -> Optional[str]:
        return self.where.to_where_clause() if self.where else None

    def order_by_tokens(
        self,
        *,
        known_metrics: List[str],
        known_dimensions: Dict[str, dict],
    ) -> List[str]:
        """
        Normalize order_by to MetricFlow tokens: ['+token_day__coin_name', '-average_price_usd'].
        Accepts raw strings ('-metric', 'dimension ASC') or OrderByField objects.
        """
        specs: List[OrderByField] = []
        for ob in self.order_by or []:
            if isinstance(ob, OrderByField):
                specs.append(ob)
            else:
                specs.append(OrderByField.parse(ob, metrics=known_metrics, dimensions=known_dimensions))
        return [spec.to_metricflow_token() for spec in specs]


class CreateQueryResponse(BaseModel):
    status: str = Field(..., description="Status of the query creation (CREATED or ERROR).")
    query: dict = Field(..., description="The validated query structure.")
    error: Optional[str] = Field(default=None, description="Error message if status=ERROR.")


class FetchResultsResponse(BaseModel):
    status: str = Field(..., description="Status of query execution (SUCCESSFUL or ERROR).")
    results: str = Field(..., description="Formatted query results.")
    error: Optional[str] = Field(default=None, description="Error message if status=ERROR.")


# ---------- DBT client ----------
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

    # --------------------- Cache Management ---------------------
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

    # --------------------- Metrics/Dimensions Fetching ---------------------
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

    # --------------------- Dimension Values (Cached + Filtered) ---------------------
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

    # --------------------- Public API ---------------------
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
        Validate metrics, group_by, where, and order_by using metrics+dimensions cache.
        Normalizes order_by to MetricFlow tokens: ['+dimension', '-metric'].
        Enforces: any ORDER BY dimension must appear in GROUP BY.
        """
        if self._metrics_cache is None:
            self._try_load_metrics_from_file()

        # --- Known catalog ---
        known_metrics = [m["name"] for m in self._metrics_cache.get("metrics", [])]
        known_dimensions: Dict[str, dict] = self._metrics_cache.get("dimensions", {})

        # --- Validate metrics ---
        for m in query_params.metrics:
            if m not in known_metrics:
                return CreateQueryResponse(
                    status="ERROR",
                    query=query_params.dict(),
                    error=f"Metric '{m}' is not defined."
                )

        # --- Validate group_by dimensions exist ---
        for gb in (query_params.group_by or []):
            if gb.dimension not in known_dimensions:
                return CreateQueryResponse(
                    status="ERROR",
                    query=query_params.dict(),
                    error=f"Dimension '{gb.dimension}' is not available."
                )

        # --- Validate WHERE clauses (dimension existence + optional value hinting) ---
        if query_params.where:
            for cond in query_params.where.conditions:
                if isinstance(cond, FilterField):
                    if cond.dimension not in known_dimensions:
                        return CreateQueryResponse(
                            status="ERROR",
                            query=query_params.dict(),
                            error=f"Dimension '{cond.dimension}' is not available."
                        )
                    candidates = self.fetch_dimension_values_filtered(cond.dimension, cond.value, max_results=20)
                    if candidates and cond.value not in candidates:
                        return CreateQueryResponse(
                            status="ERROR",
                            query=query_params.dict(),
                            error=(
                                f"Invalid value '{cond.value}' for dimension '{cond.dimension}'. "
                                f"Did you mean: {candidates[:5]} ?"
                            )
                        )

        # --- Normalize and validate ORDER BY ---
        normalized_order_tokens: List[str] = []
        try:
            specs: List[OrderByField] = []
            for ob in (query_params.order_by or []):
                specs.append(
                    ob if isinstance(ob, OrderByField)
                    else OrderByField.parse(ob, metrics=known_metrics, dimensions=known_dimensions)
                )

            # Enforce: ORDER BY dimensions must be ⊆ GROUP BY
            group_set = set(query_params.group_by_expressions)  # resolved names incl. time grains
            missing_dims = [
                s.name for s in specs
                if s.target == OrderTarget.DIMENSION and s.name not in group_set
            ]
            if missing_dims:
                return CreateQueryResponse(
                    status="ERROR",
                    query=query_params.dict(),
                    error=(
                        f"ORDER BY dimension(s) {missing_dims} must appear in GROUP BY. "
                        f"Current GROUP BY: {sorted(group_set) or '[]'}"
                    )
                )

            normalized_order_tokens = [s.to_metricflow_token() for s in specs]
        except Exception as e:
            return CreateQueryResponse(
                status="ERROR",
                query=query_params.dict(),
                error=f"Invalid order_by: {e}"
            )

        # --- Success: return normalized query dict ---
        query_dict = query_params.dict()
        if normalized_order_tokens:
            query_dict["order_by"] = normalized_order_tokens  # e.g. ['+metric_time__week', '-average_price_usd']

        return CreateQueryResponse(status="CREATED", query=query_dict)

    # --------------------- MetricFlow execution ---------------------
    def parse_metricflow_table(self, raw_output: str) -> list[dict]:
        text = ANSI_RE.sub("", raw_output or "")

        clean = []
        for line in text.splitlines():
            s = line.rstrip()
            if not s:
                continue
            if s.lstrip().startswith(SPINNER_PREFIXES):
                continue
            if any(s.startswith(prefix) for prefix in NOISE_STARTS):
                continue
            if re.match(r"^\[\d{2}:\d{2}:\d{2}\]", s.strip()):
                continue
            clean.append(s)

        if not clean:
            return []

        # find header followed by dashed ruler
        header_idx = None
        for i in range(len(clean) - 1):
            if DASH_LINE_RE.match(clean[i + 1].strip()):
                header_idx = i
                break
        if header_idx is None:
            header_idx = 0

        header_cols = COL_SPLIT_RE.split(clean[header_idx].strip())
        data_start = header_idx + 2 if header_idx + 1 < len(clean) and DASH_LINE_RE.match(clean[header_idx + 1].strip()) else header_idx + 1

        rows = []
        for line in clean[data_start:]:
            if DASH_LINE_RE.match(line.strip()):
                continue
            cols = COL_SPLIT_RE.split(line.strip())
            if not cols:
                continue
            if len(cols) < len(header_cols):
                cols += [""] * (len(header_cols) - len(cols))
            elif len(cols) > len(header_cols):
                cols = cols[:len(header_cols)]
            rows.append(dict(zip(header_cols, cols)))

        return rows

    def run_query_from_dict(self, query_dict: CreateQueryInput) -> FetchResultsResponse:
        """
        Executes a MetricFlow query using the given CreateQueryInput.
        Returns a FetchResultsResponse with status, formatted results, and optional error.
        """
        metrics_list = query_dict.metrics
        group_bys = query_dict.group_by_expressions
        limit_value = query_dict.limit
        where_clause = query_dict.where_clause

        if not metrics_list:
            return FetchResultsResponse(
                status="ERROR",
                results="No results due to missing metrics.",
                error="No metrics provided."
            )

        # Normalize order tokens + enforce subset rule again (in case called directly)
        order_tokens: List[str] = []
        try:
            cache_metrics = [m["name"] for m in self._metrics_cache.get("metrics", [])]
            cache_dims: Dict[str, dict] = self._metrics_cache.get("dimensions", {})

            specs: List[OrderByField] = []
            for ob in (query_dict.order_by or []):
                specs.append(
                    ob if isinstance(ob, OrderByField)
                    else OrderByField.parse(ob, metrics=cache_metrics, dimensions=cache_dims)
                )

            gb_set = set(group_bys)
            missing = [s.name for s in specs if s.target == OrderTarget.DIMENSION and s.name not in gb_set]
            if missing:
                return FetchResultsResponse(
                    status="ERROR",
                    results="No results due to invalid ORDER BY.",
                    error=(f"ORDER BY dimension(s) {missing} must appear in GROUP BY. "
                           f"Current GROUP BY: {sorted(gb_set) or '[]'}")
                )

            order_tokens = [s.to_metricflow_token() for s in specs]   # '+/- field' as MetricFlow expects
        except Exception as e:
            return FetchResultsResponse(
                status="ERROR",
                results="No results due to invalid ORDER BY.",
                error=str(e)
            )

        # Build CLI
        command = ["mf", "query", "--metrics", ",".join(metrics_list)]
        if group_bys:
            command.extend(["--group-by", ",".join(group_bys)])
        if order_tokens:
            command.extend(["--order", ",".join(order_tokens)])  # tokens, not ASC/DESC
        if where_clause:
            command.extend(["--where", where_clause])
        if limit_value is not None:
            command.extend(["--limit", str(limit_value)])

        logging.info(f"Running MetricFlow query: {' '.join(command)}")

        # Execute
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
                    results=f"No results due to query failure. Query is {command}",
                    error=f"MetricFlow failed with code {result.returncode}: {result.stderr}, logs: {result.stdout}"
                )

            parsed_rows = self.parse_metricflow_table(result.stdout)

            if not parsed_rows:
                return FetchResultsResponse(
                    status="SUCCESSFUL",
                    results=f"No rows returned by the query. logs: {result.stdout}",
                    error=None
                )

            # Format results as Markdown table
            header = list(parsed_rows[0].keys())
            table = "| " + " | ".join(header) + " |\n"
            table += "| " + " | ".join("---" for _ in header) + " |\n"
            for row in parsed_rows[:10]:
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
