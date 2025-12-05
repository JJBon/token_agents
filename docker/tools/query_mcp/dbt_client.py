# tools/query_mcp/dbt_client.py

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
from datetime import datetime, date, timedelta


# ---------- Output cleaning helpers ----------
ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")  # strip ANSI
SPINNER_PREFIXES = ("⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏")
NOISE_STARTS = (
    "‼️ Warning:", "💡 Please update", "✔ Success", "Initiating query", "written query",
)
DASH_LINE_RE = re.compile(r"^\s*-{2,}(?:\s+-{2,})+\s*$")  # lines of ----  ---- ...
COL_SPLIT_RE = re.compile(r"\s{2,}")  # split on 2+ spaces

logger = logging.getLogger(__name__)

NUMERIC_COMPARATORS = {"<", "<=", ">", ">="}

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?(Z)?$")

_RELATIVE_DAY_RE = re.compile(r"^today([+-])(\d+)[dD]$")  # e.g. today-7d

def _manifest_time_base_names(manifest: dict) -> set[str]:
    """Return base names (e.g., 'inserted_at') of dimensions that are type: time."""
    bases = set()
    for sm in (manifest.get("semantic_models") or {}).values():
        for d in (sm.get("dimensions") or []):
            try:
                # dbt semantic model format: each d has 'name' and 'type'
                if (d.get("type") or "").lower() == "time":
                    bases.add(d["name"])
            except Exception:
                continue
    return bases

def _is_fully_qualified_time_dim(dim_fq: str, time_bases: set[str]) -> bool:
    """
    A fully qualified dim (like 'token_day__inserted_at') is time if it ends with '__<time_base>'.
    Also treat 'metric_time' as time.
    """
    if dim_fq == "metric_time" or dim_fq.startswith("metric_time__"):
        return True
    for base in time_bases:
        if dim_fq.endswith(f"__{base}"):
            return True
    return False

def _iso_utc_today() -> str:
    return date.today().isoformat()  # keep simple (no timezone ambiguity for dates)

def _resolve_time_token(value: str) -> str:
    """Return an ISO date/datetime string if value is a known relative token, else the original."""
    if not isinstance(value, str):
        return value

    v = value.strip().lower()
    # common patterns
    if v in ("{{ today() }}", "today"):
        return _iso_utc_today()
    if v in ("{{ now() }}", "now"):
        # Keep ISO datetime; most metric_time filters are date, but safe to return full ISO.
        return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    if v == "yesterday":
        return (date.today() - timedelta(days=1)).isoformat()

    m = _RELATIVE_DAY_RE.match(v)
    if m:
        sign, num = m.groups()
        delta = int(num)
        if sign == "-":
            return (date.today() - timedelta(days=delta)).isoformat()
        else:
            return (date.today() + timedelta(days=delta)).isoformat()

    # unchanged (user supplied a literal already)
    return value

def _quote_if_time_literal(s: str) -> str:
    """Quote date/datetime literals for MetricFlow where clauses."""
    if isinstance(s, str):
        if _DATE_RE.match(s) or _DATETIME_RE.match(s):
            return f"'{s}'"
    return s

def _looks_numeric_literal(s: str) -> bool:
    if s is None:
        return False
    s = str(s).strip()
    # Accept ints, floats, scientific notation
    return bool(re.fullmatch(r"[+-]?\d+(\.\d+)?([eE][+-]?\d+)?", s))

def _limit_10(n: Optional[int]) -> int:
    try:
        n = int(n or 10)
    except Exception:
        n = 10
    return max(1, min(n, 10))



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
        if self.type == DimensionType.TIME:
            grain = (self.aggregation.value if self.aggregation else "day")
            dim_expr = f"{{{{ TimeDimension('{self.dimension}','{grain}') }}}}"
            resolved = _resolve_time_token(str(self.value))
            literal = _quote_if_time_literal(resolved)
            return f"{dim_expr} {self.operator} {literal}"

        elif self.type == DimensionType.DIMENSION:
            dim_expr = (
                f"{{{{ Dimension('{self.dimension}') }}}}"
                if not self.dimension.startswith("{{ Dimension(")
                else self.dimension
            )
            return f"{dim_expr} {self.operator} '{self.value}'"

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
        Normalize order_by to MetricFlow tokens: ['+token_day__coin_name', '-averaDBTCoreClientge_price_usd'].
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

    def _subprocess_env(self, extra_env: Optional[dict]) -> dict:
        env = os.environ.copy()
        if extra_env:
            env.update(extra_env)
        return env

    def _normalize_query_in_place(self, q: CreateQueryInput) -> None:
        if self._metrics_cache is None:
            self._try_load_metrics_from_file()
        known_dims: Dict[str, dict] = (self._metrics_cache or {}).get("dimensions", {})
        if not q.where:
            return
        for cond in q.where.conditions:
            if not isinstance(cond, FilterField):
                continue
            meta = known_dims.get(cond.dimension, {})
            cache_type = meta.get("type")  # "time" | "dimension"
            if cache_type == "time":
                cond.type = DimensionType.TIME
                if cond.aggregation is None:
                    cond.aggregation = TimeAggregation.day   # or week if you prefer
            elif cache_type == "dimension":
                cond.type = DimensionType.DIMENSION

    def _ordinal_index_dim_for(self, dim_name: str) -> Optional[str]:
        """
        If you follow a naming convention like '<dim>_index' for numeric ordering,
        suggest it in error messages.
        """
        candidate = f"{dim_name}_index"
        if self._metrics_cache and candidate in self._metrics_cache.get("dimensions", {}):
            return candidate
        return None

    def _load_ordinal_hints_from_manifest(self, manifest: dict) -> None:
        """Scan manifest for meta.ordinal on dimensions/columns."""
        ordinals = set()
        ordering: dict[str, list[str]] = {}

        # 1) Semantic Models (dbt Semantic Layer / MetricFlow)
        for sm in manifest.get("semantic_models", {}).values():
            for dim in sm.get("dimensions", []):
                meta = (dim.get("config") or {})
                meta = (meta.get('meta') or {})
                if meta.get("ordinal") is True:
                    name = dim.get("name")
                    if name:
                        ordinals.add(name)
                        if isinstance(meta.get("order"), list):
                            ordering[name] = meta["order"]

        # 2) Models -> columns meta (optional, if you use column meta instead)
        for model in manifest.get("nodes", {}).values():
            if model.get("resource_type") == "model":
                for col in (model.get("columns") or {}).values():
                    meta = (col.get("meta") or {})
                    if meta.get("ordinal") is True:
                        name = col.get("name")
                        if name:
                            ordinals.add(name)
                            if isinstance(meta.get("order"), list):
                                ordering[name] = meta["order"]

        self._ordinal_dims = ordinals
        self._ordinal_dim_order = ordering

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

    def _anchor_metric_for_dim(self, dimension: str) -> Optional[str]:
        anchors = (self._metrics_cache or {}).get("dim_anchor_metric", {})
        m = anchors.get(dimension)
        if m:
            return m
        # fallback: first metric that lists this dimension
        d2m = (self._metrics_cache or {}).get("dim_to_metrics", {})
        ms = d2m.get(dimension) or []
        return ms[0] if ms else None

    # --------------------- Metrics/Dimensions Fetching ---------------------
    def _build_metrics_cache(self, env: Optional[dict] = None):
        logging.info("Building metrics cache...")
        metrics_from_ls = self._get_all_metrics_info(env=env)

        manifest_data = {}
        if os.path.exists(self.manifest_path):
            with open(self.manifest_path, "r") as f:
                manifest_data = json.load(f)

        time_bases = _manifest_time_base_names(manifest_data)

        manifest_metrics = manifest_data.get("metrics", {})
        dimensions_map = {}
        metrics_list = []

        for uid, info in metrics_from_ls.items():
            metric_name = info.get("name", "unknown_metric")
            manifest_def = manifest_metrics.get(uid, {})
            description = manifest_def.get("description", info.get("description", ""))

            dims_for_metric = self._fetch_dimensions_for_metric(metric_name)

            metrics_list.append({
                "name": metric_name,
                "description": description,
                "dimensions": dims_for_metric
            })

            for dim in dims_for_metric:
                if dim not in dimensions_map:
                    dim_type = "time" if _is_fully_qualified_time_dim(dim, time_bases) else "dimension"
                    dimensions_map[dim] = {"type": dim_type, "values": []}

        # Ensure 'metric_time' exists and is time
        if "metric_time" not in dimensions_map:
            dimensions_map["metric_time"] = {"type": "time", "values": []}

        self._metrics_cache = {"metrics": metrics_list, "dimensions": dimensions_map}
        self._write_metrics_to_file()


    def _get_all_metrics_info(self, env: Optional[dict] = None):
        command = ["dbt", "ls", "--resource-type", "metric", "--output", "json", "--quiet"]
        result = subprocess.run(command, cwd=self.project_dir, capture_output=True, text=True, check=True,env=self._subprocess_env(env))
        lines = result.stdout.strip().split("\n")
        return {json.loads(line)["unique_id"]: json.loads(line) for line in lines if line.strip()}

    def _fetch_dimensions_for_metric(self, metric_name: str, env: Optional[dict] = None) -> List[str]:
        command = ["mf", "list", "dimensions", "--metrics", metric_name]
        result = subprocess.run(command, cwd=self.project_dir, capture_output=True, text=True, check=False, env=self._subprocess_env(env))
        if result.returncode != 0:
            logging.warning(f"MetricFlow failed for {metric_name}: {result.stderr}")
            return []
        return [line.replace("• ", "").strip() for line in result.stdout.splitlines() if line.startswith("• ")]

    # --------------------- Dimension Values (Cached + Filtered) ---------------------
    def _build_dimension_values_cache(self, env: Optional[dict] = None):
        logging.info("Building dimension values cache...")
        if not self._metrics_cache:
            self._build_metrics_cache(env=env)

        dimension_values = {}
        first_metric = self._metrics_cache["metrics"][0]["name"] if self._metrics_cache["metrics"] else None

        for dim, meta in self._metrics_cache.get("dimensions", {}).items():
            # Try to fetch values for both time and categorical. MetricFlow supports it.
            vals = self._fetch_dimension_values(first_metric, dim, env=env)
            # Keep the cache reasonably small; you can tune this cap.
            dimension_values[dim] = vals[:200]

        with open(self.dimension_values_file, "w") as f:
            json.dump(dimension_values, f, indent=2)
        logging.info(f"Dimension values cached to {self.dimension_values_file}")

    def _fetch_dimension_values(self, metric_name: str, dimension: str,  env: Optional[dict] = None) -> List[str]:
        cmd = ["mf", "list", "dimension-values", "--metrics", metric_name, "--dimension", dimension]
        res = subprocess.run(cmd, cwd=self.project_dir, capture_output=True, text=True, check=False, env=self._subprocess_env(env))
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
    
    def _fallback_dimension_values(self, dimension: str, max_results: int) -> List[str]:
        """Best-effort: read cache and return the first N values for a dimension."""
        try:
            if not os.path.exists(self.dimension_values_file):
                # try to (re)build and read again
                self._build_dimension_values_cache()
            with open(self.dimension_values_file, "r") as f:
                cache = json.load(f)
            return (cache.get(dimension) or [])[:max_results]
        except Exception:
            logging.exception("fallback_dimension_values failed")
            return []

    def fetch_dimension_values_filtered(
    self,
    dimension: str,
    query: Optional[str],
    max_results: int = 10,
) -> List[str]:
        """
        Fetch up to max_results matching values for a dimension from cached data.
        - If query is empty/None → return first N cached values for this dimension.
        - If query looks numeric → return [] (don’t fuzzy search numbers).
        - On any error → return first N cached values.
        """
        n = _limit_10(max_results)
        try:
            # empty / null query → list candidates from cache (no live call)
            if not query or str(query).strip() == "":
                return self._fallback_dimension_values(dimension, n)

            # short-circuit numeric-like strings
            if _looks_numeric_literal(query):
                return []

            if not os.path.exists(self.dimension_values_file):
                logging.warning("Dimension cache not found. Rebuilding...")
                self._build_dimension_values_cache()

            with open(self.dimension_values_file, "r") as f:
                dimension_cache = json.load(f)

            all_values = dimension_cache.get(dimension, [])
            return difflib.get_close_matches(query, all_values, n=n, cutoff=0.3)

        except Exception:
            logging.exception("fetch_dimension_values_filtered failed; falling back to cached head")
            return self._fallback_dimension_values(dimension, n)

    def search_dimension_values(
    self,
    dimension: str,
    query: Optional[str],
    max_results: int = 10,
    metric: Optional[str] = None
) -> dict:
        """
        If query is empty → return up to 10 values from cache (no live MetricFlow call).
        Otherwise → fuzzy match over cached values (also capped at 10).
        """
        n = _limit_10(max_results)
        try:
            anchor = metric or self._anchor_metric_for_dim(dimension)

            # Empty query: prefer cache so the bot can suggest valid values quickly.
            if not query or str(query).strip() == "":
                vals = self._fallback_dimension_values(dimension, n)
                return {
                    "dimension": dimension,
                    "query": query,
                    "matches": vals,
                    "metric": anchor,
                    "status": "OK" if vals else "EMPTY",
                    "hint": None if vals else (
                        f"No cached values for '{dimension}'. "
                        f"Run refreshMetrics() or specify metric= that exposes this dimension."
                    ),
                }

            # Numeric-like → don't fuzzy-search labels
            if _looks_numeric_literal(query):
                return {
                    "dimension": dimension,
                    "query": query,
                    "matches": [],
                    "metric": anchor,
                    "status": "OK",
                    "hint": None,
                }

            # Ensure cache exists before fuzzy
            if not os.path.exists(self.dimension_values_file):
                self._build_dimension_values_cache()

            matches = self.fetch_dimension_values_filtered(dimension, query, n)
            return {
                "dimension": dimension,
                "query": query,
                "matches": matches,
                "metric": anchor,
                "status": "OK",
                "hint": None,
            }

        except Exception as e:
            return {
                "dimension": dimension,
                "query": query,
                "matches": self._fallback_dimension_values(dimension, n),
                "metric": metric or self._anchor_metric_for_dim(dimension),
                "status": "ERROR",
                "error": str(e),
            }

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
        if self._metrics_cache is None:
            self._try_load_metrics_from_file()

        known_metrics = [m["name"] for m in self._metrics_cache.get("metrics", [])]
        known_dimensions: Dict[str, dict] = self._metrics_cache.get("dimensions", {})

        # Existence checks first
        for gb in (query_params.group_by or []):
            if gb.dimension not in known_dimensions:
                return CreateQueryResponse(status="ERROR", query=query_params.dict(),
                                        error=f"Dimension '{gb.dimension}' is not available.")
        for m in query_params.metrics:
            if m not in known_metrics:
                return CreateQueryResponse(status="ERROR", query=query_params.dict(),
                                        error=f"Metric '{m}' is not defined.")

        # ✅ Normalize the right object
        self._normalize_query_in_place(query_params)

        # WHERE validation, operator sanity, label hinting
        if query_params.where:
            for cond in query_params.where.conditions:
                if not isinstance(cond, FilterField):
                    continue

                if cond.dimension not in known_dimensions:
                    return CreateQueryResponse(status="ERROR", query=query_params.dict(),
                                            error=f"Dimension '{cond.dimension}' is not available.")

                if cond.type == DimensionType.DIMENSION and cond.operator in NUMERIC_COMPARATORS:
                    idx_hint = self._ordinal_index_dim_for(cond.dimension) or ""
                    hint = f" Use '{idx_hint}' for numeric comparisons." if idx_hint else ""
                    return CreateQueryResponse(
                        status="ERROR",
                        query=query_params.dict(),
                        error=(f"Numeric comparator '{cond.operator}' is invalid for categorical "
                            f"dimension '{cond.dimension}'.{hint} Use '=' or 'IN (...)'.")
                    )

                if cond.type == DimensionType.DIMENSION and cond.operator not in NUMERIC_COMPARATORS:
                    candidates = self.fetch_dimension_values_filtered(cond.dimension, cond.value, max_results=20)
                    if candidates and cond.value not in candidates and not _looks_numeric_literal(cond.value):
                        return CreateQueryResponse(
                            status="ERROR",
                            query=query_params.dict(),
                            error=(f"Invalid value '{cond.value}' for dimension '{cond.dimension}'. "
                                f"Did you mean: {candidates[:5]} ?")
                        )

        # ORDER BY normalization + GROUP BY subset rule
        normalized_order_tokens: List[str] = []
        try:
            specs: List[OrderByField] = []
            for ob in (query_params.order_by or []):
                specs.append(
                    ob if isinstance(ob, OrderByField)
                    else OrderByField.parse(ob, metrics=known_metrics, dimensions=known_dimensions)
                )
            group_set = set(query_params.group_by_expressions)
            missing_dims = [s.name for s in specs if s.target == OrderTarget.DIMENSION and s.name not in group_set]
            if missing_dims:
                return CreateQueryResponse(
                    status="ERROR",
                    query=query_params.dict(),
                    error=(f"ORDER BY dimension(s) {missing_dims} must appear in GROUP BY. "
                        f"Current GROUP BY: {sorted(group_set) or '[]'}")
                )
            normalized_order_tokens = [s.to_metricflow_token() for s in specs]
        except Exception as e:
            return CreateQueryResponse(status="ERROR", query=query_params.dict(), error=f"Invalid order_by: {e}")

        # Return the normalized query
        query_dict = query_params.dict()
        if normalized_order_tokens:
            query_dict["order_by"] = normalized_order_tokens
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

    def run_query_from_dict(self, query_dict: CreateQueryInput, env: Optional[dict] = None) -> FetchResultsResponse:
        """
        Executes a MetricFlow query using the given CreateQueryInput.
        Returns a FetchResultsResponse with status, formatted results, and optional error.
        """
        self._normalize_query_in_place(query_dict)
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
                check=False,
                env = self._subprocess_env(env) 
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
