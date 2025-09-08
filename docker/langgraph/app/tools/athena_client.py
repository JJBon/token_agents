# athena_client.py
import os
import time
import boto3
from botocore.exceptions import ClientError

# --- Config (single source of truth from env) -------------------------------
AWS_REGION       = os.environ.get("AWS_REGION", "us-east-1")
GLUE_DATABASE    = os.environ.get("GLUE_DATABASE", "news_agent")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
ATHENA_CATALOG   = os.environ.get("ATHENA_CATALOG", "AwsDataCatalog")

# Optional when workgroup uses Managed Query Results
ATHENA_OUT = os.environ.get("ATHENA_OUTPUT_S3")

# "auto" (default) will introspect WG; set "true"/"false" to force
ATHENA_USE_MANAGED_RESULTS = os.environ.get("ATHENA_USE_MANAGED_RESULTS", "auto").lower()

athena = boto3.client("athena", region_name=AWS_REGION)
_WG_CACHE = None


# --- Helpers ----------------------------------------------------------------
def _wait(qid: str) -> str:
    while True:
        r = athena.get_query_execution(QueryExecutionId=qid)
        s = r["QueryExecution"]["Status"]["State"]
        if s in ("FAILED", "CANCELLED"):
            reason = r["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena FAILED: {reason}")
        if s == "SUCCEEDED":
            return qid
        time.sleep(0.5)


def _workgroup_uses_managed_results() -> bool:
    global _WG_CACHE
    if _WG_CACHE is None:
        _WG_CACHE = athena.get_work_group(WorkGroup=ATHENA_WORKGROUP)
    cfg = _WG_CACHE["WorkGroup"]["Configuration"]
    return "ManagedQueryResultsConfiguration" in cfg

def _should_use_managed_results() -> bool:
    if ATHENA_USE_MANAGED_RESULTS in ("true", "1", "yes"):
        return True
    if ATHENA_USE_MANAGED_RESULTS in ("false", "0", "no"):
        return False
    try:
        return _workgroup_uses_managed_results()
    except Exception:
        return False

def _start_query(args: dict) -> str:
    """Start query with nice error messages."""
    try:
        print(args)
        q = athena.start_query_execution(**args)
    except ClientError as e:
        msg = str(e)
        if "ManagedQueryResultsConfiguration and ResultConfiguration cannot be set together" in msg:
            raise RuntimeError(
                "WorkGroup has Managed Query Results enabled, but ResultConfiguration was provided. "
                "Unset ATHENA_OUTPUT_S3 or set ATHENA_USE_MANAGED_RESULTS=true."
            ) from e
        if "Queries of this type are not supported" in msg:
            wg = athena.get_work_group(WorkGroup=ATHENA_WORKGROUP)
            eng = wg["WorkGroup"]["Configuration"].get("EngineVersion", {})
            raise RuntimeError(
                "Athena says 'Queries of this type are not supported'. Ensure Engine v3 "
                "and Catalog='AwsDataCatalog'. "
                f"Selected={eng.get('SelectedEngineVersion')}, Effective={eng.get('EffectiveEngineVersion')}."
            ) from e
        raise
    return _wait(q["QueryExecutionId"])

# --- Public API --------------------------------------------------------------
def query(sql: str, database: str | None = None) -> str:
    """Run SQL in a database context (use for table queries). Returns QueryExecutionId."""
    args = {
        "QueryString": sql,
        "QueryExecutionContext": {
            "Database": database or GLUE_DATABASE,
            "Catalog": ATHENA_CATALOG,
        },
        "WorkGroup": ATHENA_WORKGROUP,
    }
    if not _should_use_managed_results():
        if not ATHENA_OUT:
            raise RuntimeError(
                "ATHENA_OUTPUT_S3 is not set and WorkGroup does not appear to use Managed Query Results. "
                "Set ATHENA_OUTPUT_S3 or set ATHENA_USE_MANAGED_RESULTS=true."
            )
        args["ResultConfiguration"] = {"OutputLocation": ATHENA_OUT}
    return _start_query(args)


def query_global(sql: str) -> str:
    """Run SQL without binding to a specific database (use for CREATE DATABASE / information_schema)."""
    args = {
        "QueryString": sql,
        "QueryExecutionContext": {
            "Catalog": ATHENA_CATALOG,  # no Database on purpose
        },
        "WorkGroup": ATHENA_WORKGROUP,
    }
    if not _should_use_managed_results():
        if not ATHENA_OUT:
            raise RuntimeError(
                "ATHENA_OUTPUT_S3 is not set and WorkGroup does not appear to use Managed Query Results. "
                "Set ATHENA_OUTPUT_S3 or set ATHENA_USE_MANAGED_RESULTS=true."
            )
        args["ResultConfiguration"] = {"OutputLocation": ATHENA_OUT}
    return _start_query(args)

def rows(qid: str) -> list[list[str]]:
    res = athena.get_query_results(QueryExecutionId=qid)
    rs = res.get("ResultSet", {}).get("Rows", [])
    if not rs:
        return []
    return [[c.get("VarCharValue") for c in row["Data"]] for row in rs[1:]]

def table_exists(schema: str, table: str) -> bool:
    q = query_global(f"""
        SELECT 1
        FROM information_schema.tables
        WHERE lower(table_schema) = lower('{schema}')
          AND lower(table_name)   = lower('{table}')
        LIMIT 1
    """)
    return bool(rows(q))

def column_exists(schema: str, table: str, column: str) -> bool:
    q = query_global(f"""
        SELECT 1
        FROM information_schema.columns
        WHERE lower(table_schema) = lower('{schema}')
          AND lower(table_name)   = lower('{table}')
          AND lower(column_name)  = lower('{column}')
        LIMIT 1
    """)
    return bool(rows(q))
