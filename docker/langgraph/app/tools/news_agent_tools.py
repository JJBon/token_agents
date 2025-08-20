# tools_sql.py
import json, time
import boto3
from typing import Dict, List
from langchain_core.tools import tool
from schemas import NormalizeRequest, NormalizeResponse, NormalizedMention
import os

athena = boto3.client("athena", region_name="us-east-1")
GLUE_DB = "news_agent"
DIM_TBL = "dim_assets"
ATHENA_OUT = os.environ["ATHENA_OUTPUT_S3"]

def _athena(sql: str) -> List[Dict[str,str]]:
    q = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": GLUE_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUT},
        WorkGroup="primary",
    )
    qid = q["QueryExecutionId"]
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st in ("FAILED","CANCELLED"):
            raise RuntimeError(f"Athena failed for SQL:\n{sql}")
        if st == "SUCCEEDED": break
        time.sleep(0.5)
    res = athena.get_query_results(QueryExecutionId=qid)
    rows = res.get("ResultSet", {}).get("Rows", [])
    if not rows: return []
    headers = [c["VarCharValue"] for c in rows[0]["Data"]]
    out = []
    for r in rows[1:]:
        d = {}
        for i, c in enumerate(r["Data"]):
            v = c.get("VarCharValue")
            d[headers[i]] = v
        out.append(d)
    return out

def _sql_escape(s: str) -> str:
    return s.replace("'", "''")

@tool("normalize_mentions", args_schema=NormalizeRequest, return_direct=False)
def normalize_mentions(req: NormalizeRequest) -> NormalizeResponse:
    """
    Map (name/symbol) mentions to canonical (symbol,name) using dim_assets in Glue/Athena.
    Fails closed: only returns entries that matched the catalog.
    """
    symbols = [m.symbol.strip().upper() for m in req.mentions if m.symbol]
    names   = [m.name.strip().lower() for m in req.mentions if m.name]

    clauses = []
    if symbols:
        in_syms = ",".join(f"'{_sql_escape(s)}'" for s in symbols)
        clauses.append(f"symbol IN ({in_syms})")
    if names:
        ors = " OR ".join(
            [
                f"lower(name) = '{_sql_escape(n)}'",
                f"'{_sql_escape(n)}' = ANY (aliases)"
            ]
            for n in names
        )  # expanded below to valid SQL
        # Trino/Presto doesn't allow row-wise OR generation like that; build a union of predicates properly:
        name_preds = []
        for n in names:
            name_preds.append(f"lower(name) = '{_sql_escape(n)}'")
            name_preds.append(f"contains(aliases, '{_sql_escape(n)}')")
        clauses.append("(" + " OR ".join(name_preds) + ")")

    where = " OR ".join(clauses) if clauses else "false"

    sql = f"""
    SELECT lower(name) AS name, upper(symbol) AS symbol
    FROM {GLUE_DB}.{DIM_TBL}
    WHERE is_active = true AND ({where})
    """
    rows = _athena(sql)

    # consolidate by symbol (prefer exact symbol matches)
    seen = set()
    out: List[NormalizedMention] = []
    for r in rows:
        key = r["symbol"]
        if key in seen: 
            continue
        seen.add(key)
        # simple confidence rule: 0.9 if symbol matched, else 0.7
        conf = 0.9 if r["symbol"] in symbols else 0.7
        out.append(NormalizedMention(name=r["name"], symbol=r["symbol"], confidence=conf))
    return NormalizeResponse(mentions=out)
