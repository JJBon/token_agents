# news_ingest_graph.py
from __future__ import annotations
import os, json, asyncio, logging
from typing import Any, Dict, List, Annotated, TypedDict
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from tools.news_tools.models import TokenMention

from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langchain_core.runnables import RunnableLambda

log = logging.getLogger(__name__)

from tools.news_tools.tools import (
    ensure_iceberg_tables_tool,
    fetch_news_api_tool, dedupe_news_ids_tool,
    scrape_article_text_tool, llm_extract_mentions_tool,
    persist_bronze_tool, persist_iceberg_tool,
    merge_hints_and_tokens,
    # S3 sync mode (existing)
    kb_ingest_news_tool,
    # ✅ Direct ingest mode (new)
    kb_direct_ingest_news_tool,
    # kb_retrieve_news_tool,  # (optional in this flow)
)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
DEFAULT_INGEST_MODE = os.getenv("KB_INGEST_MODE", "s3").lower()  # "s3" | "direct"

async def _to_thread(func, /, *args, **kwargs):
    return await asyncio.to_thread(lambda: func(*args, **kwargs))

async def _call_llm_with_retry(payload: dict, attempts: int = 3, base_sleep: float = 0.5):
    last_exc = None
    for i in range(attempts):
        try:
            return await _to_thread(llm_extract_mentions_tool.invoke, payload)
        except Exception as e:
            last_exc = e
            log.warning("LLM extract failed (try %d/%d) for %s: %s",
                        i+1, attempts, (payload.get("url") or payload.get("title") or ""), e)
        await asyncio.sleep(base_sleep * (2 ** i))
    log.error("LLM extract ultimately failed for %s: %s",
              (payload.get("url") or payload.get("title") or ""), last_exc)
    return None

def _tokens_from_llm_out(llm_out) -> list[TokenMention]:
    if llm_out is None:
        return []
    tokens = getattr(llm_out, "tokens", None)
    if tokens is not None:
        return tokens or []
    if isinstance(llm_out, dict):
        raw = llm_out.get("tokens") or []
        out = []
        for t in raw:
            try:
                if isinstance(t, TokenMention):
                    out.append(t)
                elif isinstance(t, dict):
                    out.append(TokenMention(**t))
            except Exception:
                continue
        return out
    return []

class State(TypedDict):
    messages: Annotated[List[Any], add_messages]
    api_url: str
    max_articles: int
    timeout_s: int
    extractor_temperature: float

    # ingest controls
    ingest_mode: str                  # "s3" | "direct"
    wait_for_ingest: bool             # only relevant for S3 ingest

    items: List[Dict]
    to_process: List[Dict]
    enriched_new: List[Dict]
    enriched_all: List[Dict]

    dedup_skipped: int
    # S3 sync fields
    kb_uploaded: int
    kb_ingestion_job_id: str | None
    kb_ingestion_status: str | None
    # Direct ingest fields
    kb_direct_ok: int
    kb_direct_fail: int
    kb_direct_errors: List[Dict[str, str]]

    bronze_count: int
    iceberg_count: int

# ----- Nodes -----
def _ensure_tables(state: State):
    ensure_iceberg_tables_tool.invoke({})
    return {}

def _fetch_api(state: State):
    out = fetch_news_api_tool.invoke({"api_url": state["api_url"], "timeout_s": state["timeout_s"]})
    return {"items": out.items}

def _dedupe(state: State):
    out = dedupe_news_ids_tool.invoke({"items": state.get("items", [])})
    return {"to_process": out.to_process, "dedup_skipped": out.dedup_skipped}

async def _extract(state: State):
    to_process = state.get("to_process", [])[: state["max_articles"]]
    if not to_process:
        return {"enriched_new": []}

    sem = asyncio.Semaphore(int(os.getenv("EXTRACT_CONCURRENCY", "8")))

    async def process(item: Dict[str, Any]) -> Dict[str, Any]:
        async with sem:
            try:
                url = item.get("news_url") or ""
                text_out = await _to_thread(scrape_article_text_tool.invoke, {"url": url, "timeout_s": state["timeout_s"]})
                text = (getattr(text_out, "text", "") or "").strip()
                if not text:
                    text = f"{item.get('title','')}\n\n{item.get('text','')}".strip()

                llm_out = await _call_llm_with_retry({
                    "title":  item.get("title",""),
                    "source": item.get("source_name",""),
                    "url":    url,
                    "body":   text,
                })
                tokens = _tokens_from_llm_out(llm_out)
                currencies = merge_hints_and_tokens(text, tokens)

                out = {**item}
                out["full_text"] = text[:240_000]
                out["currencies"] = currencies
                out["_evidence"] = []
                return out

            except Exception as e:
                log.error("extract/process failed for %s: %s", item.get("news_url") or item.get("title") or "", e)
                return {**item, "full_text": "", "currencies": [], "_evidence": [], "_error": f"{type(e).__name__}: {e}"}

    results = await asyncio.gather(*(process(x) for x in to_process), return_exceptions=True)
    enriched_new: list[Dict[str, Any]] = []
    for r in results:
        if isinstance(r, Exception):
            log.error("process raised: %s", r)
            continue
        enriched_new.append(r)

    return {"enriched_new": enriched_new}

def _ingest(state: State):
    """
    Switchable ingest:
      - "s3": upload to S3 + start ingestion job (existing behavior)
      - "direct": call Bedrock direct ingest API (per-doc results)
    """
    rows = state.get("enriched_new", [])
    mode = (state.get("ingest_mode") or DEFAULT_INGEST_MODE).lower()

    if mode == "direct":
        resp = kb_direct_ingest_news_tool.invoke({"rows": rows})
        results = resp.get("results", []) if isinstance(resp, dict) else []
        ok = sum(1 for r in results if (r.get("status") or "").upper() == "SUCCESS")
        fail_items = [
            {"documentId": r.get("documentId"), "reason": r.get("reason") or r.get("statusReason") or "unknown"}
            for r in results if (r.get("status") or "").upper() != "SUCCESS"
        ]
        return {
            "kb_direct_ok": ok,
            "kb_direct_fail": len(fail_items),
            "kb_direct_errors": fail_items,
            # keep S3 fields empty in direct mode
            "kb_uploaded": 0, "kb_ingestion_job_id": None, "kb_ingestion_status": None,
        }

    # default: S3 sync mode
    out = kb_ingest_news_tool.invoke({"rows": rows, "wait": state.get("wait_for_ingest", False)})
    return {
        "kb_uploaded": int(out.get("uploaded", 0)),
        "kb_ingestion_job_id": out.get("ingestion_job_id"),
        "kb_ingestion_status": out.get("status"),
        # reset direct fields in s3 mode
        "kb_direct_ok": 0, "kb_direct_fail": 0, "kb_direct_errors": [],
    }

def _stitch(state: State):
    by_id = {x["news_id"]: x for x in state.get("enriched_new", [])}
    enriched_all = [by_id.get(it["news_id"], it) for it in state.get("items", [])]
    return {"enriched_all": enriched_all}

def _persist_bronze(state: State):
    out = persist_bronze_tool.invoke({
        "rows": state.get("enriched_new", []),
        "extractor_temperature": state["extractor_temperature"]
    })
    return {"bronze_count": out.count}

def _persist_iceberg(state: State):
    out = persist_iceberg_tool.invoke({
        "rows": state.get("enriched_new", []),
        "extractor_temperature": state["extractor_temperature"]
    })
    return {"iceberg_count": out.count}

def _inject_token(url: str, token: str) -> str:
    if not token:
        return url
    if "{TOKEN}" in url:
        return url.replace("{TOKEN}", token)
    parts = urlsplit(url)
    qs = dict(parse_qsl(parts.query, keep_blank_values=True))
    qs.setdefault("token", token)
    new_query = urlencode(qs, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))

# ----- Build graph -----
def build_graph():
    g = StateGraph(State)
    g.add_node("ensure_tables",   RunnableLambda(_ensure_tables))
    g.add_node("fetch_api",       RunnableLambda(_fetch_api))
    g.add_node("dedupe",          RunnableLambda(_dedupe))
    g.add_node("extract",         RunnableLambda(_extract))
    g.add_node("ingest",          RunnableLambda(_ingest))          # 🔁 unified ingest
    g.add_node("stitch",          RunnableLambda(_stitch))
    g.add_node("persist_bronze",  RunnableLambda(_persist_bronze))
    g.add_node("persist_iceberg", RunnableLambda(_persist_iceberg))

    g.add_edge(START,           "ensure_tables")
    g.add_edge("ensure_tables", "fetch_api")
    g.add_edge("fetch_api",     "dedupe")
    g.add_edge("dedupe",        "extract")
    g.add_edge("extract",       "ingest")
    g.add_edge("ingest",        "stitch")
    g.add_edge("stitch",        "persist_bronze")
    g.add_edge("persist_bronze","persist_iceberg")
    g.add_edge("persist_iceberg", END)
    return g

async def run_once(
    api_url: str,
    *,
    max_articles: int = 50,
    timeout_s: int = 15,
    extractor_temperature: float = 0.3,
    ingest_mode: str = DEFAULT_INGEST_MODE,   # "s3" or "direct"
    wait_for_ingest: bool = False,            # only used by "s3" mode
):
    app = build_graph().compile()
    init: State = {
        "messages": [],
        "api_url": api_url,
        "max_articles": max_articles,
        "timeout_s": timeout_s,
        "extractor_temperature": extractor_temperature,
        "ingest_mode": ingest_mode,
        "wait_for_ingest": wait_for_ingest,

        "items": [], "to_process": [], "enriched_new": [], "enriched_all": [],
        "dedup_skipped": 0,
        "kb_uploaded": 0, "kb_ingestion_job_id": None, "kb_ingestion_status": None,
        "kb_direct_ok": 0, "kb_direct_fail": 0, "kb_direct_errors": [],
        "bronze_count": 0, "iceberg_count": 0,
    }
    result: State = await app.ainvoke(init)
    # compact summary
    summary = {
        "dedup_skipped": result.get("dedup_skipped", 0),
        "ingest_mode": ingest_mode,
        # S3 mode metrics
        "kb_uploaded": result.get("kb_uploaded", 0),
        "kb_ingestion_job_id": result.get("kb_ingestion_job_id"),
        "kb_ingestion_status": result.get("kb_ingestion_status"),
        # Direct mode metrics
        "kb_direct_ok": result.get("kb_direct_ok", 0),
        "kb_direct_fail": result.get("kb_direct_fail", 0),
        "kb_direct_errors": result.get("kb_direct_errors", []),
        # Data lake
        "bronze_count": result.get("bronze_count", 0),
        "iceberg_count": result.get("iceberg_count", 0),
        "total_items": len(result.get("enriched_all", [])),
    }
    print(json.dumps(summary, indent=2))
    return result

if __name__ == "__main__":
    import argparse, os, asyncio
    p = argparse.ArgumentParser()
    p.add_argument("--api-url", default=os.environ.get("CRYPTONEWS_URL"))
    p.add_argument("--max-articles", type=int, default=int(os.environ.get("MAX_ARTICLES", 50)))
    p.add_argument("--timeout-s", type=int, default=int(os.environ.get("TIMEOUT_S", 15)))
    p.add_argument("--extractor-temperature", type=float, default=float(os.environ.get("EXTRACTOR_TEMPERATURE", 0.3)))
    p.add_argument("--ingest-mode", choices=["s3","direct"], default=os.environ.get("KB_INGEST_MODE", DEFAULT_INGEST_MODE))
    p.add_argument("--wait-for-ingest", action="store_true",
                   default=os.environ.get("WAIT_FOR_INGEST", "false").lower() == "true")
    args = p.parse_args()

    if not args.api_url:
        raise SystemExit("Missing --api-url or CRYPTONEWS_URL")

    token_env = "CRYPTONEWS_TOKEN"
    token = os.environ.get(token_env)
    final_url = _inject_token(args.api_url, token)

    asyncio.run(run_once(
        api_url=final_url,
        max_articles=args.max_articles,
        timeout_s=args.timeout_s,
        extractor_temperature=args.extractor_temperature,
        ingest_mode=args.ingest_mode,
        wait_for_ingest=args.wait_for_ingest,
    ))
