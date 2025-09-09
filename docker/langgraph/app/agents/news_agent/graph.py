# news_ingest_graph.py
from __future__ import annotations
import os, json, asyncio
from typing import Any, Dict, List, Annotated, TypedDict

from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langchain_core.runnables import RunnableLambda

from tools.news_tools.tools import (
    ensure_iceberg_tables_tool,
    fetch_news_api_tool, dedupe_news_ids_tool,
    scrape_article_text_tool, llm_extract_mentions_tool,
    # index_vectors_tool,  # 🔁 removed: we now use KB ingestion instead
    persist_bronze_tool, persist_iceberg_tool,
    merge_hints_and_tokens,
    # ✅ new tools:
    kb_ingest_news_tool,
    # kb_retrieve_news_tool,  # (import if you want retrieval in this flow)
)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

class State(TypedDict):
    messages: Annotated[List[Any], add_messages]
    api_url: str
    max_articles: int
    timeout_s: int
    extractor_temperature: float
    wait_for_ingest: bool  # whether to block on the ingestion job

    items: List[Dict]
    to_process: List[Dict]
    enriched_new: List[Dict]
    enriched_all: List[Dict]

    dedup_skipped: int
    # indexed_chunks: int  # 🔁 removed
    kb_uploaded: int
    kb_ingestion_job_id: str | None
    kb_ingestion_status: str | None
    bronze_count: int
    iceberg_count: int

# ----- Nodes (each node calls a tool) -----
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

    async def process(item: Dict[str, Any]) -> Dict[str, Any]:
        url = item.get("news_url") or ""
        text = scrape_article_text_tool.invoke({"url": url, "timeout_s": state["timeout_s"]}).text if url else ""
        if not text:
            text = f"{item.get('title','')}\n\n{item.get('text','')}"
        llm_out = llm_extract_mentions_tool.invoke({
            "title": item.get("title",""),
            "source": item.get("source_name",""),
            "url": url, "body": text
        })
        currencies = merge_hints_and_tokens(text, llm_out.tokens)
        out = {**item}
        out["full_text"] = text[:240_000]
        out["currencies"] = currencies
        out["_evidence"] = []
        return out

    enriched_new = await asyncio.gather(*[process(x) for x in to_process])
    return {"enriched_new": enriched_new}

def _kb_ingest(state: State):
    """
    Upload plain-text docs + sidecar metadata.json to the news S3 bucket,
    then StartIngestionJob on the Bedrock KB data source.
    """
    out = kb_ingest_news_tool.invoke({
        "rows": state.get("enriched_new", []),
        "wait": state.get("wait_for_ingest", False),
    })
    return {
        "kb_uploaded": int(out.get("uploaded", 0)),
        "kb_ingestion_job_id": out.get("ingestion_job_id"),
        "kb_ingestion_status": out.get("status"),
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

# ----- Build graph -----
def build_graph():
    g = StateGraph(State)
    g.add_node("ensure_tables",   RunnableLambda(_ensure_tables))
    g.add_node("fetch_api",       RunnableLambda(_fetch_api))
    g.add_node("dedupe",          RunnableLambda(_dedupe))
    g.add_node("extract",         RunnableLambda(_extract))
    g.add_node("kb_ingest",       RunnableLambda(_kb_ingest))      # 🔁 replaces index_vectors
    g.add_node("stitch",          RunnableLambda(_stitch))
    g.add_node("persist_bronze",  RunnableLambda(_persist_bronze))
    g.add_node("persist_iceberg", RunnableLambda(_persist_iceberg))

    g.add_edge(START,           "ensure_tables")
    g.add_edge("ensure_tables", "fetch_api")
    g.add_edge("fetch_api",     "dedupe")
    g.add_edge("dedupe",        "extract")
    g.add_edge("extract",       "kb_ingest")       # 🔁
    g.add_edge("kb_ingest",     "stitch")
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
    wait_for_ingest: bool = False,   # <- pass True if you want to block until KB job completes
):
    app = build_graph().compile()
    init: State = {
        "messages": [],
        "api_url": api_url,
        "max_articles": max_articles,
        "timeout_s": timeout_s,
        "extractor_temperature": extractor_temperature,
        "wait_for_ingest": wait_for_ingest,

        "items": [], "to_process": [], "enriched_new": [], "enriched_all": [],
        "dedup_skipped": 0,
        "kb_uploaded": 0,
        "kb_ingestion_job_id": None,
        "kb_ingestion_status": None,
        "bronze_count": 0,
        "iceberg_count": 0,
    }
    result: State = await app.ainvoke(init)
    print(json.dumps({
        "dedup_skipped": result.get("dedup_skipped", 0),
        "kb_uploaded": result.get("kb_uploaded", 0),
        "kb_ingestion_job_id": result.get("kb_ingestion_job_id"),
        "kb_ingestion_status": result.get("kb_ingestion_status"),
        "bronze_count": result.get("bronze_count", 0),
        "iceberg_count": result.get("iceberg_count", 0),
        "total_items": len(result.get("enriched_all", [])),
    }, indent=2))
    return result

if __name__ == "__main__":
    import asyncio, os
    url = os.environ.get("CRYPTONEWS_URL")
    if not url:
        print("Set CRYPTONEWS_URL")
    else:
        asyncio.run(run_once(url, wait_for_ingest=False))
