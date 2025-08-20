# sentiment_agent_refactor.py — Athena/Iceberg grounded summaries + lightweight insight memory
#
# What’s new vs your current file:
#  - Adds history features (rolling stats, per-asset trend) fetched from Athena
#  - Adds a structured Insight memory table in Iceberg
from __future__ import annotations
from typing import Annotated, Any, TypedDict, List, Dict
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langchain_core.runnables import RunnableLambda
import json

from tools.market_tools.tools_wrapper import (
    signals_tool, summarize_market_tool,
    upsert_summary_tool, store_insights_tool, score_insights_tool
)
import os 
import asyncio

class State(TypedDict):
    messages: Annotated[List[Any], add_messages]
    days: int
    write: bool            # ← NEW
    score: bool            # ← NEW
    signals: Dict
    summary: Dict
    persisted: bool
    scored: bool

async def _fetch_signals(state: State):
    days = state.get("days", 3)
    sigs = signals_tool.invoke({"days": days, "limit_headlines": 40})
    return {"signals": sigs.model_dump()}

async def _summarize(state: State):
    days = state.get("days", 3)
    summary = await summarize_market_tool.ainvoke({"days": days})
    return {"summary": summary.model_dump()}

def _persist(state: State):
    if not state.get("write", False):
        return {"persisted": False}
    days = state.get("days", 3)
    s = state["summary"]
    upsert_summary_tool.invoke({"days": days, "summary": s})
    if s.get("new_insights"):
        ws, we = s["time_window"].split("..")
        store_insights_tool.invoke({
            "window_start": ws,
            "window_end": we,
            "insights": s["new_insights"],
            "valid_days": 7
        })
    return {"persisted": True}

def _score(state: State):
    if not state.get("score", False):
        return {"scored": False}
    s = state["summary"]
    _, we = s["time_window"].split("..")
    score_insights_tool.invoke({"window_end_ts": we, "net_threshold": 0.05})
    return {"scored": True}

def build_graph() -> StateGraph:
    g = StateGraph(State)
    g.add_node("fetch_signals", RunnableLambda(_fetch_signals))
    g.add_node("summarize",     RunnableLambda(_summarize))
    g.add_node("persist",       RunnableLambda(_persist))
    g.add_node("score",         RunnableLambda(_score))

    g.add_edge(START, "fetch_signals")
    g.add_edge("fetch_signals", "summarize")
    g.add_edge("summarize", "persist")
    g.add_edge("persist", "score")
    g.add_edge("score", END)
    return g

async def run_once(days: int = 3, write: bool = False, score: bool = True):
    """Compile and run the deterministic graph once."""
    app = build_graph().compile()
    # messages can be left empty; they’re useful if you log AI/Human turns
    init: State = {
        "messages": [],
        "days": days,
        "write": write,
        "score": score,
        "signals": {},
        "summary": {},
        "persisted": False,
        "scored": False,
    }
    result: State = await app.ainvoke(init)
    # Pretty print the summary result
    if "summary" in result and result["summary"]:
        print(json.dumps(result["summary"], indent=2))
    else:
        print("No summary produced.")
    return result

if __name__ == "__main__":
    async def _main():
        days = int(os.environ.get("SUMMARY_DAYS", "3"))
        write = os.environ.get("WRITE_SUMMARY", "false").lower() == "true"
        # If SCORE_INSIGHTS not set, default to score only when we’re writing
        score = os.environ.get("SCORE_INSIGHTS", "").lower()
        score_flag = (score == "true") if score in ("true", "false") else write
        await run_once(days=days, write=write, score=score_flag)
    asyncio.run(_main())