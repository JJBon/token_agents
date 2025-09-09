from langgraph.graph import StateGraph, START, END
from typing import TypedDict, List, Dict, Any
from tools.market_tools.market_hybrid_tools import (
    list_research_docs_tool,
    retrieve_research_chunks_tool,
    select_news_queries_from_athena_tool,
    query_news_vectors_tool,
    synthesize_marketing_brief_tool,
    athena_news_latest_tool,
)
from tools.market_tools.storage_glue_marketing import ensure_marketing_tables
import re
import os 

class State(TypedDict):
    # inputs
    s3_uri: str  # optional (if provided, we’ll bias toward its doc too)
    max_docs: int
    k_per_doc: int
    # working
    research_doc_ids: List[str]
    research_chunks_by_doc: Dict[str, List[Dict[str, Any]]]
    research_chunks_all: List[Dict[str, Any]]
    athena_news: List[Dict[str, Any]]
    queries: List[str]
    news_items: List[Dict[str, Any]]
    news_assignments: List[Dict[str, Any]]  # [{news_id, chunk_id, doc_id, sim}]
    # outputs
    per_doc_briefs: Dict[str, Dict[str, Any]]   # doc_id -> brief
    overview: Dict[str, Any]                    # consolidated view

def ensure_tables(state: State) -> State:
    ensure_marketing_tables()
    # sensible defaults
    state.setdefault("max_docs", 6)
    state.setdefault("k_per_doc", 6)
    return state

def discover_research_docs(state: State) -> State:
    # Broad pull; you can change the seed based on recent news keywords too.
    lr = list_research_docs_tool.invoke({"seed": "crypto research report", "k": 300}) or {}
    docs = lr.get("docs", [])
    picked = [d["doc_id"] for d in docs[: state["max_docs"]]]
    # If user provided a specific doc via s3_uri, bias-include it
    if state.get("s3_uri"):
        fname = state["s3_uri"].rstrip("/").split("/")[-1]
        did = fname.rsplit(".", 1)[0]
        if did not in picked:
            picked = [did] + picked[:-1] if picked else [did]
    state["research_doc_ids"] = picked
    return state

def retrieve_research_multi(state: State) -> State:
    by_doc, all_chunks = {}, []
    for did in (state.get("research_doc_ids") or []):
        out = retrieve_research_chunks_tool.invoke({"doc_id": did, "k": state.get("k_per_doc", 6)}) or {}
        chunks = out.get("chunks", []) or []
        by_doc[did] = chunks
        all_chunks.extend(chunks)
    state["research_chunks_by_doc"] = by_doc
    state["research_chunks_all"] = all_chunks
    return state

def fetch_athena_latest(state: State) -> State:
    state["athena_news"] = athena_news_latest_tool.invoke({"days_back": 365, "limit": 1000}) or []
    return state

def choose_queries(state: State) -> State:
    # Use ALL research chunks, not just one paper
    out = select_news_queries_from_athena_tool.invoke({
        "research_chunks": state.get("research_chunks_all", []),
        "athena_news": state.get("athena_news", []),
        "min_q": 6, "max_q": 12
    }) or {}
    seen, clean = set(), []
    for q in (out.get("queries") or []):
        q2 = q.strip()
        if q2 and q2.lower() not in seen:
            seen.add(q2.lower()); clean.append(q2)
    state["queries"] = clean
    return state

def query_news_vectors(state: State) -> State:
    items = query_news_vectors_tool.invoke({
        "queries": state.get("queries", []),
        "top_k_per_query": 30,
        "max_total": 300,
        "metadata_filter": {"doc_type": "news"},
    }) or []

    if len(items) < 40:
        baseline = state.get("athena_news", [])[:100]
        seen = {i.get("news_id") for i in items if i.get("news_id")}
        for it in baseline:
            nid = it.get("news_id")
            if nid and nid not in seen:
                it["score"] = it.get("score", 0.01)
                items.append(it); seen.add(nid)

    dedup, seen_t = [], set()
    for it in sorted(items, key=lambda x: x.get("score", 0), reverse=True):
        t = (it.get("title") or "").strip().lower()
        if t and t not in seen_t:
            seen_t.add(t); dedup.append(it)
    state["news_items"] = dedup[:200]
    return state

# ----- assignment: pick best chunk across ALL papers for each news -----
_CHUNK_DOC_RX = re.compile(r"^research#(?P<doc>[^#]+)#")

def _doc_from_chunk_id(cid: str) -> str:
    m = _CHUNK_DOC_RX.match(str(cid or ""))
    return m.group("doc") if m else ""

def assign_news_to_papers(state: State) -> State:
    # Reuse your cosine matching helper via synth tool’s internal function:
    # We'll do a tiny local version to avoid tight coupling.
    def _embed(texts):  # mirrors tools helper
        from vectors.embeddings import embed_texts
        region = os.getenv("AWS_REGION", "us-east-1")
        vecs = embed_texts(texts, region=region) or []
        return list(vecs) if isinstance(vecs, list) else [list(vecs)]
    def _cos(a, b):
        import math
        dot = sum(x*y for x,y in zip(a,b))
        na = math.sqrt(sum(x*x for x in a)); nb = math.sqrt(sum(y*y for y in b))
        return 0.0 if na==0 or nb==0 else dot/(na*nb)

    chunks = state.get("research_chunks_all", [])
    if not chunks:
        state["news_assignments"] = []
        return state

    # prep
    chunk_ids, chunk_texts = [], []
    for c in chunks:
        cid, txt = c.get("chunk_id"), (c.get("text") or "")[:600]
        if cid and txt:
            chunk_ids.append(cid); chunk_texts.append(txt)
    chunk_vecs = _embed(chunk_texts)

    # news text (title + tags + symbols)
    news = state.get("news_items", [])
    news_ids, news_texts = [], []
    for n in news:
        nid = n.get("news_id")
        if not nid: continue
        t = n.get("title") or ""
        tags = " ".join(n.get("tags") or [])
        syms = " ".join(n.get("symbols") or [])
        txt = " ".join([t, tags, syms])[:200].strip()
        if not txt: continue
        news_ids.append(nid); news_texts.append(txt)
    news_vecs = _embed(news_texts)

    # pair
    assignments = []
    MIN_SIM = float(os.getenv("PAIR_MIN_SIM", "0.22"))  # tuneable
    for i, nid in enumerate(news_ids):
        best = ("", -1.0)  # (chunk_id, sim)
        for j, cid in enumerate(chunk_ids):
            sim = _cos(news_vecs[i], chunk_vecs[j])
            if sim > best[1]:
                best = (cid, sim)
        cid, sim = best
        if cid and sim >= MIN_SIM:
            assignments.append({
                "news_id": nid,
                "chunk_id": cid,
                "doc_id": _doc_from_chunk_id(cid),
                "sim": float(round(sim, 4)),
            })
    state["news_assignments"] = assignments
    return state

def synthesize_briefs_per_doc(state: State) -> State:
    # Build per-doc sets and call your existing brief tool per doc.
    per_doc: Dict[str, Dict[str, Any]] = {}
    by_doc_news: Dict[str, List[Dict[str, Any]]] = {}
    by_doc_chunks = state.get("research_chunks_by_doc", {})

    # group news by doc
    nid2news = {n.get("news_id"): n for n in state.get("news_items", []) if n.get("news_id")}
    for a in state.get("news_assignments", []):
        did, nid = a["doc_id"], a["news_id"]
        if not did or nid not in nid2news: 
            continue
        by_doc_news.setdefault(did, []).append(nid2news[nid])

    # call brief per doc (only if it got at least one news)
    for did, news_list in by_doc_news.items():
        rchunks = by_doc_chunks.get(did, [])  # only that doc’s chunks
        meta = {"doc_id": did, "title": did.replace("_"," ").title()}
        ranked = sorted(news_list, key=lambda x: x.get("score", 0), reverse=True)[:40]
        out = synthesize_marketing_brief_tool.invoke({
            "doc_meta": meta,
            "research_chunks": rchunks,
            "news_items": ranked
        }) or {}
        per_doc[did] = out

    # overview = shallow aggregation
    total_pairs = len(state.get("news_assignments", []))
    overview = {
        "doc_count": len(per_doc),
        "assigned_news": total_pairs,
        "docs_ranked": sorted(
            [{"doc_id": did, "news": len(by_doc_news.get(did, []))} for did in per_doc.keys()],
            key=lambda x: x["news"], reverse=True
        )
    }
    state["per_doc_briefs"] = per_doc
    state["overview"] = overview
    return state

# ----- build graph -----
g = StateGraph(State)
g.add_node("ensure_tables", ensure_tables)
g.add_node("discover_research_docs", discover_research_docs)
g.add_node("retrieve_research_multi", retrieve_research_multi)
g.add_node("fetch_athena_latest", fetch_athena_latest)
g.add_node("choose_queries", choose_queries)
g.add_node("query_news_vectors", query_news_vectors)
g.add_node("assign_news_to_papers", assign_news_to_papers)
g.add_node("synthesize_briefs_per_doc", synthesize_briefs_per_doc)

g.add_edge(START, "ensure_tables")
g.add_edge("ensure_tables", "discover_research_docs")
g.add_edge("discover_research_docs", "retrieve_research_multi")
g.add_edge("retrieve_research_multi", "fetch_athena_latest")
g.add_edge("fetch_athena_latest", "choose_queries")
g.add_edge("choose_queries", "query_news_vectors")
g.add_edge("query_news_vectors", "assign_news_to_papers")
g.add_edge("assign_news_to_papers", "synthesize_briefs_per_doc")

app = g.compile()

# Example:

if __name__ == "__main__":
    import asyncio, json

    res = asyncio.run(app.ainvoke({"max_docs": 6, "k_per_doc": 6}))

    # quick health/debug stats
    print("\n=== Pipeline stats ===")
    print("research_chunks:", len(res.get("research_chunks_all", [])))
    print("news_items:", len(res.get("news_items", [])))
    print("news_assignments:", len(res.get("news_assignments", [])))
    print("\n=== Overview ===")
    print(json.dumps(res.get("overview", {}), indent=2))

    print("\n=== Per-doc briefs (truncated) ===")
    briefs = res.get("per_doc_briefs", {}) or {}
    for did, brief in briefs.items():
        title = brief.get("title") or did
        # adjust these keys to whatever your synth tool returns
        body = (
            brief.get("summary")
            or brief.get("content")
            or brief.get("body")
            or brief.get("text")
            or ""
        )
        print(f"\n# {title}\n{body[:1500]}{'…' if len(body) > 1500 else ''}")
