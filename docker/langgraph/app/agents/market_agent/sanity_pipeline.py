import os, json
from tools.market_tools.market_hybrid_tools import (
    list_research_docs_tool,
    ensure_research_doc_ready_tool,
    kb_ingest_research_tool,
    run_market_agent_for_all_docs_tool,
)

LIMIT_DOCS   = int(os.getenv("SANITY_DOC_LIMIT", "4"))
DAYS_BACK    = int(os.getenv("NEWS_DAYS_BACK", "120"))
REINGEST     = os.getenv("REINGEST_RESEARCH", "0") == "1"
REINGEST_WAIT= os.getenv("REINGEST_WAIT", "1") == "1"
MIN_CHARS    = int(os.getenv("REINGEST_MIN_CHARS", "0"))  # 0 => always upload

def _build_docs_for_sidecar(docs):
    docs_to_ingest = []
    for d in docs:
        doc_id = d.get("doc_id")
        title  = d.get("title") or doc_id
        if not doc_id:
            continue

        # Pull chunks (and classify missing metadata)
        prepared = ensure_research_doc_ready_tool.invoke({"doc_id": doc_id, "k": 80})
        chunks = prepared.get("chunks") or []

        # Join chunk text; if short/empty, still provide a tiny body so TXT exists
        text_parts = [(c.get("text") or "").strip() for c in chunks if (c.get("text") or "").strip()]
        joined = "\n\n".join(text_parts)
        if len(joined) < MIN_CHARS:
            joined = (joined or (title + "\n")).strip()

        # Aggregate doc-level metadata
        syms    = sorted({s for c in chunks for s in (c.get("symbols") or []) if s})[:16]
        tags    = sorted({t for c in chunks for t in (c.get("tags") or []) if t})[:24]
        drivers = sorted({r for c in chunks for r in (c.get("drivers") or []) if r})[:16]

        docs_to_ingest.append({
            "doc_id": doc_id,
            "title": title,
            "text": joined,
            # optional date—leave blank unless you track one
            "as_of": os.getenv("RESEARCH_AS_OF", ""),
            "symbols": syms,
            "tags": tags,
            "drivers": drivers,
        })

        print(f"  · built {doc_id}: chunks={len(chunks)}, text_len={len(joined)}")
    return docs_to_ingest

def main():
    # Optional re-ingestion to attach sidecars to existing PDFs
    if REINGEST:
        all_docs = (list_research_docs_tool.invoke({"k": LIMIT_DOCS}).get("docs") or [])[:LIMIT_DOCS]
        print(f"\n▶ Re-ingesting research docs into KB sidecars (count={len(all_docs)}, wait={REINGEST_WAIT}) …")
        docs_to_ingest = _build_docs_for_sidecar(all_docs)
        if docs_to_ingest:
            res = kb_ingest_research_tool.invoke({"docs": docs_to_ingest, "wait": REINGEST_WAIT})
            print("Ingestion result:", json.dumps(res, indent=2))
        else:
            print("ℹ️  No docs built (but continuing).")

    # Full run across docs -> briefs -> aggregate
    result = run_market_agent_for_all_docs_tool.invoke({
        "limit_docs": LIMIT_DOCS,
        "days_back": DAYS_BACK,
    })

    print("\n=== SUMMARY ===")
    print(f"Docs processed: {result.get('count_docs')}")
    final = result.get("final") or {}
    print("Final aggregate brief:")
    print(json.dumps(final, indent=2))

    per_doc = result.get("per_doc") or []
    for i, d in enumerate(per_doc, 1):
        print("\n" + "="*80)
        print(f"[{i}/{len(per_doc)}] DOC: {d.get('title') or d.get('doc_id')}")
        print(f"  classified_updates: {d.get('classified_updates', 0)}")
        print(f"  queries: {', '.join(d.get('queries', [])[:8])}")
        news = d.get("news") or []
        print(f"  matched_news: {len(news)}")
        for h in news:
            print(f"    - {h.get('title')}  (score={h.get('score'):.3f}, src={h.get('source_name')})")
        brief = d.get("brief") or {}
        print("  brief.summary:")
        sent = d.get("sentiment") or {}
        if sent:
            print(f"  sentiment.stance: {sent.get('stance')} (net={sent.get('net_score')})")
            if sent.get("bullish_drivers"):
                print("  bullish_drivers:", ", ".join(sent["bullish_drivers"][:3]))
            if sent.get("bearish_drivers"):
                print("  bearish_drivers:", ", ".join(sent["bearish_drivers"][:3]))

if __name__ == "__main__":
    main()
