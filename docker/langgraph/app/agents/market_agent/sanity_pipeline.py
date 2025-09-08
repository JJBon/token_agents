# sanity_pipeline.py
import os, json
from tools.market_tools.market_hybrid_tools import (
    retrieve_research_chunks_tool,
    athena_news_latest_tool,
    select_news_queries_from_athena_tool,
    query_news_vectors_tool,
    synthesize_marketing_brief_tool,
)




# 1) research
res = retrieve_research_chunks_tool.invoke({
    "k": 10,
})
chunks = res["chunks"]
print("research chunks:", len(chunks), chunks)

# 2) latest headlines from Athena
ath = athena_news_latest_tool.invoke({"days_back": 365, "limit": 1000})
print("athena headlines:", len(ath), "sample:", ath[0]["title"] if ath else None)

# 3) derive queries
sel = select_news_queries_from_athena_tool.invoke({"research_chunks": chunks, "athena_news": ath})
queries = sel["queries"]
print("queries:", len(queries), queries[:5])

# 4) vector search
hits = query_news_vectors_tool.invoke({
    "queries": queries,
    "top_k_per_query": 20,
    "max_total": 200,
    "metadata_filter": {"doc_type": "news"},
})
print("vector hits:", len(hits), [h["news_id"] for h in hits[:5]])

# # 5) synthesize
brief = synthesize_marketing_brief_tool.invoke({
    "doc_meta": {"doc_id": DOC_ID, "title": "Test Research Paper Marketing Agent"},
    "research_chunks": chunks,
    "news_items": hits[:20],
})
print("brief: ",json.dumps(brief, indent=2))
