# tools/market_tools_new.py
import os, io, re, json, hashlib, boto3
from typing import List, Dict, Any
from langchain_core.tools import tool
from tools.athena_client import query, rows
from vectors.s3vectors_client import S3Vectors
from vectors.embeddings import embed_texts
import botocore

import os, time


AWS_REGION = os.getenv("AWS_REGION","us-east-1")
S3_BUCKET  = os.getenv("S3_BUCKET")
VEC_BUCKET = os.getenv("VEC_BUCKET")
VEC_INDEX_RESEARCH = os.getenv("VEC_INDEX_RESEARCH")
_READY_STATES = {"ready", "active", "available", "created"}

s3 = boto3.client("s3", region_name=AWS_REGION)



def _metric_alias(m: str) -> str:
    m = (m or "").lower()
    if m in ("cos", "cosine", "angular"):
        return "cosine"
    if m in ("l2", "euclidean"):
        return "euclidean"
    return "cosine"  # safe default


def _state_of(resp: dict) -> str:
    return str(
        resp.get("status")
        or resp.get("state")
        or (resp.get("index") or {}).get("status")
        or (resp.get("index") or {}).get("state")
        or ""
    ).lower()

@tool("ensure_research_vector_index")
def ensure_research_vector_index_tool(probe_text: str = "probe", metric: str = "cosine") -> dict:
    """
    Ensure S3Vectors index exists and is READY.
    Uses get_index/create_index (snake_case) and the required parameters:
    - vectorBucketName, indexName, dataType, dimension, distanceMetric
    """
    # infer embedding dimension
    dim = len(embed_texts([probe_text], region=AWS_REGION)[0])
    dist = _metric_alias(metric)  # -> "cosine" or "euclidean"

    cli = boto3.client("s3vectors", region_name=AWS_REGION)

    # exists?
    try:
        r = cli.get_index(vectorBucketName=VEC_BUCKET, indexName=VEC_INDEX_RESEARCH)
    except botocore.exceptions.ClientError as e:
        print("unable to get index")
        code = e.response.get("Error", {}).get("Code", "")
        if code not in ("NotFoundException", "ResourceNotFoundException"):
            return {"ok": False, "error": str(e), "where": "get_index"}

        # create (must use lowercase enums the service supports)
        print(f"[s3vectors] creating index dim={dim} metric={dist}")
        try:
            cli.create_index(
                vectorBucketName=VEC_BUCKET,
                indexName=VEC_INDEX_RESEARCH,
                dataType="float32",        # <- required, lowercase
                dimension=dim,
                distanceMetric=dist        # <- only 'cosine' or 'euclidean'
            )
        except botocore.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code not in ("ConflictException", "ResourceInUseException"):
                return {"ok": False, "error": str(e), "where": "create_index"}


    # show server view before failing
    try:
        lst = cli.list_indices(vectorBucketName=VEC_BUCKET)
    except Exception as e:
        lst = {"error": str(e)}
    return {"ok": False, "error": "index not ready after wait", "list_indices": lst, "metric": dist}

# ---------- helpers ----------
def _norm_text(t: str) -> str:
    return re.sub(r"\s+"," ", (t or "").strip())

def _hash(s: str, n=16) -> str:
    return hashlib.blake2b(_norm_text(s).encode("utf-8"), digest_size=n).hexdigest()

def _chunk(text: str, max_len: int = 1400) -> List[str]:
    text = _norm_text(text)
    if len(text) <= max_len: return [text]
    chunks, i = [], 0
    while i < len(text):
        j = min(i+max_len, len(text))
        k = text.rfind(".", i, j)
        cut = k+1 if k!=-1 and k>i+400 else j
        chunks.append(text[i:cut].strip()); i = cut
    return [c for c in chunks if c]

def _chunk_key(doc_id: str, chunk_text: str, model: str = "e5-small") -> str:
    return f"research#{doc_id}#m:{model}#v1#h:{_hash(chunk_text, n=8)}"

# ---------- 1) Chunk PDFs dropped in S3 ----------
@tool("chunk_research_pdf")
def chunk_research_pdf_tool(s3_uri: str) -> Dict[str, Any]:
    """
    Read a PDF at s3://bucket/key, extract text, make chunks, return doc meta and chunks.
    """
    assert isinstance(s3_uri, str) and s3_uri.startswith("s3://"), "s3_uri must be like s3://bucket/key"
    _, _, path = s3_uri.partition("s3://")
    bucket, _, key = path.partition("/")
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()

    # Try pdfminer.six then fallback
    try:
        from pdfminer.high_level import extract_text
        txt = extract_text(io.BytesIO(data)) or ""
    except Exception:
        txt = data.decode("latin-1", "ignore")

    chunks = _chunk(txt)
    doc_id = _hash(s3_uri + ":" + (txt[:2048] or ""), n=16)  # stable-ish
    manifest = {
        "doc_id": doc_id,
        "source_s3": s3_uri,
        "pages": None,
        "chunks_count": len(chunks),
        "content_hash": _hash(txt, n=16),
    }
    chunk_rows = [{"doc_id": doc_id, "ord": i, "text": ch, "chunk_hash": _hash(ch, n=8)} for i, ch in enumerate(chunks)]
    return {"meta": manifest, "chunks": chunk_rows}

# ---------- 2) Index research chunks into S3 Vectors (idempotent by key) ----------
@tool("index_research_vectors")
def index_research_vectors_tool(doc_id: str, chunks: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Upsert chunks to S3Vectors with content-addressed keys.
    """
    print("searching existing vectors")
    if not chunks:
        return {"indexed": 0}
    s3v = S3Vectors(region=AWS_REGION, bucket=VEC_BUCKET, index=VEC_INDEX_RESEARCH)
    texts = [c["text"] for c in chunks]
    vecs  = embed_texts(texts, region=AWS_REGION)  # returns list[list[float]]
    batch = []
    for c, v in zip(chunks, vecs):
        key = _chunk_key(doc_id, c["text"])
        batch.append({
            "key": key,
            "data": {"float32": [float(x) for x in v]},
            "metadata": {
                "doc_id": doc_id,
                "ord": int(c["ord"]),
                "chunk_hash": c["chunk_hash"],
                "type": "research",
            },
        })
    s3v.put_vectors(batch)  # upsert semantics
    return {"indexed": len(batch)}

# ---------- 3) Extract topics/keywords from the research text ----------
@tool("extract_research_topics")
def extract_research_topics_tool(text: str, top_k: int = 12) -> Dict[str, List[str]]:
    """
    Use Bedrock LLM to produce normalized topics/keywords for the research paper.
    """
    from langchain_aws import ChatBedrockConverse
    llm = ChatBedrockConverse(
        model="anthropic.claude-3-haiku-20240307-v1:0",
        provider="anthropic",
        temperature=0.2,
        client=boto3.client("bedrock-runtime", region_name=AWS_REGION),
    )
    prompt = (
        "Return JSON with two arrays: topics and keywords (lowercased, deduped), "
        f"<= {top_k} items each. Focus on domain terms.\n\nTEXT:\n{_norm_text(text)[:6000]}"
    )
    resp = llm.invoke(prompt)
    try:
        data = json.loads(resp.content[0].text)  # adjust if your wrapper differs
    except Exception:
        data = {"topics": [], "keywords": []}
    return {"topics": data.get("topics", []), "keywords": data.get("keywords", [])}

# ---------- 4) Bridge: fetch candidate news relevant to the research ----------
@tool("find_relevant_news")
def find_relevant_news_tool(topics: List[str], keywords: List[str], days_back: int = 30, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Query Athena (news_agent.cryptoapi_news) using topic/keyword LIKE + recent window.
    Returns list of dicts: news_id, title, news_url, source_name, published_at, sentiment, symbols[]
    """
    toks = [t.strip().lower() for t in set((topics or []) + (keywords or [])) if t]
    if not toks:
        return []

    # Build OR filter across title and tags; safe-ish concat for Athena/Trino
    def _esc(s: str) -> str:
        return s.replace("'", "''")
    clauses = [
        f"lower(title) LIKE '%{_esc(t)}%' OR array_join(transform(api_payload_obj.tags, x -> lower(x)), ' ') LIKE '%{_esc(t)}%'"
        for t in toks
    ]
    like = " OR ".join(clauses)

    sql = f"""
    SELECT
      news_id,
      title,
      news_url,
      source_name,
      published_at,
      sentiment,
      transform(currencies_arr, x -> x.symbol) AS symbols
    FROM news_agent.cryptoapi_news
    WHERE published_at >= current_timestamp - INTERVAL '{int(days_back)}' DAY
      AND ({like})
    ORDER BY published_at DESC
    LIMIT {int(limit)}
    """
    qid = query(sql)
    cols = ["news_id","title","news_url","source_name","published_at","sentiment","symbols"]
    return [dict(zip(cols, r)) for r in rows(qid)]
