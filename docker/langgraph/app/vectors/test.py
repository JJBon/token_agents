# vectors/test.py
import os, inspect, sys
from vectors.s3vectors_client import S3Vectors
from vectors.embeddings import embed_texts

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
VEC_BUCKET = os.getenv("VEC_BUCKET", "news-index")
VEC_INDEX  = os.getenv("VEC_INDEX",  "crypto-research-index")

s3v = S3Vectors(region=AWS_REGION, bucket=VEC_BUCKET, index=VEC_INDEX)

# Show the actual signature so you can see what's allowed
#print("S3Vectors.query signature:", inspect.signature(S3Vectors.query), file=sys.stderr)

#query_text = "YZi Labs invests in USD.AI stablecoin, backed by AI hardware"
query_text = "crypto"
qvec = embed_texts([query_text], region=AWS_REGION)[0]

def call_query_positional(client, vec, top_k=5):
    """Try common positional signatures: (vec, k), (vec,), or () if the client stores its own query vec."""
    for args in [(vec, top_k), (vec,), ()]:
        try:
            return client.query(*args)
        except TypeError:
            continue
    raise RuntimeError("Could not call S3Vectors.query with any positional variant.")
DOC_ID = "8bd6ab8f5f14d6bee4f38542429110f9"
resp = s3v.query(qvec, 25)  # top_k=5
print("response: ", resp)
for r in resp:
    # Common response fields across shapes
    key   = r.get("key") or r.get("id")
    dist  = r.get("distance") or r.get("score")
    meta  = r.get("metadata", {})
    url   = meta.get("url")
    head  = meta.get("headline")
    print(f"{dist:.4f} | {head} | {url} | {key}")

# Normalize results into an iterable of matches
def iter_matches(r):
    if isinstance(r, dict):
        if "matches" in r: return r["matches"]
        if "results" in r: return r["results"]
    return r  # assume it's already a list

def is_news(m):
    meta = (m or {}).get("metadata", {})
    return meta.get("doc_type") == "news"

hits = s3v.query(qvec, 5, {"doc_type": "news"})

def _dist(h):
    # prefer distance; fall back to 1 - score if only similarity is present
    if "distance" in h: 
        return float(h["distance"])
    if "score" in h:     
        return 1.0 - float(h["score"])
    return float("inf")


seen_keys = set()
rows = []
for h in hits:
    if not isinstance(h, dict): 
        continue
    key = h.get("key") or h.get("id")
    if not key or key in seen_keys:
        continue
    seen_keys.add(key)

    meta = h.get("metadata") or {}
    rows.append((_dist(h), meta.get("headline"), meta.get("url"), key))

# distance asc (closest first)
rows.sort(key=lambda x: x[0])

for d, title, url, key in rows:
    if d == float("inf"):
        print(f"--no-dist-- | {title} | {url} | {key}")
    else:
        print(f"{d:.4f} | {title} | {url} | {key}")