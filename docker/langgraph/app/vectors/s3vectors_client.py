# Minimal wrapper around AWS S3 Vectors
from __future__ import annotations
from typing import List, Optional, Dict, Any
import boto3

class S3Vectors:
    def __init__(self, region: str, bucket: str, index: str):
        self.cli = boto3.client("s3vectors", region_name=region)
        self.bucket = bucket
        self.index = index

    def _wrap_query_vec(self, vec: List[float]) -> Dict[str, Any]:
        # Your service model says queryVector must be { float32: [...] }
        # Ensure plain Python floats (not numpy types)
        return {"float32": [float(x) for x in vec]}

    def query(self, query_vec, top_k=10, filt=None):
        payload = {
            "vectorBucketName": self.bucket,
            "indexName": self.index,
            "topK": int(top_k),
            "queryVector": {"float32": [float(x) for x in query_vec]},
            "returnMetadata": True,
            "returnDistance": True,
        }
        if filt:
            payload["filter"] = filt
        out = self.cli.query_vectors(**payload)

        if isinstance(out, dict):
            for k in ("vectors", "neighbors", "matches", "results"):
                if k in out:
                    return out[k]
        return out  # as-is (already a list)

    # (Optional) If you also have a put_vectors, make sure it uses vectorBucketName
    def put_vectors(self, vectors: List[Dict[str, Any]]):
        # expect each item like:
        # {"key": "...", "data": {"float32": [...]}, "metadata": {...}}
        payload = {
            "vectorBucketName": self.bucket,
            "indexName": self.index,
            "vectors": vectors,
        }
        return self.cli.put_vectors(**payload)