from __future__ import annotations
import json, boto3
from typing import List

EMBED_MODEL = "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v2:0"

def embed_texts(texts: List[str], region: str = "us-east-1") -> List[List[float]]:
    br = boto3.client("bedrock-runtime", region_name=region)
    out = []
    for t in texts:
        r = br.invoke_model(modelId=EMBED_MODEL, body=json.dumps({"inputText": t[:4000]}))
        out.append(json.loads(r["body"].read())["embedding"])
    return out
