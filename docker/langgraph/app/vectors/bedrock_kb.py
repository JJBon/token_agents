# vectors/bedrock_kb.py
import boto3

client = boto3.client("bedrock-agent-runtime", region_name="us-east-1")

def kb_query_texts(index: str, texts: list, top_k: int = 20):
    """
    Query a Bedrock Knowledge Base by semantic similarity.
    index: your KB ID (string)
    texts: list of query strings
    """
    results = []
    for t in texts:
        resp = client.retrieve(
            knowledgeBaseId=index,   # e.g. your NEWS KB ID
            retrievalQuery={"text": t},
            retrievalConfiguration={"vectorSearchConfiguration": {"numberOfResults": top_k}},
        )
        for item in resp.get("retrievalResults", []):
            results.append({
                "text": item["content"]["text"],
                "score": item["score"],
                "payload": item.get("metadata", {}),
            })
    return results
