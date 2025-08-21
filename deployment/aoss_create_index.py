#!/usr/bin/env python3
"""
Create (or noop) an OpenSearch Serverless vector index for an Amazon Bedrock KB.

Usage:
  ./aoss_create_index_boto3.py \
    --collection-arn arn:aws:aoss:us-east-1:123456789012:collection/abc123 \
    --region us-east-1 \
    --index bedrock-knowledge-base-default-index \
    --vector-dim 1024 \
    [--assume-role-arn arn:aws:iam::123456789012:role/your-role]
"""

import argparse, json, sys, time, typing as t
import boto3
from botocore.exceptions import ClientError


def build_schema(vector_dim: int, ef_search: int, shards: int, replicas: int) -> dict:
    return {
        "settings": {
            "index": {
                "knn": True,
                "knn.algo_param.ef_search": str(ef_search),
                "number_of_shards": str(shards),
                "number_of_replicas": str(replicas),
            }
        },
        "mappings": {
            "properties": {
                "bedrock-knowledge-base-default-vector": {
                    "type": "knn_vector",
                    "dimension": vector_dim,
                    "method": {
                        "engine": "faiss",
                        "name": "hnsw",
                        "space_type": "l2",
                        "parameters": {"ef_construction": 512, "m": 16},
                    },
                },
                "AMAZON_BEDROCK_TEXT_CHUNK": {"type": "text"},
                "AMAZON_BEDROCK_METADATA": {"type": "text"},
            }
        },
    }


def make_client(region: str, assume_role_arn: str | None):
    if assume_role_arn:
        sts = boto3.client("sts", region_name=region)
        resp = sts.assume_role(
            RoleArn=assume_role_arn,
            RoleSessionName=f"aoss-index-create-{int(time.time())}",
        )
        creds = resp["Credentials"]
        return boto3.client(
            "opensearchserverless",
            region_name=region,
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )
    return boto3.client("opensearchserverless", region_name=region)


def whoami(region: str):
    return boto3.client("sts", region_name=region).get_caller_identity()["Arn"]


def index_exists(aoss, col_id: str, index_name: str) -> bool:
    try:
        aoss.get_index(id=col_id, indexName=index_name)
        return True
    except aoss.exceptions.ResourceNotFoundException:
        return False


def wait_until_visible(aoss, col_id: str, index_name: str, timeout_sec: int = 60) -> bool:
    """Poll get_index until it returns 200 or timeout."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if index_exists(aoss, col_id, index_name):
            return True
        time.sleep(2)
    return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--collection-arn", required=True, help="AOSS collection ARN")
    p.add_argument("--region", required=True)
    p.add_argument("--index", required=True, help="Index name to create")
    p.add_argument("--vector-dim", type=int, default=1024)
    p.add_argument("--ef-search", type=int, default=512)
    p.add_argument("--shards", type=int, default=1)
    p.add_argument("--replicas", type=int, default=1)
    p.add_argument("--assume-role-arn", default=None)
    args = p.parse_args()

    col_id = args.collection_arn.split("/")[-1]
    aoss = make_client(args.region, args.assume_role_arn)

    print(f"🔐 Calling AOSS as: {whoami(args.region)}")
    if index_exists(aoss, col_id, args.index):
        print(f"✅ Index {args.index} already exists.")
        return 0

    schema = build_schema(args.vector_dim, args.ef_search, args.shards, args.replicas)
    print(f"🛠  Creating index {args.index} in collection {col_id} ...")
    try:
        aoss.create_index(id=col_id, indexName=args.index, indexSchema=schema)
    except aoss.exceptions.ConflictException:
        print(f"ℹ️  Index {args.index} already exists (Conflict).")
        return 0
    except ClientError as e:
        print(f"❌ Create failed: {e}", file=sys.stderr)
        return 1

    if wait_until_visible(aoss, col_id, args.index, timeout_sec=90):
        print(f"✅ Index {args.index} is visible.")
        return 0

    print("❌ Timed out waiting for index visibility.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
