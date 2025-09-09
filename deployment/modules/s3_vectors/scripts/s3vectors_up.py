#!/usr/bin/env python3
"""
Create (idempotent) an Amazon S3 Vectors bucket and index.

Requires: boto3 >= 1.34.150 (or any version that includes 's3vectors').
"""

import argparse, sys, os, json
import boto3
from botocore.exceptions import ClientError

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--bucket-name", required=True)
    p.add_argument("--index-name", required=True)
    p.add_argument("--dimension", type=int, required=True)
    p.add_argument("--distance-metric", choices=["cosine", "euclidean"], default="cosine")
    p.add_argument("--non-filterable-keys", default="", help="Comma-separated list, optional")
    p.add_argument("--sse-type", choices=["AES256", "aws:kms"], default="AES256")
    p.add_argument("--kms-key-arn", default="")
    return p.parse_args()

def ensure_bucket(client, bucket_name, sse_type, kms_arn):
    # Try GetVectorBucket; if not found, create.
    try:
        client.get_vector_bucket(vectorBucketName=bucket_name)
        print(f"✅ Vector bucket '{bucket_name}' already exists.")
    except client.exceptions.NotFoundException:
        enc = {"sseType": sse_type}
        if sse_type == "aws:kms":
            if not kms_arn:
                print("❌ --kms-key-arn required when --sse-type aws:kms", file=sys.stderr)
                sys.exit(1)
            enc["kmsKeyArn"] = kms_arn
        print(f"🪣 Creating vector bucket '{bucket_name}' ...")
        client.create_vector_bucket(vectorBucketName=bucket_name, encryptionConfiguration=enc)
        print("✅ Bucket created.")
    except ClientError as e:
        print(f"❌ get/create bucket failed: {e}", file=sys.stderr)
        sys.exit(1)

def ensure_index(client, bucket_name, index_name, dim, metric, non_filterable_keys):
    # Check by listing and matching name (or GetIndex once SDK exposes it).
    try:
        exists = False
        paginator = client.get_paginator("list_indexes")
        for page in paginator.paginate(vectorBucketName=bucket_name):
            for idx in page.get("indexes", []):
                if idx.get("indexName") == index_name:
                    exists = True
                    break
            if exists: break
        if exists:
            print(f"✅ Vector index '{index_name}' already exists.")
            return

        print(f"🧭 Creating vector index '{index_name}' (dim={dim}, metric={metric}) ...")
        client.create_index(
            vectorBucketName=bucket_name,
            indexName=index_name,
            dataType="float32",
            dimension=dim,
            distanceMetric=metric,
            metadataConfiguration={"nonFilterableMetadataKeys": non_filterable_keys},
        )
        print("✅ Index created.")
    except ClientError as e:
        print(f"❌ create index failed: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    a = parse_args()
    os.environ["AWS_DEFAULT_REGION"] = a.region
    client = boto3.client("s3vectors", region_name=a.region)

    non_filterable = [k for k in (x.strip() for x in a.non_filterable_keys.split(",")) if k]
    ensure_bucket(client, a.bucket_name, a.sse_type, a.kms_key_arn)
    ensure_index(client, a.bucket_name, a.index_name, a.dimension, a.distance_metric, non_filterable)

if __name__ == "__main__":
    main()
