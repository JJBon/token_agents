import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from datetime import datetime
from io import BytesIO
import os
import time

S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = "coingecko/raw"
PER_PAGE = 250

def fetch_all_pages(per_page=250, max_pages=20, pause=2):
    all_data, page = [], 1
    while page <= max_pages:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "sparkline": "true",
                "page": page,
                "per_page": PER_PAGE,
                "price_change_percentage": "1h,24h,7d",
                "precision": 3,
                "x_cg_demo_api_key": os.environ["COINGECKO_API_KEY"]
            },
        )
        if resp.status_code == 429:
            time.sleep(pause * 2)
            continue

        resp.raise_for_status()
        chunk = resp.json()
        if not chunk:
            break
        print(chunk)
        all_data.extend(chunk)
        page += 1
    print(all_data)
    return all_data

def lambda_handler(event, context):
    raw_data = fetch_all_pages()
    #raw = response.json()

    if not isinstance(raw_data, list):
            raise ValueError("Unexpected API format")

    fields = [
            "name", "current_price", "market_cap", "circulating_supply", "total_supply",
            "last_updated", "ath", "atl", "roi", "price_change_percentage_1h_in_currency",
            "price_change_percentage_24h_in_currency", "price_change_percentage_7d_in_currency",
            "total_volume", "high_24h", "low_24h"
        ]

    def extract(row):
            flat = {k: row.get(k, None) for k in fields if k != "roi"}
            flat["roi"] = row.get("roi", {}).get("percentage") if row.get("roi") else None
            return flat

    df = pd.DataFrame([extract(row) for row in raw_data])
    df["last_updated"] = pd.to_datetime(df["last_updated"], utc=True)
    df["inserted_at"] = pd.Timestamp.utcnow()
    df["ds"] = df["inserted_at"].dt.date.astype(str)

    table = pa.Table.from_pandas(df)
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    partition = df["ds"].iloc[0]
    timestamp = datetime.utcnow().isoformat()
    s3_key = f"{S3_PREFIX}/{partition}-snapshot.parquet"

    s3 = boto3.client("s3")
    s3.upload_fileobj(buffer, S3_BUCKET, s3_key)

    return {"status": "success", "rows": len(df), "s3_key": s3_key}

if __name__ == "__main__":
     lambda_handler(None,None)