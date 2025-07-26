import boto3
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import tempfile
import os
from pathlib import Path
from pyspark.sql import SparkSession

# CONFIGURATION
local_parquet_path = Path(".")
output_file_name = "consolidated_2025_07_17.parquet"
s3_bucket = os.environ["S3_DATA_BUCKET"]
s3_output_key = f"consolidated/{output_file_name}"
s3_output_uri = f"s3://{s3_bucket}/consolidated/"
glue_database = "agentic"
glue_table_name = "raw_data"

# AWS Clients
s3 = boto3.client("s3")
glue = boto3.client("glue")


def list_local_parquet_files(directory: Path):
    return list(directory.glob("*.parquet"))


def normalize_table(file_path: Path):
    table = pq.read_table(file_path)
    df = table.to_pandas()

    # Fix last_updated
    if "last_updated" in df.columns:
        if pd.api.types.is_integer_dtype(df["last_updated"]):
            df["last_updated"] = pd.to_datetime(df["last_updated"], unit="s")
        elif pd.api.types.is_datetime64_any_dtype(df["last_updated"]):
            if df["last_updated"].dt.tz is not None:
                df["last_updated"] = df["last_updated"].dt.tz_localize(None)
            df["last_updated"] = df["last_updated"].astype("datetime64[us]")

    # Fix inserted_at
    if "inserted_at" in df.columns and pd.api.types.is_datetime64_any_dtype(df["inserted_at"]):
        if df["inserted_at"].dt.tz is not None:
            df["inserted_at"] = df["inserted_at"].dt.tz_localize(None)
        df["inserted_at"] = df["inserted_at"].astype("datetime64[us]")

    return pa.Table.from_pandas(df)


def consolidate_tables(tables):
    return pa.concat_tables(tables, promote_options="default")


def write_consolidated_parquet(table: pa.Table, output_file: str):
    # Force a Spark-compatible schema
    schema = pa.schema([
        ("name", pa.string()),
        ("current_price", pa.float64()),
        ("market_cap", pa.int64()),
        ("circulating_supply", pa.float64()),
        ("total_supply", pa.float64()),
        ("last_updated", pa.timestamp("us")),
        ("ath", pa.float64()),
        ("atl", pa.float64()),
        ("price_change_percentage_1h_in_currency", pa.float64()),
        ("price_change_percentage_24h_in_currency", pa.float64()),
        ("price_change_percentage_7d_in_currency", pa.float64()),
        ("total_volume", pa.float64()),
        ("high_24h", pa.float64()),
        ("low_24h", pa.float64()),
        ("roi", pa.float64()),
        ("inserted_at", pa.timestamp("us")),
        ("ds", pa.string())
    ])

    df = table.to_pandas()
    clean_table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(clean_table, output_file, compression="snappy")
    print(f"✅ Wrote consolidated Parquet to {output_file}")


def upload_to_s3(local_path: str, bucket: str, key: str):
    s3.upload_file(local_path, bucket, key)
    print(f"✅ Uploaded to s3://{bucket}/{key}")


def create_glue_table(s3_location: str):
    columns = [
        {"Name": "name", "Type": "string"},
        {"Name": "current_price", "Type": "double"},
        {"Name": "market_cap", "Type": "bigint"},
        {"Name": "circulating_supply", "Type": "double"},
        {"Name": "total_supply", "Type": "double"},
        {"Name": "last_updated", "Type": "timestamp"},
        {"Name": "ath", "Type": "double"},
        {"Name": "atl", "Type": "double"},
        {"Name": "price_change_percentage_1h_in_currency", "Type": "double"},
        {"Name": "price_change_percentage_24h_in_currency", "Type": "double"},
        {"Name": "price_change_percentage_7d_in_currency", "Type": "double"},
        {"Name": "total_volume", "Type": "double"},
        {"Name": "high_24h", "Type": "double"},
        {"Name": "low_24h", "Type": "double"},
        {"Name": "roi", "Type": "double"},
        {"Name": "inserted_at", "Type": "timestamp"},
        {"Name": "ds", "Type": "string"}
    ]

    glue.create_table(
        DatabaseName=glue_database,
        TableInput={
            'Name': glue_table_name,
            'StorageDescriptor': {
                'Columns': columns,
                'Location': s3_location,
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                    'Parameters': {'serialization.format': '1'}
                },
                'Compressed': True,
                'StoredAsSubDirectories': False
            },
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': 'parquet',
                'compressionType': 'snappy',
                'typeOfData': 'file'
            }
        }
    )
    print(f"✅ Glue table '{glue_table_name}' created in database '{glue_database}'")


def validate_with_spark(local_parquet_file: str):
    print("🚦 Validating with Spark...")
    spark = SparkSession.builder.appName("CheckParquet").getOrCreate()
    df = spark.read.parquet(local_parquet_file)
    df.printSchema()
    df.show(5)
    spark.stop()
    print("✅ Spark validation complete.")


# ---------- MAIN ----------
if __name__ == "__main__":
    print(f"🔍 Looking for local Parquet files in {local_parquet_path.resolve()}")
    files = list_local_parquet_files(local_parquet_path)
    if not files:
        raise FileNotFoundError("❌ No local .parquet files found!")

    print("📥 Reading and normalizing each file...")
    tables = [normalize_table(f) for f in files]

    print("🔄 Consolidating tables...")
    combined = consolidate_tables(tables)

    print("💾 Writing local consolidated file...")
    write_consolidated_parquet(combined, output_file_name)

    print("🚦 Validating file with Spark before upload...")
    validate_with_spark(output_file_name)

    print("🚀 Uploading to S3...")
    upload_to_s3(output_file_name, s3_bucket, s3_output_key)

    print("📚 Creating Glue table...")
    create_glue_table(s3_output_uri)

    print("✅ Workflow complete.")
