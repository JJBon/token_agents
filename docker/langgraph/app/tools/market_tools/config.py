# config.py
import os

GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "news_agent")
TBL_NEWS     = os.environ.get("ICEBERG_TABLE_NEWS", "crpytoapi_news")
ATHENA_OUT   = os.environ["ATHENA_OUTPUT_S3"]
AWS_REGION   = os.environ.get("AWS_REGION", "us-east-1")
WORKGROUP    = os.environ.get("ATHENA_WORKGROUP", "primary")

S3_BUCKET = os.environ.get("S3_BUCKET")
S3_PREFIX = os.environ.get("S3_PREFIX", "news_agent")
WAREHOUSE = os.environ.get("ATHENA_WAREHOUSE_S3", f"s3://{S3_BUCKET}/{S3_PREFIX}/warehouse/")
SUMMARY_DAYS = int(os.environ.get("SUMMARY_DAYS", "3"))
WRITE_SUMMARY = os.environ.get("WRITE_SUMMARY", "false").lower() == "true"