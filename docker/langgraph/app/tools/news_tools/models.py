# news_tools.py
from __future__ import annotations
import os, re, json, time, asyncio, logging
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests
from pydantic import BaseModel, Field, ValidationError
from langchain_core.tools import tool
from langchain_aws import ChatBedrockConverse

# vectors
from vectors.s3vectors_client import S3Vectors
from vectors.embeddings import embed_texts

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
VEC_BUCKET = os.getenv("VEC_BUCKET")
VEC_INDEX  = os.getenv("VEC_INDEX")

# ---------------- Types ----------------
class EnsureOut(BaseModel):
    ok: bool

class FetchIn(BaseModel):
    api_url: str
    timeout_s: int = 15

class Item(BaseModel):
    news_id: str
    title: Optional[str] = None
    text: Optional[str] = None
    source_name: Optional[str] = None
    news_url: Optional[str] = None
    date: Optional[str] = None
    sentiment: Optional[Any] = None
    currencies: List[Dict[str, Any]] = Field(default_factory=list)
    published_at_iso: Optional[str] = None
    # passthrough-any
    extra: Dict[str, Any] = Field(default_factory=dict)

class FetchOut(BaseModel):
    items: List[Dict[str, Any]]

class DedupeIn(BaseModel):
    items: List[Dict[str, Any]]

class DedupeOut(BaseModel):
    to_process: List[Dict[str, Any]]
    dedup_skipped: int

class ScrapeIn(BaseModel):
    url: str
    timeout_s: int = 15

class ScrapeOut(BaseModel):
    text: str

class TokenMention(BaseModel):
    name: str
    symbol: Optional[str] = None
    confidence: float = Field(ge=0, le=1)

class ExtractOneIn(BaseModel):
    title: str = ""
    source: str = ""
    url: str = ""
    body: str = ""

class ExtractOneOut(BaseModel):
    tokens: List[TokenMention] = Field(default_factory=list)

class IndexVectorsIn(BaseModel):
    rows: List[Dict[str, Any]]
    region: str = AWS_REGION
    bucket: str = VEC_BUCKET
    index: str  = VEC_INDEX

class IndexVectorsOut(BaseModel):
    chunks_indexed: int

class PersistBronzeIn(BaseModel):
    rows: List[Dict[str, Any]]
    extractor_temperature: float = 0.3

class PersistBronzeOut(BaseModel):
    count: int

class PersistIcebergIn(BaseModel):
    rows: List[Dict[str, Any]]
    extractor_temperature: float = 0.3

class PersistIcebergOut(BaseModel):
    count: int

