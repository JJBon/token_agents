# tools/market_tools/write_models.py
from __future__ import annotations
from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import List, Optional, Literal
from datetime import datetime, timezone

def _to_utc_naive(ts: datetime) -> datetime:
    # Athena TIMESTAMP is timezone-naive; store UTC without tzinfo
    if ts.tzinfo is None:
        return ts
    return ts.astimezone(timezone.utc).replace(tzinfo=None)

class CleanInsight(BaseModel):
    insight_id: str
    created_at: datetime
    window_start: datetime
    window_end: datetime
    title: str
    thesis: str
    tags: List[str] = Field(default_factory=list)
    evidence_refs: List[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    valid_until: datetime
    parent_ids: List[str] = Field(default_factory=list)
    hit: Optional[bool] = None
    hit_reason: Optional[str] = None

    @field_validator("created_at","window_start","window_end","valid_until", mode="before")
    @classmethod
    def _parse_ts(cls, v):
        if isinstance(v, str):
            # Let pandas-like parser handle variety, or write your own
            from datetime import datetime
            from pandas import to_datetime
            dt = to_datetime(v, utc=True).to_pydatetime()
            return dt
        return v

    @field_validator("created_at","window_start","window_end","valid_until")
    @classmethod
    def _utc_naive(cls, v: datetime):
        return _to_utc_naive(v)

    @field_validator("tags", "evidence_refs", "parent_ids", mode="before")
    @classmethod
    def _coerce_list(cls, v):
        if v is None: return []
        if isinstance(v, list): return [str(x) for x in v]
        s = str(v).strip()
        if s == "" or s.lower() in ("null","nan","none"): return []
        if s.startswith("[") and s.endswith("]"):
            import json
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return [str(x) for x in arr]
            except Exception:
                pass
            inner = s[1:-1]
            return [p.strip().strip('"').strip("'") for p in inner.split(",") if p.strip()]
        return [p.strip() for p in s.split(",") if p.strip()]

    @field_validator("title","thesis")
    @classmethod
    def _non_empty(cls, v: str):
        if not v or not str(v).strip():
            raise ValueError("required non-empty")
        return v.strip()

    @field_validator("evidence_refs")
    @classmethod
    def _evidence_contract(cls, v: list[str]):
        if not v:
            raise ValueError("evidence_refs cannot be empty")
        ok = any(x.startswith("runs:") for x in v) or any("sql:qid=" in x for x in v)
        if not ok:
            raise ValueError("evidence_refs must include 'runs:…' or 'sql:qid=…'")
        return v

class SummaryWriteRow(BaseModel):
    run_ts: datetime
    window_start: datetime
    window_end: datetime
    days: int
    sample_size: int
    pos: int
    neg: int
    neu: int
    net_score: float
    outlook: Literal["bullish","bearish","neutral"]
    confidence: float
    drivers: List[str] = Field(default_factory=list)
    risks: List[str] = Field(default_factory=list)
    top_assets: List[str] = Field(default_factory=list)  # "sym,name,stance" strings
    narrative: str
    new_insights_json: Optional[str] = None

    @field_validator("run_ts","window_start","window_end", mode="before")
    @classmethod
    def _parse_ts(cls, v):
        from pandas import to_datetime
        return to_datetime(v, utc=True).to_pydatetime().replace(tzinfo=None)
