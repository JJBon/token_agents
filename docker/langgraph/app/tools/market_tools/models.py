from pydantic import BaseModel, Field, ValidationError
from pydantic import field_validator
from typing import List, Optional, Dict, Any, Tuple, Literal
import json

class AssetBrief(BaseModel):
    symbol: Optional[str] = None
    name: Optional[str] = None
    stance: Literal["bullish", "bearish", "neutral"] = "neutral"
    rationale: str = ""

class NewInsight(BaseModel):
    title: str
    thesis: str
    tags: List[str] = []
    evidence_refs: List[str] = []   # e.g., ["runs:2025-08-12..2025-08-15","sql:qid=abc123"]
    confidence: float = 0.5

class MarketSummary(BaseModel):
    time_window: str
    sample_size: int
    pos: int
    neg: int
    neu: int
    net_score: float
    outlook: Literal["bullish", "bearish", "neutral"] = "neutral"
    confidence: float = 0.5
    drivers: List[str] = Field(default_factory=list)
    risks: List[str] = Field(default_factory=list)
    top_assets: List[AssetBrief] = Field(default_factory=list)
    narrative: str = ""
    new_insights: List[NewInsight] = Field(default_factory=list)

    @field_validator("drivers", "risks", mode="before")
    @classmethod
    def _coerce_str_list(cls, v):
        if v is None:
            return []
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            v = v.strip()
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                return [v]
        return v

    @field_validator("top_assets", mode="before")
    @classmethod
    def _coerce_assets(cls, v):
        if v is None:
            return []
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            v = v.strip()
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                return []
        return v