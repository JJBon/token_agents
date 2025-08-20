# schemas.py
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional

class RawMention(BaseModel):
    name: str = Field(..., description="Human-readable name in article text")
    symbol: Optional[str] = Field(None, description="Ticker if known")

class NormalizedMention(BaseModel):
    name: str
    symbol: str
    confidence: float = Field(ge=0, le=1)

class NormalizeRequest(BaseModel):
    mentions: List[RawMention]

class NormalizeResponse(BaseModel):
    mentions: List[NormalizedMention]
