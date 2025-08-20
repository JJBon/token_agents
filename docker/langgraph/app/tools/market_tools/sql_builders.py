# tools/market_tools/sql_builders.py
from __future__ import annotations
from typing import Iterable
from datetime import datetime

def esc(s: str) -> str:
    return s.replace("'", "''")

def ts(dt: datetime) -> str:
    # Athena TIMESTAMP 'YYYY-MM-DD HH:MM:SS'
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}'"

def arr_str(strs: Iterable[str]) -> str:
    inner = ", ".join(f"'{esc(x)}'" for x in strs)
    return f"ARRAY[{inner}]"
