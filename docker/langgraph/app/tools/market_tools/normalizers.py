# tools/market_tools/normalizers.py
from __future__ import annotations
from typing import List
from datetime import datetime, timedelta, timezone
from tools.market_tools.write_models import CleanInsight
from tools.market_tools.models import NewInsight

def normalize_new_insights(
    insights: List[NewInsight],
    window_start_iso: str,
    window_end_iso: str,
    default_valid_days: int = 7,
) -> List[CleanInsight]:
    from pandas import to_datetime
    ws = to_datetime(window_start_iso, utc=True).to_pydatetime()
    we = to_datetime(window_end_iso, utc=True).to_pydatetime()
    now = datetime.now(timezone.utc)
    vus = now + timedelta(days=default_valid_days)

    cleaned: List[CleanInsight] = []
    for i, ni in enumerate(insights):
        # Build a default runs evidence if LLM forgot it
        ev = list(ni.evidence_refs or [])
        runs_ev = f"runs:{ws.strftime('%Y-%m-%dT%H:%M:%SZ')}..{we.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        if not any(x.startswith("runs:") for x in ev):
            ev.append(runs_ev)

        # minimal id if caller hasn't set one (uuid4 > timestamp)
        import uuid
        iid = f"ins_{uuid.uuid4().hex}"

        cleaned.append(
            CleanInsight(
                insight_id=iid,
                created_at=now,
                window_start=ws,
                window_end=we,
                title=ni.title,
                thesis=ni.thesis,
                tags=ni.tags or [],
                evidence_refs=ev,
                confidence=ni.confidence if ni.confidence is not None else 0.5,
                valid_until=vus,
                parent_ids=[],  # always a list, never '[]' string
                hit=None,
                hit_reason=None,
            )
        )
    return cleaned
