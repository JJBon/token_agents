"""
title: Synthetic Jira Generator
author: You
description: OWUI pipeline that uses an injectable engine for synthetic Jira data.
required_open_webui_version: 0.4.3
version: 1.3.1
license: MIT
"""
from __future__ import annotations

import os, json, uuid, tempfile, importlib
from pathlib import Path
from typing import Any, Dict, List, Union, Iterator
from pydantic import BaseModel, Field


# default engine path (the core we just created)
DEFAULT_ENGINE = os.getenv("SYNTH_ENGINE", "synthcore.generator:Generator")
DEFAULT_SCENARIO = os.getenv("SYNTH_SCENARIO", "synthcore.scenario:Scenario")
OUTPUT_ROOT = os.getenv("SYNTH_OUTPUT_ROOT", "/generated")
OUTPUT_FORMAT = os.getenv("SYNTH_OUTPUT_FORMAT","parquet")
RETURN_ZIP = os.getenv("SYNTH_RETURN_ZIP","true").lower() not in ("0","false","no","off","")

def _load_symbol(path: str):
    """Import 'pkg.mod:Symbol' and return the symbol."""
    mod, sym = path.split(":")
    m = importlib.import_module(mod)
    return getattr(m, sym)

Engine = _load_symbol(DEFAULT_ENGINE)
Scenario = _load_symbol(DEFAULT_SCENARIO)

class Pipeline:
    class Valves(BaseModel):
        OUTPUT_ROOT: str = Field(default =OUTPUT_ROOT)
        OUTPUT_FORMAT: str = Field(default=OUTPUT_FORMAT)
        RETURN_ZIP: bool = Field(default=False)
        VERSION: str  = Field(default="1.3.1")

    def __init__(self, engine=None):
        # allow explicit injection for tests: Pipeline(engine=FakeEngine)
        self.engine = engine or Engine
        self.name = "Synthetic Jira Generator (DI)"
        self.valves = self.Valves()

    def pipe(self, user_message: str, model_id: str, messages: List[Dict[str, Any]], body: Dict[str, Any]) -> Union[str, Iterator[str]]:
        yield "<thinking>Generating synthetic Jira dataset…</thinking>\n"
        scenario = {}
        data_section = (body or {}).get("data") or {}
        if isinstance(data_section.get("scenario"), dict):
            scenario = data_section["scenario"]
        elif isinstance(body.get("scenario"), dict):
            scenario = body["scenario"]
        else:
            try:
                maybe = messages[-1].get("content","") if messages else ""
                if isinstance(maybe, str) and maybe.strip().startswith("{"):
                    tmp = json.loads(maybe)
                    scenario = tmp.get("scenario", tmp)
            except Exception:
                scenario = {}

        session_id = scenario.get("session_id") or str(uuid.uuid4())
        scen = Scenario(
            session_id=session_id,
            company_name=scenario.get("company_name","Acme"),
            industry=scenario.get("industry","Tech"),
            num_projects=int(scenario.get("num_projects",2)),
            users_per_project=int(scenario.get("users_per_project",12)),
            sprint_length_days=int(scenario.get("sprint_length_days",14)),
            sprint_count=int(scenario.get("sprint_count",6)),
            bug_ratio=float(scenario.get("bug_ratio",0.35)),
            feature_ratio=float(scenario.get("feature_ratio",0.55)),
            chore_ratio=float(scenario.get("chore_ratio",0.10)),
            avg_story_points=float(scenario.get("avg_story_points",5.0)),
            start_date=scenario.get("start_date")
        )
        fmt = (body.get("format") or scenario.get("format") or self.valves.OUTPUT_FORMAT).lower()
        if fmt not in ("parquet","csv","both"): fmt = "parquet"

        Path(self.valves.OUTPUT_ROOT).mkdir(parents=True, exist_ok=True)
        tmp_root = tempfile.mkdtemp(prefix="synth_jira_", dir=self.valves.OUTPUT_ROOT)
        out_dir = Path(tmp_root) / "dataset"

        dfs = self.engine.generate(scen)
        qa  = self.engine.validate(dfs)
        paths = self.engine.export(dfs, str(out_dir), fmt=fmt)

        zip_path = ""
        if self.valves.RETURN_ZIP:
            zip_path = str(Path(tmp_root) / f"{scen.session_id}.zip")
            self.engine.zip_dir(str(out_dir), zip_path)

        tables = {k: int(len(v)) for k, v in dfs.items()}
        summary = {"session_id": scen.session_id, "tables": tables, "paths": paths, "zip_path": zip_path, "qa": qa, "format": fmt, "version": self.valves.VERSION}

        yield "### ✅ Synthetic dataset generated\n"
        for k,v in tables.items(): yield f"- **{k}**: {v}\n"
        if zip_path: yield f"\n**ZIP**: `{zip_path}`\n"
        yield "\n```json\n" + json.dumps(summary, indent=2) + "\n```\n"
