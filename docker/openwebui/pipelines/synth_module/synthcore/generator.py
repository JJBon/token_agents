from __future__ import annotations
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict
import os, zipfile
import numpy as np
import pandas as pd

from .scenario import Scenario

def _seed(session_id: str) -> None:
    # stable seed from session id
    np.random.seed(abs(hash(session_id)) % (2**32 - 1))

def _iso(t: datetime) -> str:
    return t.isoformat()

class Generator:
    @staticmethod
    def generate(s: Scenario) -> dict[str, pd.DataFrame]:
        _seed(s.session_id)
        start = (
            datetime.fromisoformat(s.start_date)
            if s.start_date
            else datetime.now() - timedelta(days=90)
        )

        users, projects, boards, sprints, epics = [], [], [], [], []
        issues, trans, comm, logs = [], [], [], []

        # Users
        uid = 1
        domain = s.company_name.lower().replace(" ", "")
        for _p in range(s.num_projects):
            for _ in range(s.users_per_project):
                users.append({
                    "user_id": f"U{uid:05d}",
                    "display_name": f"User {uid}",
                    "email": f"user{uid}@{domain}.com",
                    "role": np.random.choice(["Engineer", "QA", "PM", "Designer", "Data"]),
                    "active": True
                })
                uid += 1

        # Projects, Boards, Sprints
        for pid in range(1, s.num_projects + 1):
            pkey = f"P{pid}"
            projects.append({"project_key": pkey, "name": f"{s.company_name} Project {pid}"})
            boards.append({"board_id": f"B{pid}", "project_key": pkey, "type": "scrum"})
            t0 = start
            for si in range(1, s.sprint_count + 1):
                t1 = t0 + timedelta(days=s.sprint_length_days - 1)
                sprints.append({
                    "sprint_id": f"S{pid}-{si}",
                    "board_id": f"B{pid}",
                    "project_key": pkey,
                    "name": f"Sprint {si}",
                    "state": "closed",
                    "start_date": _iso(t0),
                    "end_date": _iso(t1),
                })
                t0 = t1 + timedelta(days=1)

        # Epics
        epic_id = 1
        for pr in projects:
            for _ in range(max(2, int(np.random.poisson(4)))):
                epics.append({
                    "epic_key": f"{pr['project_key']}-E{epic_id}",
                    "project_key": pr["project_key"],
                    "summary": f"Epic {epic_id}",
                    "created_at": _iso(start),
                })
                epic_id += 1

        # Issues + events
        iid = 1
        mix = (
            ["Bug"] * int(s.bug_ratio * 100)
            + ["Story"] * int(s.feature_ratio * 100)
            + ["Chore"] * int(s.chore_ratio * 100)
        )
        all_users = [u["user_id"] for u in users]

        for pr in projects:
            pkey = pr["project_key"]
            ps = [sp for sp in sprints if sp["project_key"] == pkey]
            total = max(10, int(s.users_per_project * s.sprint_count * np.random.uniform(0.8, 1.2)))
            ep_p = [e for e in epics if e["project_key"] == pkey]

            for _ in range(total):
                sp = np.random.choice(ps)
                created = datetime.fromisoformat(sp["start_date"]) + timedelta(
                    days=np.random.randint(0, max(1, s.sprint_length_days - 2))
                )
                status = np.random.choice(
                    ["To Do", "In Progress", "In Review", "Done"],
                    p=[0.25, 0.35, 0.20, 0.20]
                )
                resolved = (
                    created + timedelta(days=np.random.randint(1, max(2, s.sprint_length_days // 2)))
                    if status == "Done" else None
                )
                epic = np.random.choice(ep_p)["epic_key"] if ep_p else None
                assignee = np.random.choice(all_users)

                ik = f"{pkey}-{iid}"
                issues.append({
                    "issue_key": ik,
                    "project_key": pkey,
                    "type": np.random.choice(mix),
                    "summary": f"Item {iid}",
                    "story_points": max(1, int(np.random.normal(s.avg_story_points, 2))),
                    "status": status,
                    "epic_key": epic,
                    "assignee_user_id": assignee,
                    "sprint_id": sp["sprint_id"],
                    "created_at": _iso(created),
                    "resolved_at": _iso(resolved) if resolved else None,
                })

                flow = ["To Do", "In Progress", "In Review", "Done"]
                cur = "To Do"
                for nxt in flow[1:]:
                    ts = created + timedelta(days=np.random.randint(0, max(1, s.sprint_length_days - 1)))
                    trans.append({"issue_key": ik, "from_status": cur, "to_status": nxt, "at": _iso(ts)})
                    cur = nxt
                    if nxt == status:
                        break

                for _c in range(np.random.randint(0, 3)):
                    comm.append({
                        "issue_key": ik, "user_id": assignee, "body": "LGTM",
                        "at": _iso(created + timedelta(hours=np.random.randint(1, 72)))
                    })
                for _w in range(np.random.randint(0, 3)):
                    logs.append({
                        "issue_key": ik, "user_id": assignee, "hours": round(np.random.uniform(0.5, 6.0), 1),
                        "at": _iso(created + timedelta(hours=np.random.randint(2, 96)))
                    })
                iid += 1

        dfs = {
            "users": pd.DataFrame(users).drop_duplicates("user_id"),
            "projects": pd.DataFrame(projects).drop_duplicates("project_key"),
            "boards": pd.DataFrame(boards).drop_duplicates("board_id"),
            "sprints": pd.DataFrame(sprints).drop_duplicates("sprint_id"),
            "epics": pd.DataFrame(epics).drop_duplicates("epic_key"),
            "issues": pd.DataFrame(issues).drop_duplicates("issue_key"),
            "transitions": pd.DataFrame(trans),
            "comments": pd.DataFrame(comm),
            "worklogs": pd.DataFrame(logs),
        }
        return dfs  # <--- IMPORTANT

    @staticmethod
    def validate(dfs: dict[str, pd.DataFrame]) -> Dict[str, Any]:
        qa: Dict[str, Any] = {"checks": {}, "errors": []}

        def cov(child, col, parent, pk):
            if len(dfs[child]) == 0:
                return 1.0
            return float(dfs[child][col].isin(dfs[parent][pk]).mean())

        qa["checks"]["issues.project_key_fk"] = cov("issues", "project_key", "projects", "project_key")
        qa["checks"]["issues.assignee_user_id_fk"] = cov("issues", "assignee_user_id", "users", "user_id")

        # --- fixed timestamp sanity: compare per-row, not by position ---
        ts_ok = True
        issues = dfs.get("issues")
        if issues is not None and not issues.empty:
            # Work on rows that actually have a resolved timestamp
            dt = issues.dropna(subset=["resolved_at"]).copy()
            if not dt.empty:
                dt["resolved_at_dt"] = pd.to_datetime(dt["resolved_at"], errors="coerce")
                dt["created_at_dt"]  = pd.to_datetime(dt["created_at"],  errors="coerce")
                # Only keep rows we could parse
                dt = dt.dropna(subset=["resolved_at_dt", "created_at_dt"])
                ts_ok = bool((dt["resolved_at_dt"] >= dt["created_at_dt"]).all())

        qa["checks"]["issues.resolved_after_created"] = ts_ok

        qa["ok"] = all([
            qa["checks"]["issues.project_key_fk"] == 1.0,
            qa["checks"]["issues.assignee_user_id_fk"] == 1.0,
            ts_ok,
        ])
        return qa

    @staticmethod
    def export(dfs: dict[str, pd.DataFrame], base_dir: str, fmt: str = "parquet") -> dict[str, str]:
        base = Path(base_dir)
        base.mkdir(parents=True, exist_ok=True)
        paths: dict[str, str] = {}
        for name, df in dfs.items():
            if fmt in ("parquet", "both"):
                p = base / f"{name}.parquet"
                df.to_parquet(p, index=False)
                paths[name] = str(p)
            if fmt in ("csv", "both"):
                c = base / f"{name}.csv"
                df.to_csv(c, index=False)
                paths[name + ("_csv" if fmt == "both" else "")] = str(c)
        return paths  # <--- IMPORTANT

    @staticmethod
    def zip_dir(src_dir: str, zip_path: str) -> None:
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
            for root, _, files in os.walk(src_dir):
                for f in files:
                    full = Path(root) / f
                    z.write(full, arcname=str(Path(root).relative_to(src_dir) / f))
