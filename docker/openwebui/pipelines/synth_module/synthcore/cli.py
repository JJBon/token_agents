import json, os, tempfile
from pathlib import Path
from .scenario import Scenario
from .generator import Generator

def main():
    # quick defaults for local test
    scen = Scenario(
        session_id=os.getenv("SESSION_ID","dev-local"),
        company_name=os.getenv("COMPANY","Acme"),
        industry=os.getenv("INDUSTRY","Tech"),
        num_projects=int(os.getenv("NUM_PROJECTS","2")),
        users_per_project=int(os.getenv("USERS_PER_PROJECT","8")),
        sprint_length_days=int(os.getenv("SPRINT_LEN","14")),
        sprint_count=int(os.getenv("SPRINTS","5")),
        bug_ratio=float(os.getenv("BUG","0.35")),
        feature_ratio=float(os.getenv("FEAT","0.55")),
        chore_ratio=float(os.getenv("CHORE","0.10")),
        avg_story_points=float(os.getenv("SP","5")),
        start_date=os.getenv("START_DATE"),  # optional ISO
    )
    out_root = os.getenv("OUT","/tmp")
    tmp_root = tempfile.mkdtemp(prefix="synth_jira_", dir=out_root)
    out_dir = Path(tmp_root) / "dataset"

    dfs  = Generator.generate(scen)
    qa   = Generator.validate(dfs)
    paths= Generator.export(dfs, str(out_dir), fmt=os.getenv("FMT","parquet"))
    zipf = str(Path(tmp_root) / f"{scen.session_id}.zip")
    Generator.zip_dir(str(out_dir), zipf)

    print(json.dumps({
        "session_id": scen.session_id,
        "tables": {k:len(v) for k,v in dfs.items()},
        "qa": qa,
        "paths": paths,
        "zip_path": zipf
    }, indent=2))

if __name__ == "__main__":
    main()
