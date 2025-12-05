from dataclasses import dataclass

@dataclass
class Scenario:
    session_id: str
    company_name: str
    industry: str
    num_projects: int = 2
    users_per_project: int = 12
    sprint_length_days: int = 14
    sprint_count: int = 6
    bug_ratio: float = 0.35
    feature_ratio: float = 0.55
    chore_ratio: float = 0.10
    avg_story_points: float = 5.0
    start_date: str | None = None  # ISO string
