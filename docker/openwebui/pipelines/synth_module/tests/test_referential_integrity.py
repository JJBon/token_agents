from synthcore.scenario import Scenario
from synthcore.generator import Generator

def test_fk_integrity():
    s = Scenario(session_id="seed-2", company_name="Acme", industry="Tech")
    dfs = Generator.generate(s)
    qa = Generator.validate(dfs)
    assert qa["ok"]
    assert qa["checks"]["issues.project_key_fk"] == 1.0
    assert qa["checks"]["issues.assignee_user_id_fk"] == 1.0
