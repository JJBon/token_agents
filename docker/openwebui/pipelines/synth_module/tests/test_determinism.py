from synthcore.scenario import Scenario
from synthcore.generator import Generator

def test_deterministic_seed():
    s1 = Scenario(session_id="same", company_name="Acme", industry="Tech")
    s2 = Scenario(session_id="same", company_name="Acme", industry="Tech")
    dfs1 = Generator.generate(s1)
    dfs2 = Generator.generate(s2)
    assert dfs1["users"].equals(dfs2["users"])
    assert dfs1["projects"].equals(dfs2["projects"])
