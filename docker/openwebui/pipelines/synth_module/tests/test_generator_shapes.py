import pandas as pd
from synthcore.scenario import Scenario
from synthcore.generator import Generator

def test_basic_shapes():
    s = Scenario(session_id="seed-1", company_name="Acme", industry="Tech")
    dfs = Generator.generate(s)
    assert set(dfs.keys()) == {"users","projects","boards","sprints","epics","issues","transitions","comments","worklogs"}
    for df in dfs.values():
        assert isinstance(df, pd.DataFrame)
