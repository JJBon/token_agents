import pytest
import asyncio
from tests.utils.runner import load_scenario, run_scenario

@pytest.mark.asyncio
@pytest.mark.parametrize("scenario_file", ["basic_bitcoin.yaml"])
async def test_scenarios(scenario_file, graph):
    scenario = load_scenario(f"tests/scenarios/{scenario_file}")
    score = await run_scenario(scenario, simulate=True)
    assert score >= 0.8, f"Tool call accuracy too low for {scenario_file}"

@pytest.mark.asyncio
@pytest.mark.integration
async def test_scenario_integration(graph):
    scenario = load_scenario("tests/scenarios/basic_bitcoin.yaml")
    score = await run_scenario(scenario, simulate=False, graph=graph)
    assert score >= 0.8, "Integration test failed for Bitcoin scenario"
