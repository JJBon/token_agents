import pytest


class DummyGraph:
    async def ainvoke(self, *args, **kwargs):  # pragma: no cover - simple stub
        return {}


@pytest.fixture
def graph():
    """Return a minimal graph stub for tests that require a graph fixture."""
    return DummyGraph()
