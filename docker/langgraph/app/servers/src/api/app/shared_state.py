# servers/src/api/app/shared_state.py
from typing import Optional
from langgraph.graph.state import CompiledStateGraph

dataframe_assistant_graph: Optional[CompiledStateGraph] = None


async def _build_graph_once() -> CompiledStateGraph:
    """Internal helper to build the graph exactly once."""
    global dataframe_assistant_graph

    # If already initialized, just return it
    if dataframe_assistant_graph is not None:
        return dataframe_assistant_graph

    #from agents.query_agent.graph import build_graph
    from agents.conversation_agent.graph import build_graph 
    #from agents.supervisor_agent.graph import graph

    # If build_graph is *sync*, drop the await:
    dataframe_assistant_graph = build_graph()
    #dataframe_assistant_graph = graph
    return dataframe_assistant_graph


async def initialize_assistant_graph():
    """Initialize the assistant graph during app startup."""
    # Just call the same helper; if something goes wrong, you'll see it in startup logs.
    await _build_graph_once()


async def get_dataframe_assistant_graph() -> CompiledStateGraph:
    """
    Get the initialized dataframe assistant graph.
    If it's not initialized yet (startup didn't run or failed),
    build it lazily on first request.
    """
    return await _build_graph_once()
