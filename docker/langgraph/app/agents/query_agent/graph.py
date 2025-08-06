import asyncio
import uuid
from typing import Annotated, TypedDict, List, Any, Union, Dict

import boto3
from pydantic import BaseModel, Field
from langchain_aws import ChatBedrockConverse
from langchain_core.messages import HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda, RunnableConfig

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph, START
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition

from prompts.prompts import query_agent_system_prompt
from tools.dbt_tools import (
    fetch_metrics_tool,
    create_query_tool,
    fetch_query_result_tool,
    search_dimension_values_tool,
)

# optional tracing handler

class State(TypedDict):
        messages: Annotated[List[Any], add_messages]

def build_graph(model="anthropic.claude-3-haiku-20240307-v1:0"
                ,provider="anthropic",temperature=0, lf_handler=None, tools = [
        fetch_metrics_tool,
        create_query_tool,
        fetch_query_result_tool,
        search_dimension_values_tool,
    ], system_prompt=query_agent_system_prompt.prompt):

# --- LLM + tools setup ---
    bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
    llm = ChatBedrockConverse(
        model=model,
        provider=provider,
        temperature=temperature,
        client=bedrock,
    )
  
    llm_with_tools = llm.bind_tools(tools)

    # --- Prompt ---
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", f"{system_prompt}"),
            ("placeholder", "{messages}"),
        ]
    )
    query_agent_chain = prompt | llm_with_tools

    # --- State ---


    # --- Node ---
    async def query_agent(state: State, config: RunnableConfig):
        response = await query_agent_chain.ainvoke(
            {"messages": state["messages"]},
            config=config,
        )
        return {"messages": [response]}

    # --- Graph assembly ---
    memory = MemorySaver()
    graph_builder = StateGraph(State)
    graph_builder.add_node("query_agent", query_agent)
    tool_node = ToolNode(tools=tools)
    graph_builder.add_node("tools", tool_node)
    graph_builder.add_conditional_edges("query_agent", tools_condition)
    graph_builder.add_edge("tools", "query_agent")  # loop after tool runs
    graph_builder.add_edge(START, "query_agent")
    graph = graph_builder.compile(checkpointer=memory)
    graph.name = "QueryAgentGraph"
    return graph


# --- Expose as tool with structured I/O ---
class QueryGraphArgs(BaseModel):
    user_request: str = Field(..., description="Natural-language metric request")
    retries: int = Field(0, ge=0, le=2, description="How many times the agent may loop")

class QueryGraphResult(BaseModel):
    data: str = Field("", description="Extracted data block (e.g., table or raw output)")
    insight: str = Field("", description="Narrative insight")
    tools_used: List[str] = Field(default_factory=list, description="Concrete tools that executed in order")
    raw_query_result: Union[Dict[str, Any], str, None] = Field(
        None, description="Unmodified fetch_query_result payload if available"
    )

def _to_state(args: Union[QueryGraphArgs, dict]) -> State:
    if isinstance(args, dict):
        user_request = args.get("user_request", "")
    else:
        user_request = args.user_request
    return {"messages": [HumanMessage(content=user_request)]}

def _from_state(st: State) -> QueryGraphResult:
    # Get last LLM / agent message content
    text = st["messages"][-1].content if st.get("messages") else ""
    lower = text.lower()

    # Extract labeled sections
    ds_idx = lower.find("data:")
    insight_idx = lower.find("insight:")

    data_block = ""
    insight_block = ""

    if ds_idx != -1 and insight_idx != -1 and insight_idx > ds_idx:
        data_block = text[ds_idx + len("data:"):insight_idx].strip()
        insight_block = text[insight_idx + len("insight:"):].strip()
    elif ds_idx != -1:
        data_block = text[ds_idx + len("data:"):].strip()
    elif insight_idx != -1:
        insight_block = text[insight_idx + len("insight:"):].strip()
    else:
        # fallback: treat entire content as data if no explicit labels
        data_block = text.strip()

    # Collect which tools actually ran by inspecting tool messages in the state.
    tools_used = []
    raw_query_result = None
    for m in st["messages"]:
        # ToolNode inserts ToolMessage-like objects; here heuristically look at name/content
        name = getattr(m, "name", "")
        if name in {"fetch_metrics", "create_query", "fetch_query_result", "search_dimension_values"}:
            tools_used.append(name)
        # Attempt to capture raw fetch_query_result output from tool result in the trace
        if name == "fetch_query_result":
            try:
                # sometimes content is a dict-like or JSON string
                content = m.content
                if isinstance(content, str):
                    # attempt parse
                    import json as _json

                    try:
                        parsed = _json.loads(content)
                        raw_query_result = parsed
                    except Exception:
                        raw_query_result = content
                else:
                    raw_query_result = content
            except Exception:
                pass

    # Deduplicate order-preserving tools_used
    seen = set()
    ordered_tools = []
    for t in tools_used:
        if t not in seen:
            ordered_tools.append(t)
            seen.add(t)

    return QueryGraphResult(
        data=data_block,
        insight=insight_block,
        tools_used=ordered_tools,
        raw_query_result=raw_query_result,
    )

pipeline = RunnableLambda(_to_state) | build_graph() | RunnableLambda(_from_state)
query_graph_tool = pipeline.as_tool(
    args_schema=QueryGraphArgs,
    name="run_query_agent_graph",
    description="Runs the QueryAgentGraph and returns {data, insight, tools_used, raw_query_result}.",
)
