# graph_mcp.py
import os, json, asyncio, hashlib, boto3
from typing import Annotated, List, Any, Dict, Optional, Tuple, Union
from pydantic import BaseModel, Field

from langchain_core.messages import HumanMessage, AIMessage, BaseMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from langchain_openai import ChatOpenAI
from langchain_aws import ChatBedrockConverse

from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.tools import load_mcp_tools  # noqa: F401

# from langfuse.langchain import CallbackHandler
# lf_handler = CallbackHandler()

from prompts.prompts import query_agent_system_prompt


# ---------- helpers ----------
def _mcp_servers_from_env() -> Dict[str, Dict[str, Any]]:
    mode = os.getenv("MCP_MODE", "stdio").lower()
    if mode == "streamable_http":
        base = os.getenv("MCP_DBT_URL", "http://dbt-mcp:8001").rstrip("/")
        path = os.getenv("MCP_DBT_PATH", "/mcp")
        return {"dbt": {"url": f"{base}{path}", "transport": "streamable_http"}}
    return {
        "dbt": {
            "command": os.getenv("MCP_DBT_CMD", "python"),
            "args": [os.getenv("MCP_DBT_SCRIPT", "/app/tools/query_tools/mcp_tools.py")],
            "transport": "stdio",
            "cwd": os.getenv("MCP_DBT_CWD", "/app"),
            "env": dict(os.environ),
        }
    }

async def _load_all_mcp_tools():
    return await MultiServerMCPClient(_mcp_servers_from_env()).get_tools()

async def build_llm(model: str, temperature: float):
    mode = os.getenv("LLM_MODE", "bedrock").lower()
    if mode == "litellm":
        return ChatOpenAI(
            model=os.getenv("LITELLM_MODEL_NAME", "bedrock-claude-haiku"),
            base_url=os.getenv("LITELLM_BASE_URL"),
            api_key=os.getenv("LITELLM_API_KEY", "sk-noop"),
            temperature=temperature,
            timeout=60,
        )
    # Bedrock default
    client = await asyncio.to_thread(
        boto3.client, "bedrock-runtime", region_name=os.getenv("AWS_REGION", "us-east-1")
    )
    return ChatBedrockConverse(
        model=model,
        provider=os.getenv("BEDROCK_PROVIDER", "anthropic"),
        temperature=temperature,
        client=client,
    )

# ---------- state ----------
class State(dict):
    messages: Annotated[List[Any], add_messages]

# Minimal Bedrock hygiene: if user sends a new Human *immediately after*
# an AI tool_use (without tool_result yet), drop that trailing Human for this tick.
def _sanitize_for_bedrock(history: List[BaseMessage]) -> List[BaseMessage]:
    if not history:
        return history
    # find last AI with tool_calls
    idx = next((i for i in range(len(history)-1, -1, -1)
                if isinstance(history[i], AIMessage) and getattr(history[i], "tool_calls", None)), None)
    if idx is None:
        return history
    # if any Human appears after that AI before we see a tool message → drop last Human (one turn delay)
    for j in range(idx + 1, len(history)):
        m = history[j]
        if getattr(m, "type", "") == "tool":
            return history
        if isinstance(m, HumanMessage) and j == len(history) - 1:
            return history[:-1]
    return history

# ---------- graph ----------
async def build_graph(config: Optional[Dict] = None):
    cfg = config or {}
    llm = await build_llm(
        model=cfg.get("model", "anthropic.claude-3-haiku-20240307-v1:0"),
        temperature=cfg.get("temperature", 0.0),
    )
    tools = await _load_all_mcp_tools()

    tool_policy = """\
You have external tools via MCP. Prefer: fetch_metrics → create_query → fetch_query_result.
Avoid repeating the same tool call with the same arguments. Produce final answer clearly."""
    prompt = ChatPromptTemplate.from_messages([
        ("system", f"{query_agent_system_prompt.prompt}\n\n{tool_policy}"),
        ("placeholder", "{messages}"),
    ])

    llm_with_tools = llm.bind_tools(tools, tool_choice="auto")
    chain = prompt | llm_with_tools

    async def query_agent(state: State, config):
        history: List[BaseMessage] = state.get("messages") or []
        if not history or getattr(history[0], "type", "") != "human":
            history = [HumanMessage(content=" ")] + list(history)
        history = _sanitize_for_bedrock(history)
        resp = await chain.ainvoke({"messages": history}, config=config)
        return {"messages": [resp]}

    def to_tools_or_end(state: State):
        # route to tools only if latest AI proposed tool_calls
        for m in reversed(state.get("messages", [])):
            if isinstance(m, AIMessage):
                return "tools" if getattr(m, "tool_calls", None) else END
        return END

    memory = MemorySaver()
    g = StateGraph(State)
    g.add_node("query_agent", query_agent)
    g.add_node("tools", ToolNode(tools=tools))
    g.add_conditional_edges("query_agent", to_tools_or_end, {"tools": "tools", END: END})
    g.add_edge("tools", "query_agent")
    g.add_edge(START, "query_agent")

    graph = g.compile(checkpointer=memory)
    graph.name = "QueryAgentGraph"
    return graph

# ---------- (optional) thin wrapper tool ----------
class QueryGraphArgs(BaseModel):
    user_request: str = Field(...)

class QueryGraphResult(BaseModel):
    text: str = Field("")

def _to_state(args: Union[QueryGraphArgs, dict]) -> State:
    msg = args["user_request"] if isinstance(args, dict) else args.user_request
    return {"messages": [HumanMessage(content=msg)]}

def _from_state(st: State) -> QueryGraphResult:
    txt = st["messages"][-1].content if st.get("messages") else ""
    return QueryGraphResult(text=txt)

_pipeline = None
_query_graph_tool = None
_init_lock = asyncio.Lock()

async def make_pipeline(config: Optional[Dict] = None):
    graph = await build_graph(config)
    return RunnableLambda(_to_state) | graph | RunnableLambda(_from_state)

async def get_query_graph_tool(config: Optional[Dict] = None):
    global _pipeline, _query_graph_tool
    if _query_graph_tool is None:
        async with _init_lock:
            if _query_graph_tool is None:
                _pipeline = await make_pipeline(config)
                _query_graph_tool = _pipeline.as_tool(
                    args_schema=QueryGraphArgs,
                    name="run_query_agent_graph",
                    description="Runs the QueryAgentGraph with a single user message.",
                )
    return _query_graph_tool
