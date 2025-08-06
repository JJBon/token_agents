import json
from typing import Annotated, List, Any, TypedDict, Dict, Union

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

from tools.news_tools import fetch_crypto_news_tool, crypto_news_trends_tool


class State(TypedDict):
    messages: Annotated[List[Any], add_messages]


def build_graph(
    model: str = "anthropic.claude-3-haiku-20240307-v1:0",
    provider: str = "anthropic",
    temperature: float = 0,
    lf_handler=None,
    tools=None,
    system_prompt: str = "You are a crypto news agent that surfaces trending tokens with insights and links.",
    llm=None,
):
    """Build the NewsAgent graph."""
    if tools is None:
        tools = [fetch_crypto_news_tool, crypto_news_trends_tool]

    if llm is None:
        bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
        llm = ChatBedrockConverse(
            model=model,
            provider=provider,
            temperature=temperature,
            client=bedrock,
        )

    llm_with_tools = llm.bind_tools(tools)

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_prompt),
            ("placeholder", "{messages}"),
        ]
    )

    agent_chain = prompt | llm_with_tools

    async def news_agent(state: State, config: RunnableConfig):
        response = await agent_chain.ainvoke({"messages": state["messages"]}, config=config)
        return {"messages": [response]}

    memory = MemorySaver()
    graph_builder = StateGraph(State)
    graph_builder.add_node("news_agent", news_agent)
    tool_node = ToolNode(tools=tools)
    graph_builder.add_node("tools", tool_node)
    graph_builder.add_conditional_edges("news_agent", tools_condition)
    graph_builder.add_edge("tools", "news_agent")
    graph_builder.add_edge(START, "news_agent")
    graph = graph_builder.compile(checkpointer=memory)
    graph.name = "NewsAgentGraph"
    return graph


class NewsGraphArgs(BaseModel):
    user_request: str = Field(..., description="News request")


class NewsGraphResult(BaseModel):
    tokens: List[Dict[str, Any]] = Field(default_factory=list, description="Trending token insights")
    tools_used: List[str] = Field(default_factory=list, description="Tools executed in order")


def _to_state(args: Union[NewsGraphArgs, Dict[str, Any]]) -> State:
    if isinstance(args, dict):
        req = args.get("user_request", "")
    else:
        req = args.user_request
    return {"messages": [HumanMessage(content=req)]}


def _from_state(st: State) -> NewsGraphResult:
    tokens: List[Dict[str, Any]] = []
    tools_used: List[str] = []
    for m in st["messages"]:
        name = getattr(m, "name", "")
        if name in {"fetch_crypto_news", "crypto_news_trends"}:
            tools_used.append(name)
        if name == "crypto_news_trends":
            content = m.content
            if isinstance(content, str):
                try:
                    tokens = json.loads(content)
                except Exception:
                    tokens = []
            else:
                tokens = content
    ordered = []
    for t in tools_used:
        if t not in ordered:
            ordered.append(t)
    return NewsGraphResult(tokens=tokens, tools_used=ordered)


pipeline = RunnableLambda(_to_state) | build_graph() | RunnableLambda(_from_state)
news_graph_tool = pipeline.as_tool(
    args_schema=NewsGraphArgs,
    name="run_news_agent_graph",
    description="Runs the NewsAgentGraph and returns trending token insights.",
)
