from typing import Annotated, TypedDict, List, Any, Union

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

from tools.news_tools import fetch_crypto_news_tool


class State(TypedDict):
    messages: Annotated[List[Any], add_messages]


def build_graph(
    model: str = "anthropic.claude-3-haiku-20240307-v1:0",
    provider: str = "anthropic",
    temperature: float = 0,
    tools = [fetch_crypto_news_tool],
    system_prompt: str = (
        "You are a news agent that finds recent crypto-related news for user queries. "
        "Use available tools to gather information and present concise summaries."
    ),
):
    bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
    llm = ChatBedrockConverse(
        model=model,
        provider=provider,
        temperature=temperature,
        client=bedrock,
    )

    llm_with_tools = llm.bind_tools(tools)
    prompt = ChatPromptTemplate.from_messages([
        ("system", f"{system_prompt}"),
        ("placeholder", "{messages}"),
    ])
    news_agent_chain = prompt | llm_with_tools

    async def news_agent(state: State, config: RunnableConfig):
        response = await news_agent_chain.ainvoke({"messages": state["messages"]}, config=config)
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
    query: str = Field(..., description="Topic for crypto news search")
    retries: int = Field(0, ge=0, le=2, description="How many times the agent may loop")


class NewsGraphResult(BaseModel):
    articles: str = Field("", description="News articles or summary")
    tools_used: List[str] = Field(default_factory=list, description="Tools executed in order")


def _to_state(args: Union[NewsGraphArgs, dict]) -> State:
    if isinstance(args, dict):
        user_query = args.get("query", "")
    else:
        user_query = args.query
    return {"messages": [HumanMessage(content=user_query)]}


def _from_state(st: State) -> NewsGraphResult:
    text = st["messages"][-1].content if st.get("messages") else ""
    tools_used = []
    for m in st["messages"]:
        name = getattr(m, "name", "")
        if name == "fetch_crypto_news":
            tools_used.append(name)
    seen = set()
    ordered_tools = []
    for t in tools_used:
        if t not in seen:
            ordered_tools.append(t)
            seen.add(t)
    return NewsGraphResult(articles=text, tools_used=ordered_tools)


pipeline = RunnableLambda(_to_state) | build_graph() | RunnableLambda(_from_state)
news_graph_tool = pipeline.as_tool(
    args_schema=NewsGraphArgs,
    name="run_news_agent_graph",
    description="Runs the NewsAgentGraph and returns crypto news articles using tools.",
)
