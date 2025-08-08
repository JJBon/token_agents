import os, json, uuid, asyncio
from typing import Annotated, Any, List
from typing_extensions import TypedDict

import boto3
from pydantic import BaseModel, Field
from langchain_aws import BedrockLLM
from langchain_core.messages import HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import StructuredTool
from langchain_core.runnables import RunnableLambda, RunnableConfig
from langchain_aws import ChatBedrockConverse

from langchain_scrapegraph.tools import SmartScraperTool

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph, START
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition

# --- State ---
class State(TypedDict):
    messages: Annotated[List[Any], add_messages]

# --- Tool: scrape CryptoPanic ---
def scrape_cp() -> str:
    tool = SmartScraperTool()
    result = tool.invoke({
        "website_url": "https://cryptopanic.com/",
        "user_prompt": (
            "Extract the top 5 crypto news headlines and URLs from CryptoPanic homepage "
            "as JSON array of objects with keys 'title' and 'url'."
        )
    })
    return json.dumps(result)

scrape_cp_tool = StructuredTool.from_function(
    func=scrape_cp,
    name="scrape_cryptopanic",
    description="Scrape top 5 headlines & URLs from CryptoPanic homepage."
)

tools = [scrape_cp_tool]

# --- System Prompt ---
system_prompt = """
You are a news scraping agent.
Use scrape_cryptopanic() tool to get the latest headlines and URLs from CryptoPanic.
Return the JSON output directly, no extra text.
"""

# --- Build Agent Graph ---
def build_graph(config: RunnableConfig = None):
    config = config or {}
    model = config.get("model", "anthropic.claude-3-haiku-20240307-v1:0")
    provider = config.get("provider", "anthropic")
    temperature = config.get("temperature", 0.0)
    bedrock = boto3.client("bedrock-runtime", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    llm = ChatBedrockConverse(
        model=model, provider=provider, temperature=temperature, client=bedrock
    )
    llm = llm.bind_tools(tools)

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("placeholder", "{messages}")
    ])
    agent = prompt | llm

    async def agent_node(state: State, config: RunnableConfig):
        resp = await agent.ainvoke({"messages": state["messages"]}, config=config)
        return {"messages": [resp]}

    g = StateGraph(State)
    g.add_node("agent", agent_node)
    g.add_node("tools", ToolNode(tools=tools))
    g.set_entry_point("agent")
    g.add_conditional_edges("agent", tools_condition)
    g.add_edge("tools", "agent")

    return g.compile(checkpointer=MemorySaver())

# --- Runnable Interface ---
class CPArgs(BaseModel):
    user_request: str = Field(...)

class CPResult(BaseModel):
    output: Any

def to_state(args: CPArgs) -> State:
    return {"messages": [HumanMessage(content=args.user_request)]}

def from_state(state: State) -> CPResult:
    last = state["messages"][-1].content
    try:
        return CPResult(output=json.loads(last))
    except:
        return CPResult(output=last)

pipeline = (
    RunnableLambda(to_state)
    | build_graph()
    | RunnableLambda(from_state)
)

cp_agent_tool = pipeline.as_tool(
    args_schema=CPArgs,
    name="crypto_panic_scraper_agent",
    description="Use Bedrock to scrape CryptoPanic headlines via ScrapeGraph."
)

# --- CLI Test ---
if __name__ == "__main__":
    async def main():
        graph = build_graph()
        res = await graph.ainvoke(
            {"messages": [HumanMessage(content="Run CryptoPanic scraper")]},
            config={"thread_id": f"cp-agent-{uuid.uuid4()}", "tags": ["cp-agent"]}
        )
        print("Result:", res)
    asyncio.run(main())
