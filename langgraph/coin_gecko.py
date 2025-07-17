import asyncio
import boto3
import gradio as gr
import os
import logging
from typing import Annotated, TypedDict, List, Any
from dotenv import load_dotenv

from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.graph import StateGraph, START
from langchain_aws import ChatBedrockConverse
from langgraph.graph.message import add_messages
from langchain_core.messages import (
    ToolMessage,
    HumanMessage,
    BaseMessage,
)

# Enable verbose debugging
logging.basicConfig(level=logging.DEBUG)

load_dotenv(override=True)


class State(TypedDict):
    messages: Annotated[List[Any], add_messages]


def build_graph(tools, llm):
    llm_with_tools = llm.bind_tools(tools)

    async def chatbot(state: State):
        response = await llm_with_tools.ainvoke(state["messages"])
        return {"messages": [response]}

    async def tool_runner(state: State):
        node = ToolNode(tools=tools)
        result = await node.ainvoke(state)

        def coerce_to_tool_message(obj):
            if isinstance(obj, BaseMessage):
                return obj
            elif isinstance(obj, dict):
                if "messages" in obj:
                    return [coerce_to_tool_message(m) for m in obj["messages"]]
                required = ["content", "tool_call_id"]
                missing = [k for k in required if k not in obj]
                if missing:
                    raise ValueError(f"Tool result missing keys: {missing}. Got: {obj}")
                return ToolMessage(
                    content=obj["content"],
                    tool_call_id=obj["tool_call_id"],
                    name=obj.get("name")
                )
            else:
                raise ValueError(f"Invalid tool result format: {obj}")

        def flatten(messages):
            for m in messages:
                if isinstance(m, list):
                    yield from m
                else:
                    yield m

        if isinstance(result, list):
            messages = list(flatten([coerce_to_tool_message(r) for r in result]))
        else:
            messages = list(flatten([coerce_to_tool_message(result)]))

        # Resume Claude if last tool is fetch_query_result
        if messages and isinstance(messages[-1], ToolMessage):
            if messages[-1].name == "fetch_query_result":
                messages.append(HumanMessage(content="Please summarize the query result."))

        return {"messages": messages}

    builder = StateGraph(State)
    builder.add_node("chatbot", chatbot)
    builder.add_node("tools", tool_runner)
    builder.add_conditional_edges("chatbot", tools_condition, "tools")
    builder.add_edge("tools", "chatbot")
    builder.add_edge(START, "chatbot")

    return builder.compile()


async def main():
    client = MultiServerMCPClient({
        "dbt": {
            "command": "python",
            "args": [os.environ["MCP_SERVER_SCRIPT"]],
            "transport": "stdio",
            "env": {
                "DBT_PROJECT_PATH": os.environ["DBT_PROJECT_PATH"],
                "DBT_PROFILES_DIR": os.environ.get("DBT_PROFILES_DIR", ""),
            },
        }
    })

    dbt_tools = await client.get_tools(server_name="dbt")

    bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
    llm = ChatBedrockConverse(
        model="anthropic.claude-3-haiku-20240307-v1:0",
        provider="anthropic",
        temperature=0,
        max_tokens=None,
        client=bedrock,
        disable_streaming="tool_calling",
    )

    tools = dbt_tools
    graph = build_graph(tools, llm)

    async def gr_chat(message, history):
        try:
            result = await graph.ainvoke({
                "messages": [{"role": "user", "content": message}]
            })
            assistant = result["messages"][-1]
            history = history or []
            history.append({"role": "user", "content": message})
            history.append({"role": "assistant", "content": getattr(assistant, "content", str(assistant))})
            return history, "✅ Query successful"
        except Exception as e:
            return history, f"❌ Error: {str(e)}"

    with gr.Blocks() as demo:
        chatbot = gr.Chatbot(type="messages", label="CoinGecko + dbt Agent")
        txt = gr.Textbox(placeholder="Ask about crypto metrics or dbt metrics...", label="Your question")
        logbox = gr.Textbox(label="Debug Log", lines=4)

        txt.submit(gr_chat, [txt, chatbot], [chatbot, logbox])

    demo.launch()
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
