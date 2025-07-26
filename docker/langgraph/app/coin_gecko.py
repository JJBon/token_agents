import os
import json
import logging
import asyncio
import uuid
import boto3
import gradio as gr

from typing import Annotated, TypedDict, List, Any
from dotenv import load_dotenv

from langchain_core.messages import ToolMessage, HumanMessage, BaseMessage
from langchain_aws import ChatBedrockConverse
from langgraph.graph import StateGraph, START
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition

from langfuse import get_client
from langfuse.langchain import CallbackHandler

# Import dbt-tools
from tools.dbt_tools import (
    fetch_metrics_tool,
    create_query_tool,
    fetch_query_result_tool,
)

# -----------------------
# Logging & Env Setup
# -----------------------
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
load_dotenv()

lf = get_client()  # uses env vars
lf_handler = CallbackHandler()

tools = [fetch_metrics_tool, create_query_tool, fetch_query_result_tool]

# -----------------------
# State and Graph
# -----------------------
class State(TypedDict):
    messages: Annotated[List[Any], add_messages]


def build_graph(tools, llm):
    llm_with_tools = llm.bind_tools(tools)

    async def chatbot_node(state: State):
        logging.debug("LLM turn with messages: %s", state["messages"])
        response = await llm_with_tools.ainvoke(
            state["messages"]
        )
        return {"messages": [response]}

    async def tool_runner(state: State):
        logging.debug("Tool runner with state: %s", state)
        node = ToolNode(tools=tools)
        result = await node.ainvoke(state, config={"configurable":{}})
        raw_msgs = result.get("messages", [])

        msg_objs = []
        for obj in raw_msgs:
            if isinstance(obj, BaseMessage):
                msg_objs.append(obj)
            elif isinstance(obj, dict) and "content" in obj and "tool_call_id" in obj:
                msg_objs.append(ToolMessage(content=obj["content"], tool_call_id=obj["tool_call_id"], name=obj.get("name")))
            else:
                raise ValueError(f"Invalid tool output: {obj}")

        # Queue fetch_query_result after create_query success
        if msg_objs and isinstance(msg_objs[-1], ToolMessage) and msg_objs[-1].name == "create_query":
            try:
                data = json.loads(msg_objs[-1].content)
                if data.get("status") == "CREATED":
                    args = data["query"]
                    logging.debug("Queueing fetch_query_result tool call with query: %s", args)
                    # Ask the LLM to call the fetch_query_result tool next
                    msg_objs.append(HumanMessage(content=f"Run fetch_query_result with: {json.dumps(args)}"))
            except Exception as e:
                logging.error("Failed to queue fetch_query_result: %s", e)

        return {"messages": msg_objs}

    builder = StateGraph(State)
    builder.add_node("chatbot", chatbot_node)
    builder.add_node("tools", tool_runner)
    builder.add_conditional_edges("chatbot", tools_condition, "tools")
    builder.add_edge("tools", "chatbot")
    builder.add_edge(START, "chatbot")
    return builder.compile()

# -----------------------
# Gradio Format Helper
# -----------------------
def to_gradio_format(history):
    """Ensure history is a list of {role, content} dicts."""
    formatted = []
    for m in history:
        if isinstance(m, dict) and "role" in m and "content" in m:
            formatted.append(m)
        else:
            formatted.append({
                "role": getattr(m, "role", "assistant"),
                "content": getattr(m, "content", str(m))
            })
    return formatted

# -----------------------
# Main async entrypoint
# -----------------------
async def main():
    bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
    llm = ChatBedrockConverse(
        model="anthropic.claude-3-haiku-20240307-v1:0",
        provider="anthropic",
        temperature=0,
        client=bedrock,
        disable_streaming="tool_calling",
    ).with_config({"callbacks": [lf_handler]})

    graph = build_graph(tools, llm)

    async def gr_chat(message, history):
        try:
            result = await graph.ainvoke({
                "messages": [{"role": "user", "content": message},
                             {"role": "system", "content": "You are a DBT agent. First call fetch_metrics tool, then create_query, then fetch_query_result tools. restrict total number of tool call to 5 times"}
                             ],
            },config={"callbacks":[lf_handler]})
            assistant = result["messages"][-1]
            history = history or []
            history.append({"role": "user", "content": message})
            history.append({"role": "assistant", "content": getattr(assistant, "content", str(assistant))})
            return to_gradio_format(history), "✅ Query successful"
        except Exception as e:
            logging.exception("Error in chat")
            return history, f"❌ Error: {e}"

    with gr.Blocks() as demo:
        chatbot_ui = gr.Chatbot(type="messages", label="DBT Agent")
        txt = gr.Textbox(placeholder="Ask...", label="Your question")
        logbox = gr.Textbox(label="Debug Log", lines=4)
        txt.submit(gr_chat, [txt, chatbot_ui], [chatbot_ui, logbox])

    demo.launch(server_name="0.0.0.0", server_port=7860)


if __name__ == "__main__":
    asyncio.run(main())
