import os
import json
import logging
import asyncio
import uuid
import boto3
import gradio as gr

from typing import Annotated, TypedDict, List, Any
from dotenv import load_dotenv

from langchain_aws import ChatBedrockConverse
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, ToolMessage


from langfuse import get_client
from langfuse.langchain import CallbackHandler
from langfuse import observe
from prompts.prompts import query_agent_system_prompt, supervisor_agent_system_prompt
from agents.query_agent.graph import graph


# Import dbt-tools
from tools.dbt_tools import (
    fetch_metrics_tool,
    create_query_tool,
    fetch_query_result_tool,
    search_dimension_values_tool
)

# -----------------------
# Logging & Env Setup
# -----------------------
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
load_dotenv()

lf = get_client()  # uses env vars
lf_handler = CallbackHandler()

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
@observe(as_type="session", name="dbt-agent-graph")
async def run_with_trace(graph, message):
        result = await graph.ainvoke(
            {"messages": [
                {"role": "user", "content": message}
            ]
            },
            config={"callbacks": [lf_handler]}
        )
        assistant = result["messages"][-1]
        # capture assistant content for trace
        #trace_id = langfuse_context.get_current_trace_id()
        trace_id = lf.get_current_trace_id()
        print("trace id is ", trace_id)
        return assistant, trace_id

# -----------------------
# Main async entrypoint
# -----------------------
async def main():
 

    async def gr_chat(message, history):
        try:
            assistant, trace_id = await run_with_trace(graph, message)
            history = history or []
            history.append({"role": "user", "content": message})
            history.append({"role": "assistant", "content": getattr(assistant, "content", str(assistant))})
            # Expose the trace id in UI (could be hidden) for feedback
            return to_gradio_format(history), trace_id
        except Exception as e:
            logging.exception("Error in chat")
            return history, f"❌ Error: {e}"

    with gr.Blocks() as demo:
        chatbot_ui = gr.Chatbot(type="messages", label="DBT Agent")
        txt = gr.Textbox(placeholder="Ask...", label="Your question")
        feedback_rating = gr.Radio(choices=["👍", "👎"], label="Was this response helpful?", value=None)
        feedback_comment = gr.Textbox(placeholder="Optional comment", label="Feedback details")
        submit_btn = gr.Button("Submit Feedback")
        trace_id_state = gr.Textbox(visible=False)  # to stash trace_id

        # Feedback submission
        def submit_feedback(rating, comment, trace_id, last_history):
            # Normalize rating
            helpful = 1.0 if rating == "👍" else 0.0 if rating == "👎" else None
            # Attach feedback to Langfuse via updating trace or as event
            if trace_id:
                try:
                    # If you still have span object, you could update; otherwise use client method
                    lf.create_event(
                        name="user_feedback",
                        input={"rating": rating, "comment": comment},
                        output={"last_history": last_history},
                        trace_id=trace_id,
                    )
                    # Also tag the trace for easy lookup
                    lf.update_trace(  # if available; else use span.update_trace earlier
                        trace_id=trace_id,
                        tags=["user_feedback"],
                        metadata={"feedback_comment": comment, "feedback_rating": rating}
                    )
                except Exception:
                    # Fallback: store as a score
                    lf.create_score(trace_id=trace_id, name="user_feedback_helpful", value=helpful or 0.0)
            return "Thanks for the feedback!"

        submit_btn.click(
            submit_feedback,
            [feedback_rating, feedback_comment, trace_id_state, chatbot_ui],
            [gr.Textbox(label="Feedback status", interactive=False)]
        )

        txt.submit(gr_chat, [txt, chatbot_ui], [chatbot_ui, trace_id_state])

    demo.launch(server_name="0.0.0.0", server_port=7860)


if __name__ == "__main__":
    asyncio.run(main())
