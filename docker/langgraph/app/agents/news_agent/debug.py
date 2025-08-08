# news_agent/langgraph_graph.py

from typing import Annotated, TypedDict, List, Any, Dict
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langchain_core.runnables import RunnableLambda, RunnableConfig
from langchain_aws import ChatBedrockConverse, BedrockEmbeddings

from scrapegraphai.nodes.fetch_node_level_k import FetchNodeLevelK
from scrapegraphai.nodes.html_analyzer_node import HtmlAnalyzerNode
from scrapegraphai.nodes.parse_node_depth_k_node import ParseNodeDepthK
from scrapegraphai.nodes.rag_node import RAGNode
from scrapegraphai.nodes.generate_answer_node import GenerateAnswerNode

import boto3

# --- Define LangGraph State ---
class State(TypedDict):
    messages: Annotated[List[Any], add_messages]
    url: str
    user_prompt: str
    doc: Any
    analyzed_doc: Any
    parsed_doc: Any
    relevant_chunks: Any
    answer: Any

# --- LLM + Embedder Setup ---
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

llm_model = ChatBedrockConverse(
    model="anthropic.claude-3-haiku-20240307-v1:0",
    provider="anthropic",
    temperature=0,
    client=bedrock
)

embedder_model = BedrockEmbeddings(model_id="amazon.titan-embed-text-v1:0")

# --- Node Initialization ---
fetch_node = FetchNodeLevelK(
    input="url",
    output=["doc"],
    node_config={
        "headless": True,
        "depth": 1,
        "only_inside_links": True,
        "verbose": True,
    },
)

html_analyzer_node = HtmlAnalyzerNode(
    input="doc",
    output=["analyzed_doc"],
    node_config={"verbose": True, "llm_model": llm_model}
)

parse_node = ParseNodeDepthK(
    input="analyzed_doc",
    output=["parsed_doc"],
    node_config={"verbose": True}
)

rag_node = RAGNode(
    input="user_prompt & parsed_doc",
    output=["relevant_chunks"],
    node_config={
        "llm_model": llm_model,
        "embedder_model": embedder_model,
        "verbose": True
    }
)

generate_node = GenerateAnswerNode(
    input="user_prompt & relevant_chunks",
    output=["answer"],
    node_config={
        "llm_model": llm_model,
        "verbose": True
    }
)

# --- Node Wrappers ---
def wrap_node(node, required_keys=None, preprocess=None):
    def _fn(state: dict, config: RunnableConfig = None) -> dict:
        input_state = {k: state[k] for k in required_keys} if required_keys else state
        if preprocess:
            input_state = preprocess(input_state)
        output = node.execute(input_state)
        return {**state, **output}
    return RunnableLambda(_fn)

def flatten_documents(state):
    docs = []
    for entry in state["doc"]:
        if isinstance(entry, dict) and "document" in entry:
            docs.extend(entry["document"])
    state["doc"] = docs
    return state

# --- Build Graph ---
def build_graph(config: RunnableConfig = None):
    builder = StateGraph(State)

    builder.add_node("fetch", wrap_node(fetch_node, required_keys=["url"]))
    builder.add_node("analyze", wrap_node(html_analyzer_node, preprocess=flatten_documents))
    builder.add_node("parse", wrap_node(parse_node))
    builder.add_node("rag", wrap_node(rag_node))
    builder.add_node("generate", wrap_node(generate_node))

    builder.set_entry_point("fetch")
    builder.add_edge("fetch", "analyze")
    builder.add_edge("analyze", "parse")
    builder.add_edge("parse", "rag")
    builder.add_edge("rag", "generate")
    builder.add_edge("generate", END)

    return builder.compile()
