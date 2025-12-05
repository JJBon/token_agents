from fastapi import FastAPI
from langgraph_openai_serve import LangGraphOpenAIServe
from agents.query_agent.graph import build_graph  # your compiled graph

app = FastAPI()
graph = build_graph()

server = LangGraphOpenAIServe(app=app)
server.register_graph(
    graph=graph,
    model_name="my-langgraph-agent",  # what Open WebUI will see as the "model"
)
