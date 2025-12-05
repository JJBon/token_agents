# servers/medium.py
import uuid

from fastapi import APIRouter, FastAPI
from fastapi.responses import StreamingResponse
from langchain_core.messages import HumanMessage

from servers.src.api.app.shared_state import get_dataframe_assistant_graph
from servers.src.api.schemas import AssistantChatRequest
from langfuse.langchain import CallbackHandler
lf_handler = CallbackHandler()


router = APIRouter(prefix="/chat", tags=["Chatbot"])


@router.post("/dataframe-assistant", response_class=StreamingResponse)
async def dataframe_assistant_chat(
    chat_req: AssistantChatRequest,
) -> StreamingResponse:
    user_message = chat_req.message
    thread_id = chat_req.session_id or str(uuid.uuid4())

    config = {"configurable": {"thread_id": thread_id}, "callbacks": [lf_handler]}

    assistant_graph = await get_dataframe_assistant_graph()

    stream = assistant_graph.astream(
        {"messages": [HumanMessage(content=user_message)]},
        config=config,
        stream_mode="messages",
    )

    async def response_generator():
        has_yielded = False
        async for msg, metadata in stream:
            node = metadata.get("langgraph_node")
            # Debug (optional)
            # print("DEBUG node:", node, "msg:", msg)

            # Our graph nodes are 'assistant' and 'store_feedback'
            if node in ("assistant", "store_feedback"):
                content = getattr(msg, "content", None)
                if content:
                    yield content
                    has_yielded = True

        if not has_yielded:
            yield (
                "I apologize, but I couldn't generate a proper response to your question. "
                "Could you please rephrase or provide more context?"
            )

    return StreamingResponse(response_generator(), media_type="text/plain")


app = FastAPI()
app.include_router(router)
