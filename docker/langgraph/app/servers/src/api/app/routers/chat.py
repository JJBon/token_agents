import uuid

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from langchain_core.messages import HumanMessage

from servers.src.api.app.shared_state import get_dataframe_assistant_graph
from servers.src.api.schemas import AssistantChatRequest
from servers.src.variables import OLLAMA_MODEL

router = APIRouter(prefix='/chat', tags=['Chatbot'])

@router.post("/dataframe-assistant", response_class=StreamingResponse)
async def dataframe_assistant_chat(
        chat_req: AssistantChatRequest
) -> StreamingResponse:
    """
    Streams the Pandas Dataframe Assistant's response in real-time based on the user's message input.

    **Parameters:**

    - `chat_req` (`AssistantChatRequest`): The user's input message and optional session ID for conversation continuity.

    **Returns:**

    - `StreamingResponse`: A streaming plain-text response containing the assistant's generated reply in real time.
    """
    user_message = chat_req.message
    thread_id = chat_req.session_id or str(uuid.uuid4())

    config = {
        "configurable": {"thread_id": thread_id}
    }

    assistant_graph = get_dataframe_assistant_graph()
    stream = assistant_graph.astream(
        {"messages": [HumanMessage(content=user_message)]},
        config=config,
        stream_mode="messages"
    )

    async def response_generator():
        has_yielded = False
        async for msg, metadata in stream:
            if metadata['langgraph_node'] == 'agent':
                if msg.content:
                    yield msg.content
                    has_yielded = True
                
        if not has_yielded:
            yield "I apologize, but I couldn't generate a proper response to your question. Could you please rephrase or provide more context?"

    return StreamingResponse(response_generator(), media_type="text/plain")


@router.get("/model-name")
def get_current_served_model_name():
    """
    Returns the name of the currently served LLM model.

    **Returns:**

    - `dict`: A dictionary with the model name.
    """
    return {"model": OLLAMA_MODEL}
