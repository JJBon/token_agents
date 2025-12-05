from typing import Optional

from pydantic import BaseModel, Field


class AssistantChatRequest(BaseModel):
    """
    Request payload for interacting with the chatbot assistant.

    This model supports both single-turn and multi-turn conversations.
    If a session_id is provided, the assistant will retain context from previous messages within the same session.
    """

    message: str = Field(
        ...,
        description="The user's message to the chatbot. This is the main input the assistant will respond to."
    )
    session_id: Optional[str] = Field(
        None,
        description="An optional session identifier that enables conversation memory. "
                    "Use the same session_id to continue a chat and maintain context across multiple messages."
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "message": "List the columns of my data",
                "session_id": "abc123"
            }
        }
    }
