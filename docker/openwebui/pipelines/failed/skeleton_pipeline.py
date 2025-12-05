from typing import List, Optional
from pydantic import BaseModel
from schemas import OpenAIChatMessage
from utils.pipelines.main import get_last_user_message, get_last_assistant_message



class Pipeline:
    class Valves(BaseModel):
        # List target pipeline ids (models) that this filter will be connected to.
        # If you want to connect this filter to all pipelines, you can set pipelines to ["*"]
        pipelines: List[str] = []

        # Assign a priority level to the filter pipeline.
        # The priority level determines the order in which the filter pipelines are executed.
        # The lower the number, the higher the priority.
        priority: int = 0

        # Add your custom parameters/configuration here e.g. API_KEY that you want user to configure etc.
        pass

    def __init__(self):
        self.type = "filter"
        self.name = "Filter"
        self.valves = self.Valves(**{"pipelines": ["*"]})

        pass

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is stopped.
        print(f"on_shutdown:{__name__}")
        pass

    async def inlet(self, body: dict, user: Optional[dict] = None) -> dict:
        messages = body.get("messages", [])
        user_message = get_last_user_message(messages)
        
        if user_message is not None:
            # Do something

            for message in reversed(messages):
                if message["role"] == "user":
                    message["content"] = "UPDATED CORRESPONDING CONTENT THAT LLM WILL USE"
                    break

        body = {**body, "messages": messages}
        return body
        
    async def outlet(self, body: dict, user: Optional[dict] = None) -> dict:
    	messages = body["messages"]
        assistant_message = get_last_assistant_message(messages)

        if assistant_message is not None:
            # Do something
            for message in reversed(messages):
                if message["role"] == "assistant":
                    message["content"] = "UPDATED CORRESPONDING CONTENT THAT USER WILL SEE"
                    break

        body = {**body, "messages": messages}
        return body