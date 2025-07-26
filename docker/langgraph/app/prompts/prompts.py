from ragas.experimental.prompt import PydanticPrompt
from pydantic import BaseModel, Field

class QueryInput(BaseModel):
    user_query: str = Field(...)

class QueryOutput(BaseModel):
    tool_calls: list = Field(...)

class AgentPrompt(PydanticPrompt[QueryInput, QueryOutput]):
    instruction = (
        "You are a DBT agent. "
        "First call `fetch_metrics`, then `create_query`, then `fetch_query_result`. "
        "Return your output as a JSON with 'tool_calls': list of tool names in order."
    )
    input_model = QueryInput
    output_model = QueryOutput
    examples = [
        (
            QueryInput(user_query="fetch bitcoin market cap growth rate"),
            QueryOutput(tool_calls=["fetch_metrics", "create_query", "fetch_query_result"])
        )
    ]
