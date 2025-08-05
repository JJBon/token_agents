from ragas.prompt import PydanticPrompt
from pydantic import BaseModel, Field

from langfuse import get_client

lf = get_client()

query_agent_system_prompt = lf.get_prompt(
        "semantic/system-prompts/query-agent",
        label="latest"
    )

supervisor_agent_system_prompt = lf.get_prompt(
        "semantic/system-prompts/bi-supervisor",
        label="latest"
    )