import uuid
from typing import List, Dict, Optional
from ragas.messages import ToolCall, HumanMessage as RagasHuman, AIMessage as RagasAI, ToolMessage as RagasTool
from ragas.dataset_schema import MultiTurnSample
from ragas.metrics import ToolCallAccuracy

# -----------------------
# Helper Functions
# -----------------------

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage, SystemMessage
from ragas.messages import HumanMessage as RagasHuman, AIMessage as RagasAI, ToolMessage as RagasTool, ToolCall
import uuid

def normalize_content(content):
    if isinstance(content, list):
        texts = [c.get("text", "") for c in content if isinstance(c, dict) and "text" in c]
        return " ".join(texts).strip()
    return str(content)

def normalize_tool_calls(tool_calls):
    normalized = []
    for tc in tool_calls or []:
        tc_copy = dict(tc) if isinstance(tc, dict) else tc.__dict__
        tc_copy.setdefault("id", f"tool_{uuid.uuid4()}")
        normalized.append(tc_copy)
    return normalized

def convert_message(raw_msg):
    """
    Converts LangChain message objects or dicts to Ragas messages.
    """
    if isinstance(raw_msg, HumanMessage):
        return RagasHuman(content=normalize_content(raw_msg.content))

    if isinstance(raw_msg, SystemMessage):
        # Treat SystemMessage as HumanMessage for evaluation purposes
        return RagasHuman(content=normalize_content(raw_msg.content))

    if isinstance(raw_msg, AIMessage):
        tool_calls = []
        if hasattr(raw_msg, "tool_calls") and raw_msg.tool_calls:
            for tc in raw_msg.tool_calls:
                name = tc.get("name") if isinstance(tc, dict) else getattr(tc, "name", "unknown_tool")
                args = tc.get("args", {}) if isinstance(tc, dict) else getattr(tc, "args", {})
                tool_calls.append(ToolCall(name=name, args=args))
        return RagasAI(content=normalize_content(raw_msg.content), tool_calls=tool_calls)

    if isinstance(raw_msg, ToolMessage):
        return RagasTool(content=normalize_content(raw_msg.content))

    # If it's already a dict
    if isinstance(raw_msg, dict):
        msg_type = raw_msg.get("type", raw_msg.get("role", "human"))
        if msg_type.lower() in ["human", "user"]:
            return RagasHuman(content=normalize_content(raw_msg.get("content", "")))
        if msg_type.lower() in ["ai", "assistant"]:
            tool_calls = [ToolCall(name=tc["name"], args=tc.get("args", {}))
                          for tc in normalize_tool_calls(raw_msg.get("tool_calls", []))]
            return RagasAI(content=normalize_content(raw_msg.get("content", "")), tool_calls=tool_calls)
        if msg_type.lower() in ["tool"]:
            return RagasTool(content=normalize_content(raw_msg.get("content", "")))
    
    # Fallback
    return RagasHuman(content=str(raw_msg))



# -----------------------
# Evaluator
# -----------------------

async def evaluate_messages(messages: List, expected_tools: List[ToolCall], final_output: Optional[str] = None) -> Dict:
    """
    Evaluate tool call accuracy and final output quality (if provided).
    Returns a dict with individual scores and a general combined score.
    """

    # Convert to Ragas messages
    ragas_messages = [convert_message(m) for m in messages]

    # Tool Call Accuracy
    sample = MultiTurnSample(
        user_input=ragas_messages,
        reference_tool_calls=expected_tools
    )
    tool_accuracy_score = await ToolCallAccuracy().multi_turn_ascore(sample)

    # Final output heuristic
    final_output_score = 1.0
    if final_output:
        expected_keywords = [tc.args.get("value") for tc in expected_tools if "value" in tc.args]
        if expected_keywords:
            matches = sum(1 for kw in expected_keywords if kw and kw.lower() in final_output.lower())
            final_output_score = matches / len(expected_keywords)

    general_score = round((tool_accuracy_score + final_output_score) / 2, 4)

    return {
        "general_score": general_score,
        "tool_accuracy": round(tool_accuracy_score, 4),
        "final_output_score": round(final_output_score, 4),
    }
