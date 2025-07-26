import uuid
from typing import List, Dict, Optional
from ragas.messages import ToolCall, HumanMessage as RagasHuman, AIMessage as RagasAI, ToolMessage as RagasTool
from ragas.dataset_schema import MultiTurnSample
from ragas.metrics import ToolCallAccuracy

def normalize_content(content):
    if isinstance(content, list):
        texts = [c.get("text", "") for c in content if isinstance(c, dict) and "text" in c]
        return " ".join(texts).strip()
    return str(content)

def normalize_tool_calls(tool_calls: Optional[List[Dict]]):
    normalized = []
    for tc in (tool_calls or []):
        tc_copy = dict(tc)
        tc_copy.setdefault("id", f"tool_{uuid.uuid4()}")
        normalized.append(tc_copy)
    return normalized

def convert_message(raw_msg: Dict):
    msg_type = raw_msg.get("type", "human")
    if msg_type.lower() in ["human", "user"]:
        return RagasHuman(content=normalize_content(raw_msg.get("content", "")))
    if msg_type.lower() in ["ai", "assistant"]:
        tool_calls = [ToolCall(name=tc["name"], args=tc.get("args", {}))
                      for tc in normalize_tool_calls(raw_msg.get("tool_calls", []))]
        return RagasAI(content=normalize_content(raw_msg.get("content", "")), tool_calls=tool_calls)
    if msg_type.lower() in ["tool"]:
        return RagasTool(content=normalize_content(raw_msg.get("content", "")))
    return RagasHuman(content=normalize_content(raw_msg.get("content", "")))

async def evaluate_messages(messages: List, expected_tools: List[ToolCall]):
    sample = MultiTurnSample(user_input=messages, reference_tool_calls=expected_tools)
    scorer = ToolCallAccuracy()
    raw_score = await scorer.multi_turn_ascore(sample)
    return min(1.0, raw_score)
