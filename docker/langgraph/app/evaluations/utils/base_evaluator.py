import uuid
from typing import List, Dict, Optional
from ragas.messages import ToolCall, HumanMessage as RagasHuman, AIMessage as RagasAI, ToolMessage as RagasTool
from ragas.dataset_schema import MultiTurnSample
from ragas.metrics import ToolCallAccuracy

# Initialize Langfuse client

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

async def evaluate_messages(messages: List, expected_tools: List[ToolCall], final_output: Optional[str] = None) -> Dict:
    """
    Evaluate tool call accuracy and final output quality (if provided).
    Returns a dict with individual scores and a general combined score.
    Logs the scores to Langfuse for traceability.
    """

    # 1. Tool call accuracy
    sample = MultiTurnSample(user_input=messages, reference_tool_calls=expected_tools)
    tool_accuracy_score = await ToolCallAccuracy().multi_turn_ascore(sample)

    # 2. Final output score (placeholder heuristic)
    final_output_score = 0
    if final_output:
        # Example: simple keyword match, replace with your own evaluator
        expected_keywords = [tc.args.get("value") for tc in expected_tools if "value" in tc.args]
        if expected_keywords:
            matches = sum(1 for kw in expected_keywords if kw.lower() in final_output.lower())
            final_output_score = matches / len(expected_keywords)

    # 3. Combine scores
    general_score = round((tool_accuracy_score + final_output_score) / 2, 4)

    scores = {
        "general_score": general_score,
        "tool_accuracy": round(tool_accuracy_score, 4),
        "final_output_score": round(final_output_score, 4)
    }

    return scores
