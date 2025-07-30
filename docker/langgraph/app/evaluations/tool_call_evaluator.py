from typing import List, Dict, Any
from langfuse import get_client

lf = get_client()

def extract_tool_calls(messages: List[Any]) -> List[Dict[str, Any]]:
    extracted = []
    for m in messages:
        calls = getattr(m, "tool_calls", []) or getattr(m, "__dict__", {}).get("tool_calls", [])
        for tc in calls or []:
            name = getattr(tc, "name", tc.get("name"))
            args = getattr(tc, "args", tc.get("args", {}))
            extracted.append({"name": name, "args": args})
    return extracted

def evaluate_tool_calls(messages: List[Any], expected: List[Dict[str, Any]]) -> float:
    actual = extract_tool_calls(messages)
    score = sum(
        1 for idx, exp in enumerate(expected)
        if idx < len(actual)
        and actual[idx]["name"] == exp["name"]
        and actual[idx]["args"] == exp.get("args", {})
    )
    return score / len(expected) if expected else 0.0

def evaluate_response_quality(messages: List[Any], keywords: List[str]) -> float:
    final = ""
    for m in reversed(messages):
        content = getattr(m, "content", "")
        if isinstance(content, str) and content.strip():
            final = content
            break
    text = final.lower()
    return sum(kw.lower() in text for kw in keywords) / len(keywords) if keywords else 0.0

def evaluate_scenario(messages: List[Any], test_name: str,
                      expected_tools: List[Dict[str, Any]],
                      expected_keywords: List[str]) -> Dict[str, float]:
    tool_score = evaluate_tool_calls(messages, expected_tools)
    response_score = evaluate_response_quality(messages, expected_keywords)

    trace_id = None
    try:
        trace_id = lf.get_current_trace_id()
    except Exception:
        pass

    # Score span/traces
    if trace_id:
        lf.create_score(trace_id=trace_id,
                        name=f"{test_name}_tool_call_accuracy",
                        value=tool_score)
        lf.create_score(trace_id=trace_id,
                        name=f"{test_name}_response_quality",
                        value=response_score)
    else:
        lf.score_current_trace(name=f"{test_name}_tool_call_accuracy",
                               value=tool_score)
        lf.score_current_trace(name=f"{test_name}_response_quality",
                               value=response_score)

    return {"tool_call_accuracy": tool_score, "response_quality": response_score}
