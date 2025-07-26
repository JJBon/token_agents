import yaml
from evaluations.base_evaluator import convert_message, evaluate_messages
from ragas.messages import ToolCall

def load_scenario(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

async def run_scenario(scenario, simulate=False, graph=None):
    user_input = scenario["input"]
    expected_tools = [ToolCall(name=tc["name"], args=tc.get("args", {}))
                      for tc in scenario["expected_tools"]]

    if simulate:
        messages = [
            convert_message({"type": "human", "content": user_input}),
            convert_message({"type": "ai", "content": "Simulated response",
                             "tool_calls": [tc.dict() for tc in expected_tools]}),
        ]
    else:
        result = await graph.ainvoke({"messages": [user_input]})
        raw_messages = [{"type": m.__class__.__name__.replace("Message", "").lower(),
                         "content": getattr(m, "content", ""),
                         "tool_calls": getattr(m, "tool_calls", [])}
                        for m in result["messages"]]
        messages = [convert_message(m) for m in raw_messages]

    return await evaluate_messages(messages, expected_tools)
