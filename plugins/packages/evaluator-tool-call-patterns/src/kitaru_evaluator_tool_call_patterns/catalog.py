"""Default plugin catalog entry for the tool-call pattern evaluator."""


def get_definitions() -> list[dict[str, str | None]]:
    """Return the tool-call pattern evaluator definition."""
    return [
        {
            "kind": "evaluator",
            "name": "kitaru/tool-call-patterns",
            "description": "Count repeated calls to the same tool.",
            "provider": None,
            "entrypoint": "kitaru_evaluator_tool_call_patterns.evaluator:evaluate",
        }
    ]
