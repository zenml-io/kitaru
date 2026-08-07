"""Default plugin catalog entry for the latency evaluator."""


def get_definitions() -> list[dict[str, str | None]]:
    """Return the latency evaluator definition."""
    return [
        {
            "kind": "evaluator",
            "name": "kitaru/latency",
            "description": "Measure session wall-clock duration.",
            "provider": None,
            "entrypoint": "kitaru_evaluator_latency.evaluator:evaluate",
        }
    ]
