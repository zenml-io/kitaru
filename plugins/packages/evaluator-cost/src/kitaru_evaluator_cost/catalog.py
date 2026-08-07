"""Default plugin catalog entry for the cost evaluator."""


def get_definitions() -> list[dict[str, str | None]]:
    """Return the cost evaluator definition."""
    return [
        {
            "kind": "evaluator",
            "name": "kitaru/cost",
            "description": "Report the total recorded session cost.",
            "provider": None,
            "entrypoint": "kitaru_evaluator_cost.evaluator:evaluate",
        }
    ]
