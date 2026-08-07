"""Default evaluator catalog."""


def get_definitions() -> list[dict[str, str | None]]:
    """Return the built-in evaluator definitions."""
    return [
        {
            "kind": "evaluator",
            "name": "kitaru/cost",
            "description": "Report the total recorded session cost.",
            "provider": None,
            "entrypoint": "kitaru_evaluator.basic:cost",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/latency",
            "description": "Measure session wall-clock duration.",
            "provider": None,
            "entrypoint": "kitaru_evaluator.basic:latency",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/tool-call-patterns",
            "description": "Count repeated calls to the same tool.",
            "provider": None,
            "entrypoint": "kitaru_evaluator.basic:tool_call_patterns",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/session-diagnostics",
            "description": "Check session completeness and internal consistency.",
            "provider": None,
            "entrypoint": "kitaru_evaluator.deterministic:session_diagnostics",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/output-contract",
            "description": "Check output against exact and structural rules.",
            "provider": None,
            "entrypoint": "kitaru_evaluator.deterministic:output_contract",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/trajectory-signals",
            "description": "Report repetition, failed retries, and short tool cycles.",
            "provider": None,
            "entrypoint": "kitaru_evaluator.deterministic:trajectory_signals",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/tool-health",
            "description": "Report recorded tool failures and result anomalies.",
            "provider": None,
            "entrypoint": "kitaru_evaluator.deterministic:tool_health",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/timing-profile",
            "description": "Report recorded wall-clock and node timing.",
            "provider": None,
            "entrypoint": "kitaru_evaluator.deterministic:timing_profile",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/resource-budget",
            "description": "Apply configured ceilings to recorded resource use.",
            "provider": None,
            "entrypoint": "kitaru_evaluator.deterministic:resource_budget",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/tool-policy",
            "description": "Apply exact tool requirements, prohibitions, and limits.",
            "provider": None,
            "entrypoint": "kitaru_evaluator.deterministic:tool_policy",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/llm-call-signals",
            "description": "Report LLM failures, repetition, and metadata coverage.",
            "provider": None,
            "entrypoint": "kitaru_evaluator.deterministic:llm_call_signals",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/model-policy",
            "description": "Apply exact model and provider rules.",
            "provider": None,
            "entrypoint": "kitaru_evaluator.deterministic:model_policy",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/workflow-conformance",
            "description": "Compare recorded tool order with a configured workflow.",
            "provider": None,
            "entrypoint": "kitaru_evaluator.deterministic:workflow_conformance",
        },
    ]
