"""Deterministic trace-emitting agent used by export contract tests."""

import json
import os
from pathlib import Path


def main() -> None:
    """Write an ATIF trajectory containing one tool call and a final answer."""
    inputs = json.loads(os.environ["KITARU_TASK_INPUTS"])
    trace = {
        "schema_version": "ATIF-v1.7",
        "agent": {"name": "fixture", "version": "1"},
        "steps": [
            {"step_id": 1, "source": "user", "message": inputs["question"]},
            {
                "step_id": 2,
                "source": "agent",
                "message": "I will calculate it.",
                "tool_calls": [
                    {
                        "tool_call_id": "call-1",
                        "function_name": "multiply",
                        "arguments": {"a": 6, "b": 7},
                    }
                ],
                "observation": {
                    "results": [{"source_call_id": "call-1", "content": "42"}]
                },
            },
            {"step_id": 3, "source": "agent", "message": "The answer is 42."},
        ],
    }
    Path(os.environ["KITARU_TRACE_PATH"]).write_text(json.dumps(trace))


if __name__ == "__main__":
    main()
