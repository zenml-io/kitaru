"""Integration test for the durable harness example.

Uses sys.path.insert because harness.py uses relative imports
(from models import ...) which don't resolve via dotted package
imports (from examples.durable_harness.harness import ...).
The coding_agent example has the same relative-import pattern
but no integration test, so this is the first test to face it.

Mirrors the test_phase12_llm_example.py pattern for LLM mocking:
register a model alias + set a fake API key + set KITARU_LLM_MOCK_RESPONSE.
All three are required because kitaru.llm() calls resolve_model_selection()
BEFORE checking the mock env var.

Note: FlowHandle.wait() cannot extract the return value from flows with
3+ checkpoints because ZenML's dynamic pipeline does not track step
dependencies (all steps appear as terminal). This test uses KitaruClient
to verify execution completion and inspect artifacts/metadata instead.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from kitaru import KitaruClient
from kitaru.config import register_model_alias

# harness.py uses relative imports (from models import ...) so we need
# its directory on sys.path for the import to work.
sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "durable_harness"))


MOCK_PASSING_EVAL = json.dumps(
    {
        "passed": True,
        "feedback": "All criteria met.",
        "criteria_met": 4,
        "criteria_total": 4,
    }
)

_POLL_TIMEOUT_SECONDS = 60.0


def test_harness_happy_path(
    monkeypatch,
    primed_zenml,
) -> None:
    """Full flow runs end-to-end with mock LLM, passes on round 1.

    The mock response is a valid EvaluationReport JSON with passed=True.
    All four kitaru.llm() calls (planner, builder, evaluator, summarizer)
    return this same string. The planner and builder don't care — they just
    return strings. The evaluator parses it as JSON -> passed=True -> the flow
    completes on round 1 without hitting kitaru.wait().
    """
    # Model resolution runs before mock short-circuit — must succeed.
    # Pattern from test_phase12_llm_example.py.
    register_model_alias("harness", model="anthropic/claude-sonnet-4-6")
    register_model_alias("harness-fast", model="anthropic/claude-haiku-4-5-20251001")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("KITARU_LLM_MOCK_RESPONSE", MOCK_PASSING_EVAL)
    monkeypatch.setenv("DURABLE_HARNESS_FAST_MODEL", "harness-fast")

    from harness import durable_harness

    handle = durable_harness.run("test dashboard")
    assert handle.exec_id  # non-empty

    # Wait for execution to complete by polling status.
    client = KitaruClient()
    deadline = time.time() + _POLL_TIMEOUT_SECONDS
    while time.time() < deadline:
        execution = client.executions.get(handle.exec_id)
        if execution.status.value in ("completed", "failed"):
            break
        time.sleep(0.5)
    else:
        raise TimeoutError(
            f"Execution {handle.exec_id} did not complete within "
            f"{_POLL_TIMEOUT_SECONDS:.0f}s."
        )

    assert execution.status.value == "completed"
    assert execution.pending_wait is None, (
        "Flow should not be waiting (evaluator passed)"
    )

    # Verify checkpoints ran: planner, builder_round_0, evaluator_round_0.
    # No summarize_round_0 because the evaluator passed on round 1.
    checkpoint_names = {cp.name for cp in execution.checkpoints}
    assert "planner" in checkpoint_names
    assert "builder_round_0" in checkpoint_names
    assert "evaluator_round_0" in checkpoint_names
    assert "summarize_round_0" not in checkpoint_names

    # Verify expected artifacts were saved by checkpoints.
    artifact_names = {art.name for art in execution.artifacts}
    assert "spec" in artifact_names
    assert "code_round_0" in artifact_names
    assert "qa_report_round_0" in artifact_names

    # Verify flow-level metadata includes the outcome.
    assert execution.metadata.get("outcome") == "passed"
