"""Publish input override — re-run publish with a substituted decision dict.

Upstream agent and tool checkpoints stay cached from prod. Only
``publish_support_decision`` re-executes with the overridden input.

CLI equivalent:

    uv run kitaru executions replay "$PROD_ID" \\
      --at publish_support_decision \\
      --invocation-overrides '{"publish_support_decision":{"input":{"policy_label":"injected_support_decision","risk_status":"safe_to_answer","required_action":"answer_directly_with_safety_note","summary":"Injected during replay"}}}' \\
      --wait \\
      -o json
"""

from __future__ import annotations

from typing import Any

from support_agent import FINAL_DECISION_CHECKPOINT

from kitaru import KitaruClient

INJECTED_DECISION: dict[str, Any] = {
    "policy_label": "injected_support_decision",
    "risk_status": "safe_to_answer",
    "required_action": "answer_directly_with_safety_note",
    "summary": "Injected during replay to test a corrected publish payload.",
}


def replay_with_publish_input_override(prod_id: str) -> None:
    client = KitaruClient()
    submission = client.executions.replay(
        prod_id,
        at=FINAL_DECISION_CHECKPOINT,
        invocation_overrides={
            FINAL_DECISION_CHECKPOINT: {"input": INJECTED_DECISION},
        },
        wait=True,
    )
