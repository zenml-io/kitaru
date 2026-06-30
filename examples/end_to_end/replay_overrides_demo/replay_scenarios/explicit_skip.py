"""Explicit skip — reuse a recorded checkpoint instead of recomputing it.

CLI equivalent:

    uv run kitaru executions replay "$PROD_ID" \\
      --at lookup_policy_tool \\
      --flow-overrides '{"prompt_profile":"trimmed_permissions"}' \\
      --skip publish_support_decision \\
      --wait \\
      -o json
"""

from __future__ import annotations

from support_agent import FINAL_DECISION_CHECKPOINT, REPLAY_POINT

from kitaru import KitaruClient

VARIANT_PROMPT_PROFILE = "trimmed_permissions"


def replay_with_explicit_skip(prod_id: str) -> None:
    client = KitaruClient()
    client.executions.replay(
        prod_id,
        at=REPLAY_POINT,
        flow_overrides={"prompt_profile": VARIANT_PROMPT_PROFILE},
        skip=[FINAL_DECISION_CHECKPOINT],
        wait=True,
    )
