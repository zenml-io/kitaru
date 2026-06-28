"""Invocation model override — change the model on one recorded LLM call.

CLI equivalent:

    uv run kitaru executions replay "$PROD_ID" \\
      --at lookup_policy_tool \\
      --invocation-overrides '{"support_copilot_model_request_2":{"model":"openai:gpt-5-nano"}}' \\
      --wait \\
      -o json
"""

from __future__ import annotations

from support_agent import FINAL_MODEL_INVOCATION, REPLAY_POINT

from kitaru import KitaruClient

VARIANT_MODEL = "openai:gpt-5-nano"


def replay_with_invocation_model_override(prod_id: str) -> None:
    client = KitaruClient()
    submission = client.executions.replay(
        prod_id,
        at=REPLAY_POINT,
        invocation_overrides={
            FINAL_MODEL_INVOCATION: {"model": VARIANT_MODEL},
        },
        wait=True,
    )
