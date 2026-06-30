"""Flow override replay — change model and prompt for the replay run.

CLI equivalent:

    uv run kitaru executions replay "$PROD_ID" \
      --at lookup_policy_tool \
      --flow-overrides '{
        "model":"openai:gpt-5-nano",
        "prompt_profile":"trimmed_permissions"
      }' \
      --wait \
      -o json
"""

from __future__ import annotations

from support_agent import REPLAY_POINT

from kitaru import KitaruClient

VARIANT_MODEL = "openai:gpt-5-nano"
VARIANT_PROMPT_PROFILE = "trimmed_permissions"


def replay_with_flow_overrides(prod_id: str) -> None:
    client = KitaruClient()
    client.executions.replay(
        prod_id,
        at=REPLAY_POINT,
        flow_overrides={
            "model": VARIANT_MODEL,
            "prompt_profile": VARIANT_PROMPT_PROFILE,
        },
        wait=True,
    )
