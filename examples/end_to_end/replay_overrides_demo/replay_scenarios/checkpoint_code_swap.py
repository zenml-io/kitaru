"""Checkpoint code swap — replace every matching tool invocation with new code.

CLI equivalent:

    uv run kitaru executions replay "$PROD_ID" \\
      --at lookup_policy_tool \\
      --checkpoint-overrides '{"lookup_policy_tool":{"code":"mocks.lookup_policy"}}' \\
      --wait \\
      -o json
"""

from __future__ import annotations

from support_agent import REPLAY_POINT

from kitaru import KitaruClient


def replay_with_checkpoint_code_swap(prod_id: str) -> None:
    client = KitaruClient()
    submission = client.executions.replay(
        prod_id,
        at=REPLAY_POINT,
        checkpoint_overrides={
            "lookup_policy_tool": {"code": "mocks.lookup_policy"},
        },
        wait=True,
    )
