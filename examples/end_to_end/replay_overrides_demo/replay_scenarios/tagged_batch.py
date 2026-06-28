"""Tagged batch replay — replay many explicit execution IDs with one tag.

CLI equivalent:

    uv run kitaru executions replay "$ID_1" "$ID_2" "$ID_3" \\
      --at lookup_policy_tool \\
      --flow-overrides '{"model":"openai:gpt-5-nano","prompt_profile":"trimmed_permissions"}' \\
      --tag replay-overrides-demo \\
      --wait \\
      --on-error collect \\
      -o json
"""

from __future__ import annotations

import json
from pathlib import Path

from support_agent import REPLAY_POINT

from kitaru import KitaruClient

REPORTS = Path("reports")
VARIANT_MODEL = "openai:gpt-5-nano"
VARIANT_PROMPT_PROFILE = "trimmed_permissions"
REPLAY_TAG = "replay-overrides-demo"


def replay_tagged_batch(prod_ids: list[str]) -> None:
    client = KitaruClient()
    submission = client.executions.replay(
        prod_ids,
        at=REPLAY_POINT,
        flow_overrides={
            "model": VARIANT_MODEL,
            "prompt_profile": VARIANT_PROMPT_PROFILE,
        },
        tag=REPLAY_TAG,
        wait=True,
        on_error="collect",
    )

    REPORTS.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS / "tagged_batch.json"
    report_path.write_text(
        json.dumps(submission.to_json(), indent=2, sort_keys=True),
        encoding="utf-8",
    )
