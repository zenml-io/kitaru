"""Seed production-like support copilot runs for replay demos.

Prefer the dispatcher entrypoint from this example directory:

    uv run python demo.py seed
    uv run python demo.py seed --count 15

Writes one execution ID per line to ``fixtures/prod_exec_ids``. The first line is
the primary run used by single-execution replay scenarios.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from dotenv import load_dotenv
from support_agent import support_copilot_flow
from utils.runtime import quiet_runtime_logs, wait_for_execution

DEMO_ROOT = Path(__file__).resolve().parent
SCENARIOS_PATH = DEMO_ROOT / "fixtures" / "scenarios.json"
PROD_EXEC_IDS_PATH = DEMO_ROOT / "fixtures" / "prod_exec_ids"

BASELINE_MODEL = "openai:gpt-5-mini"
BASELINE_PROMPT_PROFILE = "baseline"
DEFAULT_COUNT = 1


def load_scenarios() -> list[dict[str, str]]:
    payload = json.loads(SCENARIOS_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise RuntimeError(f"{SCENARIOS_PATH} must contain a JSON list.")
    scenarios: list[dict[str, str]] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise RuntimeError(f"Scenario {index} must be an object.")
        label = str(item.get("label", "")).strip()
        customer = str(item.get("customer", "")).strip()
        prompt = str(item.get("prompt", "")).strip()
        if not label or not customer or not prompt:
            raise RuntimeError(f"Scenario {index} needs label, customer, and prompt.")
        scenarios.append({"label": label, "customer": customer, "prompt": prompt})
    return scenarios


def seed_prod_runs(*, count: int = DEFAULT_COUNT) -> list[str]:
    """Run the support copilot for ``count`` scenarios and save execution IDs."""
    quiet_runtime_logs()
    scenarios = load_scenarios()
    if count > len(scenarios):
        raise SystemExit(
            f"Requested {count} runs but only {len(scenarios)} scenarios exist in "
            f"{SCENARIOS_PATH}. Add more scenarios or lower --count."
        )

    exec_ids: list[str] = []
    for scenario in scenarios[:count]:
        handle = support_copilot_flow.run(
            prompt=scenario["prompt"],
            customer=scenario["customer"],
            model=BASELINE_MODEL,
            prompt_profile=BASELINE_PROMPT_PROFILE,
        )
        wait_for_execution(handle)
        exec_ids.append(handle.exec_id)

    PROD_EXEC_IDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROD_EXEC_IDS_PATH.write_text("\n".join(exec_ids) + "\n", encoding="utf-8")
    return exec_ids


def _parse_flag(argv: list[str], flag: str) -> tuple[list[str], str | None]:
    if flag not in argv:
        return argv, None
    index = argv.index(flag)
    if index + 1 >= len(argv):
        raise SystemExit(f"{flag} requires a value")
    return argv[:index] + argv[index + 2 :], argv[index + 1]


def main(argv: list[str]) -> None:
    load_dotenv(DEMO_ROOT / ".env")
    rest, count_raw = _parse_flag(argv, "--count")
    if rest:
        raise SystemExit(f"Unknown arguments: {' '.join(rest)}")
    seed_prod_runs(count=int(count_raw or DEFAULT_COUNT))


if __name__ == "__main__":
    main(sys.argv[1:])
