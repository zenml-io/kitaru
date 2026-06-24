"""PydanticAI support-copilot demo — seed / replay / cohort with Kitaru.

Operator story: see ``PLAYBOOK.md``. Quick commands:

    uv run python demo.py seed
    uv run python demo.py seed-cohort --count 10
    uv run python demo.py replay <PROD-ID>
    uv run python demo.py cohort --export-json reports/cohort_report.json

CLI equivalents:

    kitaru executions replay <PROD-ID> --at lookup_policy_tool \\
      --args '{"model": "openai:gpt-5-nano", "prompt_profile": "trimmed_permissions"}'
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from reporting.cohort_html import write as write_cohort_html
from reporting.comparison_html import write as write_html
from support_agent import (
    FLOW_NAME,
    REPLAY_POINT,
    SupportDecision,
    support_copilot_flow,
)
from utils import (
    cost,
    diff_decisions,
    execution_stats,
    latency,
    load_support_decision_from_execution,
    quality_judge,
)
from utils.cohort import run_cohort

import kitaru
from kitaru import KitaruClient, diff

# --- Scenario and replay variants -------------------------------------------

SCENARIO = (
    "I need to grant all members of our engineering team admin access to the "
    "production SSO settings so they can self-service identity provider changes "
    "without going through IT. Can you enable that for our account?"
)
CUSTOMER = "acme-corp / alice@acme.example"

BASELINE_MODEL = "openai:gpt-5-mini"
VARIANT_MODEL = "openai:gpt-5-nano"
VARIANT_PROMPT_PROFILE = "trimmed_permissions"

PROD_FIXTURE = Path("fixtures/prod_exec_id")
COHORT_SCENARIOS_PATH = Path("fixtures/cohort_scenarios.json")
COHORT_EXEC_IDS_FIXTURE = Path("fixtures/cohort_exec_ids")
HTML_PATH = "reports/replay_three_way.html"
COHORT_HTML_PATH = "reports/cohort_report.html"
COHORT_JSON_PATH = "reports/cohort_report.json"
_FLOW_NODES = (
    "support_copilot_model_request",
    "gather_context_tool",
    "lookup_policy_tool",
    "support_copilot_model_request_2",
    "publish_support_decision",
)


def section(text: str) -> None:
    """Print a bold step header."""
    print(f"\n\033[1m{text}\033[0m")


def decision_summary(decision: dict) -> str:
    """One-line view of a SupportDecision dict."""
    return (
        f"risk={decision.get('risk_status', '?')}  "
        f"action={decision.get('required_action', '?')}  "
        f"label={decision.get('policy_label', '?')}"
    )


def require_replay_anchor(client: KitaruClient, exec_id: str) -> None:
    """Fail clearly if the original run did not create the replay anchor."""
    run = client.executions.get(exec_id)
    checkpoint_names = [c.name for c in run.checkpoints]
    if REPLAY_POINT in checkpoint_names:
        return
    raise RuntimeError(
        f"Expected replay anchor {REPLAY_POINT!r}, but execution {exec_id!r} did not "
        f"create it. Checkpoints present: {checkpoint_names}."
    )


def write_comparison_html(
    original_id: str,
    model_replay_id: str,
    tool_replay_id: str,
    original: dict,
    model_replay: dict,
    tool_replay: dict,
) -> str:
    """Render original vs two replay variants."""
    fields = list(SupportDecision.model_fields)
    outcomes = [
        (
            f,
            original.get(f),
            model_replay.get(f),
            tool_replay.get(f),
            original.get(f) == model_replay.get(f),
            original.get(f) == tool_replay.get(f),
        )
        for f in fields
    ]

    client = KitaruClient()
    o, m, t = (
        execution_stats(client, i)
        for i in (original_id, model_replay_id, tool_replay_id)
    )

    def _runtime(stat: dict) -> str | None:
        seconds = stat["runtime_s"]
        return f"{seconds:.1f}s" if isinstance(seconds, (int, float)) else None

    run_stats = [
        ("runtime", _runtime(o), _runtime(m), _runtime(t)),
        ("total tokens", o["total_tokens"], m["total_tokens"], t["total_tokens"]),
        (
            "checkpoints",
            o["checkpoint_count"],
            m["checkpoint_count"],
            t["checkpoint_count"],
        ),
    ]

    model_drift = diff_decisions(original, model_replay).has_drift
    tool_drift = diff_decisions(original, tool_replay).has_drift
    return write_html(
        HTML_PATH,
        exec_id=original_id,
        scenario=SCENARIO[:80] + "..." if len(SCENARIO) > 80 else SCENARIO,
        cut="lookup_policy_tool",
        nodes=_FLOW_NODES,
        settings_changes=[
            ("model", BASELINE_MODEL, VARIANT_MODEL),
            ("prompt_profile", "baseline", VARIANT_PROMPT_PROFILE),
        ],
        outcomes=outcomes,
        run_stats=run_stats,
        has_reproduction_drift=False,
        has_edited_drift=model_drift or tool_drift,
        original_summary=decision_summary(original),
        reproduced_summary=decision_summary(model_replay),
        edited_summary=decision_summary(tool_replay),
    )


def run_once(*, prompt: str, customer: str) -> str:
    """Run the agent once and return its exec id."""
    handle = support_copilot_flow.run(prompt, customer, BASELINE_MODEL, "baseline")
    handle.wait()

    client = KitaruClient()
    require_replay_anchor(client, handle.exec_id)
    decision = load_support_decision_from_execution(client, handle.exec_id)
    print(f"   original exec_id={handle.exec_id}")
    print(f"   {decision_summary(decision)}")
    return handle.exec_id


def run() -> str:
    """Run the default scenario once and return its exec id."""
    section("Run the PydanticAI agent as a durable Kitaru flow")
    exec_id = run_once(prompt=SCENARIO, customer=CUSTOMER)
    print("   CLI variant replay:")
    print(
        f"     kitaru executions replay {exec_id} --at {REPLAY_POINT} "
        f'--args \'{{"model": "{VARIANT_MODEL}", '
        f'"prompt_profile": "{VARIANT_PROMPT_PROFILE}"}}\''
    )
    return exec_id


def _load_cohort_scenarios() -> list[dict[str, str]]:
    if not COHORT_SCENARIOS_PATH.is_file():
        raise RuntimeError(
            f"Missing cohort scenarios file: {COHORT_SCENARIOS_PATH}. "
            "Expected a JSON list of {label, customer, prompt} objects."
        )
    payload = json.loads(COHORT_SCENARIOS_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not payload:
        raise RuntimeError(
            f"{COHORT_SCENARIOS_PATH} must contain a non-empty JSON list."
        )
    scenarios: list[dict[str, str]] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise RuntimeError(
                f"{COHORT_SCENARIOS_PATH}[{index}] must be an object with "
                "label, customer, and prompt."
            )
        label = str(item.get("label", "")).strip()
        customer = str(item.get("customer", "")).strip()
        prompt = str(item.get("prompt", "")).strip()
        if not label or not customer or not prompt:
            raise RuntimeError(
                f"{COHORT_SCENARIOS_PATH}[{index}] missing label, customer, or prompt."
            )
        scenarios.append({"label": label, "customer": customer, "prompt": prompt})
    return scenarios


def seed_cohort(*, count: int = 10) -> list[str]:
    """Run distinct support requests and persist exec ids for cohort demos."""
    scenarios = _load_cohort_scenarios()[:count]
    if len(scenarios) < count:
        raise RuntimeError(
            f"Requested {count} cohort seeds but {COHORT_SCENARIOS_PATH} "
            f"only defines {len(scenarios)} scenario(s)."
        )

    section(f"Seed cohort — {count} distinct support_copilot_flow runs")
    exec_ids: list[str] = []
    for index, scenario in enumerate(scenarios, start=1):
        print(f"\n   [{index}/{count}] {scenario['label']}")
        exec_ids.append(
            run_once(prompt=scenario["prompt"], customer=scenario["customer"])
        )

    PROD_FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    PROD_FIXTURE.write_text(f"{exec_ids[0]}\n", encoding="utf-8")
    COHORT_EXEC_IDS_FIXTURE.write_text(
        "\n".join(exec_ids) + "\n",
        encoding="utf-8",
    )
    print(f"\n   wrote {PROD_FIXTURE} (first run for Act 2-4)")
    print(f"   wrote {COHORT_EXEC_IDS_FIXTURE} ({len(exec_ids)} exec ids)")
    print("   resolve cohort:")
    print(
        f"     kitaru executions cohort --flow {FLOW_NAME} "
        f"--at {REPLAY_POINT} --order-by=-display_cost_usd --limit {count}"
    )
    return exec_ids


def seed() -> str:
    """Run once and persist the prod exec id for clone-friendly demos."""
    exec_id = run()
    PROD_FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    PROD_FIXTURE.write_text(f"{exec_id}\n", encoding="utf-8")
    print(f"   wrote {PROD_FIXTURE}")
    return exec_id


def replay(exec_id: str) -> None:
    """Replay with model + tool changes, compare each to the original."""
    client = KitaruClient()
    require_replay_anchor(client, exec_id)
    original_decision = load_support_decision_from_execution(client, exec_id)

    section(
        f"Replay #1 — model change ({VARIANT_MODEL} + {VARIANT_PROMPT_PROFILE}) "
        "vs original"
    )
    model_handle = support_copilot_flow.replay(
        exec_id,
        at=REPLAY_POINT,
        cache=False,
        model=VARIANT_MODEL,
        prompt_profile=VARIANT_PROMPT_PROFILE,
    )
    model_handle.wait()
    model_decision = load_support_decision_from_execution(client, model_handle.exec_id)
    print(f"   model replay exec_id={model_handle.exec_id}")
    print(
        "   original → model replay: "
        f"{diff_decisions(original_decision, model_decision)}"
    )
    for url in diff(exec_id, model_handle.exec_id).urls:
        print(f"   ui: {url}")

    section("Replay #2 — tool mock vs original")
    tool_handle = support_copilot_flow.replay(
        exec_id,
        at=REPLAY_POINT,
        cache=False,
        tool={"lookup_policy": "mocks.lookup_policy"},
    )
    tool_handle.wait()
    tool_decision = load_support_decision_from_execution(client, tool_handle.exec_id)
    print(f"   tool replay exec_id={tool_handle.exec_id}")
    print(
        f"   original → tool replay: {diff_decisions(original_decision, tool_decision)}"
    )
    for url in diff(exec_id, tool_handle.exec_id).urls:
        print(f"   ui: {url}")

    path = write_comparison_html(
        exec_id,
        model_handle.exec_id,
        tool_handle.exec_id,
        original_decision,
        model_decision,
        tool_decision,
    )
    print(f"   html: {path}")

    section("Three-way compare — prod + model replay + tool replay")
    execution_diff = diff(exec_id, model_handle.exec_id, tool_handle.exec_id)
    if execution_diff.urls:
        print(f"   ui: {execution_diff.urls[0]}")
    for replay_id, checkpoint_diffs in execution_diff.compared:
        changed = [
            item.name
            for item in checkpoint_diffs
            if not item.status_match
            or any(
                left != right
                for left, right in item.artifact_hashes.values()
                if left is not None or right is not None
            )
        ]
        print(f"   diff vs {replay_id}: {len(changed)} changed checkpoint(s)")
        if changed:
            print(f"     {', '.join(changed)}")


def _resolve_cohort(limit: int) -> kitaru.CohortResult:
    deployment = os.environ.get("COHORT_DEPLOYMENT")
    return kitaru.cohort(
        flow=FLOW_NAME,
        at=REPLAY_POINT,
        deployment=deployment,
        order_by="-display_cost_usd",
        limit=limit,
    ).resolve()


def cohort(
    *,
    limit: int = 10,
    export_json: str | None = COHORT_JSON_PATH,
    with_quality_judge: bool = False,
) -> None:
    """Replay the model variant across a resolved cohort."""
    section(f"Cohort — top {limit} expensive originals with {REPLAY_POINT}")

    cohort_result = _resolve_cohort(limit)
    print(
        f"   resolved {cohort_result.matched} exec ids "
        f"(scanned {cohort_result.scanned})"
    )

    metrics = [cost, latency]
    if with_quality_judge:
        metrics.append(quality_judge)

    report = run_cohort(
        list(cohort_result.exec_ids),
        baseline_model=BASELINE_MODEL,
        variant_model=VARIANT_MODEL,
        variant_prompt_profile=VARIANT_PROMPT_PROFILE,
        metrics=metrics,
        repeats=1,
    )
    report.summary()
    regs = report.regressions()
    print(
        "   regressions:",
        [getattr(r, "name", r) for r in regs] if regs else "none",
    )

    html_path = write_cohort_html(COHORT_HTML_PATH, report)
    print(f"   html: {html_path}")

    if export_json:
        json_path = report.to_json(export_json, cohort=cohort_result.to_json())
        print(f"   json: {json_path}")

    section("Per-case compare URLs (original → variant)")
    for row in report.rows:
        for url in row.compare_urls:
            print(f"   ui ({row.base_exec_id}): {url}")


def run_all() -> None:
    """Seed, replay, and cohort in one narrated arc."""
    exec_id = seed()
    replay(exec_id)
    cohort(limit=3, export_json=COHORT_JSON_PATH, with_quality_judge=False)


def _parse_flag(argv: list[str], flag: str) -> tuple[list[str], str | None]:
    if flag not in argv:
        return argv, None
    index = argv.index(flag)
    if index + 1 >= len(argv):
        sys.exit(f"{flag} requires a value")
    value = argv[index + 1]
    remaining = argv[:index] + argv[index + 2 :]
    return remaining, value


def main(argv: list[str]) -> None:
    load_dotenv()
    command = argv[0] if argv else "run-all"
    rest = argv[1:]

    if command == "run":
        run()
    elif command == "seed":
        seed()
    elif command == "seed-cohort":
        rest, count_raw = _parse_flag(rest, "--count")
        count = int(count_raw or os.environ.get("COHORT_SEED_COUNT", "10"))
        seed_cohort(count=count)
    elif command == "replay":
        if not rest:
            if PROD_FIXTURE.is_file():
                rest = [PROD_FIXTURE.read_text(encoding="utf-8").strip()]
            else:
                sys.exit("usage: python demo.py replay <EXEC-ID>")
        replay(rest[0])
    elif command == "cohort":
        rest, export_json = _parse_flag(rest, "--export-json")
        if export_json is None:
            export_json = COHORT_JSON_PATH
        limit = int(os.environ.get("COHORT_LIMIT", "10"))
        with_judge = "--with-quality-judge" in rest
        cohort(limit=limit, export_json=export_json, with_quality_judge=with_judge)
    elif command == "run-all":
        run_all()
    else:
        sys.exit(
            f"unknown command {command!r}. "
            "try: seed | seed-cohort [--count N] | run | replay [EXEC-ID] "
            "| cohort | run-all"
        )


if __name__ == "__main__":
    main(sys.argv[1:])
