#!/usr/bin/env python
# ruff: noqa: E402,I001
"""Generate mandatory Langfuse traces for the reference-agent example."""

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from examples.end_to_end.replay_verify_reference_agent import db
from examples.end_to_end.replay_verify_reference_agent.config import (
    DEFAULT_AGENT_VERSION,
    EXAMPLE_DIR,
    FIXTURES_DIR,
    missing_trace_environment,
    load_scenarios,
    load_variants,
    select_scenarios,
)
from examples.end_to_end.replay_verify_reference_agent.graph import run_reference_agent
from examples.end_to_end.replay_verify_reference_agent.mock_api import MockApiServer


def main() -> int:
    """Run trace generation and write local fixture files."""
    args = parse_args()
    if args.validate_only:
        _validate_local_config(args)
        print("Reference-agent scenarios and variants are valid.")
        return 0

    missing = missing_trace_environment()
    if missing:
        print(
            "Trace generation requires live OpenAI and Langfuse credentials.\n"
            f"Missing: {', '.join(missing)}",
            file=sys.stderr,
        )
        return 2

    try:
        from langfuse import get_client
        from langfuse.langchain import CallbackHandler
    except ImportError as error:
        print(
            "Missing Langfuse dependency. Run with:\n"
            "  uv run --extra langgraph-openai --with langfuse "
            "examples/end_to_end/replay_verify_reference_agent/generate_traces.py",
            file=sys.stderr,
        )
        raise SystemExit(2) from error

    scenarios = select_scenarios(args.scenario_set, load_scenarios())
    variants = load_variants(_variant_names(args.variants))
    run_id = (
        args.run_id
        or f"fixture-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    )
    fixture_rows: list[dict[str, Any]] = []
    manifest_runs: list[dict[str, Any]] = []
    langfuse = get_client()

    try:
        with MockApiServer() as api:
            for variant in variants:
                for scenario in scenarios:
                    db.reset_database()
                    trace_id = uuid4().hex
                    metadata = {
                        "scenario_id": scenario.scenario_id,
                        "case_id": scenario.case_id,
                        "variant_name": variant.name,
                        "agent_version": DEFAULT_AGENT_VERSION,
                        "model": variant.model,
                        "prompt_profile": variant.prompt_profile,
                        "tool_policy_name": variant.tool_policy_name,
                        "tool_selection_mode": "llm_tool_calling",
                        "fixture_generation_run_id": run_id,
                    }
                    tags = [
                        "kitaru",
                        "replay-verify",
                        "reference-agent",
                        "stage-1",
                        variant.name,
                        scenario.scenario_id,
                    ]
                    trace_metadata = {**metadata, "tags": tags}
                    handler = CallbackHandler()
                    with langfuse.start_as_current_observation(
                        as_type="span",
                        name="reference-agent-scenario",
                        input={
                            "scenario_id": scenario.scenario_id,
                            "user_request": scenario.user_request,
                        },
                        metadata=trace_metadata,
                        trace_context={"trace_id": trace_id},
                    ) as root_span:
                        output = run_reference_agent(
                            scenario=scenario,
                            variant=variant,
                            db_path=db.DEFAULT_DB_PATH,
                            api_base_url=api.base_url,
                            kb_dir=EXAMPLE_DIR / "knowledge_base",
                            callbacks=[handler],
                            metadata=metadata,
                            tags=tags,
                        )
                        audit_log = db.get_audit_log()
                        output["audit_log"] = audit_log
                        root_span.update(output=output)
                    row = {
                        "trace_id": trace_id,
                        "metadata": metadata,
                        "tags": tags,
                        "input": {
                            "scenario_id": scenario.scenario_id,
                            "case_id": scenario.case_id,
                            "user_request": scenario.user_request,
                        },
                        "output": output,
                    }
                    fixture_rows.append(row)
                    manifest_runs.append(
                        {
                            "trace_id": trace_id,
                            "scenario_id": scenario.scenario_id,
                            "case_id": scenario.case_id,
                            "variant_name": variant.name,
                            "model": variant.model,
                            "prompt_profile": variant.prompt_profile,
                            "tool_policy_name": variant.tool_policy_name,
                            "audit_tool_names": [
                                item["tool_name"] for item in audit_log
                            ],
                        }
                    )
                    decision = output["decision"]
                    print(
                        f"{trace_id} | {variant.name} | {scenario.scenario_id} | "
                        f"{decision['required_action']} | writes={len(audit_log)}",
                        flush=True,
                    )
    finally:
        langfuse.flush()
        shutdown = getattr(langfuse, "shutdown", None)
        if callable(shutdown):
            shutdown()

    _write_fixture_files(
        run_id=run_id,
        fixture_rows=fixture_rows,
        manifest_runs=manifest_runs,
        scenario_count=len(scenarios),
        variant_names=[variant.name for variant in variants],
    )
    return 0


def parse_args() -> argparse.Namespace:
    """Parse trace-generation arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--variants",
        default="baseline,nano_trimmed_permissions,mini_tool_budget_2",
        help="Comma-separated variant names from variants/*.yaml.",
    )
    parser.add_argument(
        "--scenario-set",
        choices=["smoke", "full"],
        default="smoke",
        help="Run smoke scenarios or the full seeded scenario list.",
    )
    parser.add_argument("--run-id", help="Optional fixture_generation_run_id.")
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate scenarios and variants without OpenAI or Langfuse.",
    )
    return parser.parse_args()


def _validate_local_config(args: argparse.Namespace) -> None:
    scenarios = select_scenarios(args.scenario_set, load_scenarios())
    variants = load_variants(_variant_names(args.variants))
    if not scenarios:
        raise SystemExit("No scenarios selected")
    if not variants:
        raise SystemExit("No variants selected")


def _variant_names(value: str) -> list[str]:
    names = [item.strip() for item in value.split(",") if item.strip()]
    if not names:
        raise SystemExit("At least one variant is required")
    return names


def _write_fixture_files(
    *,
    run_id: str,
    fixture_rows: list[dict[str, Any]],
    manifest_runs: list[dict[str, Any]],
    scenario_count: int,
    variant_names: list[str],
) -> None:
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    export_path = FIXTURES_DIR / "langfuse_export.jsonl"
    with export_path.open("w", encoding="utf-8") as f:
        for row in fixture_rows:
            f.write(json.dumps(row, sort_keys=True) + "\n")

    manifest = {
        "fixture_generation_run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "agent_version": DEFAULT_AGENT_VERSION,
        "langfuse_base_url": _safe_langfuse_host(),
        "scenario_count": scenario_count,
        "variant_names": variant_names,
        "runs": manifest_runs,
    }
    manifest_path = FIXTURES_DIR / "trace_generation_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {export_path}")
    print(f"Wrote {manifest_path}")


def _safe_langfuse_host() -> str:
    from os import getenv

    host = getenv("LANGFUSE_BASE_URL", "").rstrip("/")
    if host == "https://cloud.langfuse.com":
        return host
    return "redacted"


if __name__ == "__main__":
    raise SystemExit(main())
