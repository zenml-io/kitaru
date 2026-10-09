"""Run a fixed saved test-set version using fresh native delivery conversations."""

import argparse
import asyncio
import json
import os
from typing import Any
from uuid import UUID

from kitaru.client import KitaruAPIClient
from kitaru.client.exceptions import APIError

from .models import Scenario
from .persistence import get_runner_revision, record_result
from .runner import run_scenario
from .scenario_library import read_test_set

COMPLETE_STATUSES = {"completed", "boundary-completed"}


async def execute_test_set(
    client: KitaruAPIClient,
    cohort_version_id: UUID,
    *,
    variant: str = "fix",
    model: str = "gpt-6-luna",
    backend: str = "openai",
    record: bool = False,
    server_url: str | None = None,
    api_key: str | None = None,
) -> dict[str, Any]:
    """Validate every frozen case, then execute and report each fresh result."""
    if variant not in {"fix", "control"} or backend not in {"openai", "scripted"}:
        raise ValueError("Unknown test-set variant or backend")
    saved = await read_test_set(client, cohort_version_id)
    revision = get_runner_revision()
    if saved["runnerRevision"] != revision:
        raise ValueError("Test set uses a stale runner revision; save a new test set")
    if not saved["cases"]:
        raise ValueError(
            "A regression test set must contain at least one recorded case"
        )
    for case in saved["cases"]:
        if case["runnerRevision"] not in {None, revision}:
            raise ValueError("Recorded case uses a stale runner revision")
    agent_name = "delivery-date-demo"
    if record:
        agent_name = (await client.agents.get(UUID(saved["agentId"]))).name
    receipts = []
    for case in saved["cases"]:
        receipt = {
            "sourceSessionId": case["sourceSessionId"],
            "title": case["title"],
            "scenarioHash": case["scenarioHash"],
            "mode": case["mode"],
            "continueAfterBoundary": case["continueAfterBoundary"],
            "runnerRevision": revision,
            "runId": None,
            "sessionId": None,
            "status": "partial",
            "passed": False,
        }
        try:
            result = await run_scenario(
                Scenario.model_validate(case["scenarioSnapshot"]),
                variant=variant,
                mode=case["mode"],
                backend=backend,
                model=model,
                continue_after_boundary=case["continueAfterBoundary"],
            )
            complete = result.status in COMPLETE_STATUSES
            receipt.update(
                runId=str(result.run_id),
                status="partial"
                if not complete
                else "complete"
                if result.verdict.passed
                else "check-failed",
                passed=complete and result.verdict.passed,
                executionStatus=result.status,
                checks=result.verdict.model_dump(),
                result=result.model_dump(mode="json"),
            )
            if record:
                session_id = await record_result(
                    result,
                    server_url=server_url,
                    api_key=api_key,
                    agent_name=agent_name,
                    editor_snapshot=case["editorSnapshot"],
                    source_session_id=UUID(case["sourceSessionId"]),
                    title=case["title"],
                    continue_after_boundary=case["continueAfterBoundary"],
                    client=client,
                )
                receipt["sessionId"] = str(session_id)
        except (APIError, ValueError, OSError, RuntimeError) as exc:
            receipt.update(status="partial", passed=False, error=str(exc))
        receipts.append(receipt)
    status = (
        "partial"
        if any(case["status"] == "partial" for case in receipts)
        else "check-failed"
        if any(not case["passed"] for case in receipts)
        else "complete"
    )
    return {
        "cohortId": saved["cohortId"],
        "cohortVersionId": str(cohort_version_id),
        "name": saved["name"],
        "runnerRevision": revision,
        "configuration": {"variant": variant, "backend": backend, "model": model},
        "status": status,
        "passed": status == "complete",
        "cases": receipts,
    }


async def main() -> int:
    """Print an auditable receipt and fail CI on incomplete or failed checks."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort-version", type=UUID, required=True)
    parser.add_argument("--variant", choices=["fix", "control"], default="fix")
    parser.add_argument("--model", default="gpt-6-luna")
    parser.add_argument("--backend", choices=["openai", "scripted"], default="openai")
    parser.add_argument("--record", action="store_true")
    parser.add_argument(
        "--check", action="store_true", help="Checks are always enforced"
    )
    parser.add_argument("--server-url", default=os.environ.get("KITARU_SERVER_URL"))
    args = parser.parse_args()
    api_key = os.environ.get("KITARU_API_KEY")
    async with KitaruAPIClient(base_url=args.server_url, api_key=api_key) as client:
        receipt = await execute_test_set(
            client,
            args.cohort_version,
            variant=args.variant,
            model=args.model,
            backend=args.backend,
            record=args.record,
            server_url=args.server_url,
            api_key=api_key,
        )
    print(json.dumps(receipt, indent=2))
    return int(not receipt["passed"])


if __name__ == "__main__":
    os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")
    try:
        raise SystemExit(asyncio.run(main()))
    except (APIError, ValueError, OSError, RuntimeError) as exc:
        print(json.dumps({"status": "partial", "passed": False, "error": str(exc)}))
        raise SystemExit(1) from exc
