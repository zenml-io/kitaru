"""Compatibility launcher for the TypeScript-managed Mastra demo."""

import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path

from kitaru.api_models.v1.evaluation import EvaluationResponse
from kitaru.api_models.v1.replay import ReplayResponse
from kitaru.api_models.v1.session_node import SessionNodeResponse

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_DIR = Path(__file__).resolve().parent
COMPILED_DEMO = EXAMPLE_DIR / "dist" / "demo.js"
RESULT_PREFIX = "KITARU_DEMO_RESULT "


@dataclass(frozen=True)
class DemoResult:
    """Observed record/replay proof returned to compatibility callers."""

    initial_session_id: str
    replay: ReplayResponse
    result_session_id: str
    initial_outbox_count: int
    replay_outbox_count: int
    initial_nodes: list[SessionNodeResponse]
    replay_nodes: list[SessionNodeResponse]
    evaluations: list[EvaluationResponse]
    state_dir: Path


def _require_nonempty_text(outputs: object) -> str:
    """Return non-empty recorded output text."""
    if not isinstance(outputs, dict):
        raise RuntimeError("The baseline session did not record output text")
    text = outputs.get("text")
    if not isinstance(text, str) or not text.strip():
        raise RuntimeError("The baseline session recorded empty output text")
    return text


def _parse_result(stdout: str) -> DemoResult:
    lines = [line for line in stdout.splitlines() if line.startswith(RESULT_PREFIX)]
    if len(lines) != 1:
        raise RuntimeError("The TypeScript demo did not emit one result record")
    payload = json.loads(lines[0][len(RESULT_PREFIX) :])
    return DemoResult(
        initial_session_id=payload["initial_session_id"],
        replay=ReplayResponse.model_validate(payload["replay"]),
        result_session_id=payload["result_session_id"],
        initial_outbox_count=payload["initial_outbox_count"],
        replay_outbox_count=payload["replay_outbox_count"],
        initial_nodes=[
            SessionNodeResponse.model_validate(node)
            for node in payload["initial_nodes"]
        ],
        replay_nodes=[
            SessionNodeResponse.model_validate(node) for node in payload["replay_nodes"]
        ],
        evaluations=[
            EvaluationResponse.model_validate(item) for item in payload["evaluations"]
        ],
        state_dir=Path(payload["state_dir"]),
    )


async def run_demo(
    *,
    api_url: str | None = None,
    api_key: str = "",
    state_dir: Path | None = None,
    test_model: bool = False,
) -> DemoResult:
    """Launch the compiled TypeScript management workflow."""
    if not COMPILED_DEMO.exists():
        raise RuntimeError(
            "The compiled TypeScript driver is missing. Run: "
            "pnpm --filter @zenml-io/kitaru-example-mastra-support-triage build"
        )
    if api_key:
        raise RuntimeError(
            "The TypeScript demo does not accept a bridged API key; run `kitaru login`"
        )
    command = ["node", str(COMPILED_DEMO)]
    if api_url:
        command.extend(["--api-url", api_url])
    if state_dir is not None:
        command.extend(["--state-root", str(state_dir)])
    if test_model:
        command.extend(["--test-model", "--unauthenticated"])
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_bytes, stderr_bytes = await process.communicate()
    stdout = stdout_bytes.decode()
    if process.returncode != 0:
        detail = stderr_bytes.decode().strip() or stdout.strip()
        raise RuntimeError(
            "The TypeScript Mastra demo exited with code "
            f"{process.returncode}: {detail}"
        )
    return _parse_result(stdout)


def _print_result(result: DemoResult) -> None:
    print(f"state_dir={result.state_dir}")
    print(f"initial_session_id={result.initial_session_id}")
    print(f"replay_id={result.replay.id}")
    print(f"result_session_id={result.result_session_id}")
    print(f"outbox_after_record={result.initial_outbox_count}")
    print(f"outbox_after_history_replay={result.replay_outbox_count}")


async def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-model", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    result = await run_demo(test_model=args.test_model)
    _print_result(result)


if __name__ == "__main__":
    asyncio.run(_main())
