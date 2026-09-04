#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Generate canonical insight JSON from normalized Kitaru sessions."""

import argparse
import asyncio
import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from kitaru.api_models.v1.session_node import SessionWithNodesResponse
from kitaru.insights import (
    InsightGenerationConfig,
    InsightGenerationContext,
    generate_insights,
)
from kitaru.insights.generation import ModelGenerationConfig
from kitaru.insights.observability import GenerationObserver

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "devtools" / ".run" / "insights" / "result.json"
_SESSIONS = TypeAdapter(list[SessionWithNodesResponse])


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate post-import insights from normalized SessionWithNodesResponse "
            "JSON. This does not accept raw provider JSONL."
        )
    )
    parser.add_argument("--sessions", type=Path, required=True)
    parser.add_argument("--context", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--model",
        help=(
            "Enable the two-call OpenAI path with this model; omit for "
            "deterministic mode."
        ),
    )
    parser.add_argument(
        "--observe",
        action="store_true",
        help="Record metadata-only events using environment-configured Langfuse.",
    )
    parser.add_argument(
        "--production-derived",
        action="store_true",
        help="Mark input as production-derived and enforce a safe output location.",
    )
    return parser


def validate_output_path(path: Path, *, production_derived: bool) -> Path:
    """Require production-derived output inside the repo to be gitignored."""
    resolved = path.expanduser().resolve()
    if not production_derived or not resolved.is_relative_to(REPO_ROOT):
        return resolved
    ignored = subprocess.run(
        ["git", "check-ignore", "--quiet", str(resolved)],
        cwd=REPO_ROOT,
        check=False,
    )
    if ignored.returncode != 0:
        raise ValueError(
            "production-derived output inside the repository must be gitignored"
        )
    return resolved


def _load_json(path: Path) -> Any:
    if path.suffix.lower() == ".jsonl":
        raise ValueError("the harness accepts normalized session JSON, not JSONL")
    return json.loads(path.read_text())


def _load_sessions(path: Path) -> list[SessionWithNodesResponse]:
    payload = _load_json(path)
    if isinstance(payload, dict) and "sessions" in payload:
        payload = payload["sessions"]
    return _SESSIONS.validate_python(payload)


def _load_context(path: Path) -> InsightGenerationContext:
    return InsightGenerationContext.model_validate(_load_json(path))


def _build_model(model: str | None):
    if model is None:
        return None
    from kitaru.insights.openai_generator import OpenAIInsightGenerator

    return OpenAIInsightGenerator()


def _build_observer(enabled: bool) -> GenerationObserver | None:
    if not enabled:
        return None
    from kitaru.insights.observability import LangfuseGenerationObserver

    try:
        return LangfuseGenerationObserver()
    except Exception:
        return None


def _write_result_atomic(path: Path, payload: str) -> None:
    """Atomically replace the output without exposing a partial JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary.write(payload)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_path = Path(temporary.name)
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


async def run(argv: list[str] | None = None) -> int:
    """Run the harness and write one validated canonical result."""
    args = _parser().parse_args(argv)
    output = validate_output_path(
        args.output, production_derived=args.production_derived
    )
    sessions = _load_sessions(args.sessions)
    context = _load_context(args.context)
    result = await generate_insights(
        sessions,
        context=context,
        config=InsightGenerationConfig(
            model=ModelGenerationConfig(model=args.model) if args.model else None
        ),
        generator=_build_model(args.model),
        observer=_build_observer(args.observe),
    )
    _write_result_atomic(
        output,
        json.dumps(
            result.model_dump(mode="json"),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    print(output)
    return 0


def main() -> int:
    """Run the asynchronous harness from a synchronous command entry point."""
    return asyncio.run(run())


if __name__ == "__main__":
    raise SystemExit(main())
