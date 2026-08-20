"""Target-neutral entrypoint vendored into generated evaluation projects."""

import json
import os
import sys
from pathlib import Path
from typing import Any

from kitaru.api_models.v1.evaluator import EvaluatorVersionResponse
from kitaru.api_models.v1.session_node import SessionWithNodesResponse

from ._sanitize import EphemeralSanitizer
from .evaluators import evaluate_session, load_evaluator
from .models import ExportError, MaterializedEvaluator, RewardSelector
from .trace import convert_trace

_MAX_SESSION_BYTES = 16 * 1024 * 1024
_MAX_METADATA_BYTES = 2 * 1024 * 1024
_RESULT_NAME = "result.json"


def _load_json(path: Path, *, max_bytes: int) -> Any:
    try:
        if path.stat().st_size > max_bytes:
            raise ExportError(
                "evaluator_input_too_large", "Evaluator input exceeds its fixed limit."
            )
        return json.loads(path.read_bytes())
    except ExportError:
        raise
    except (OSError, json.JSONDecodeError) as error:
        raise ExportError(
            "invalid_evaluator_input", "Evaluator input is not readable JSON."
        ) from error


def _write_result(value: dict[str, Any]) -> None:
    Path(_RESULT_NAME).write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    )


def _get_script_path(root: Path, value: object) -> Path | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ExportError(
            "invalid_evaluator_input", "Evaluator script path is invalid."
        )
    candidate = (root / value).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as error:
        raise ExportError(
            "invalid_evaluator_input", "Evaluator script path escapes its bundle."
        ) from error
    if not candidate.is_file():
        raise ExportError("invalid_evaluator_input", "Evaluator script is missing.")
    return candidate


def _run(task_path: Path, trace_path: Path, metadata_path: Path) -> dict[str, Any]:
    task = _load_json(task_path, max_bytes=_MAX_SESSION_BYTES)
    trace = _load_json(trace_path, max_bytes=_MAX_SESSION_BYTES)
    metadata = _load_json(metadata_path, max_bytes=_MAX_METADATA_BYTES)
    if not isinstance(task, dict) or not isinstance(trace, dict):
        raise ExportError(
            "invalid_evaluator_input", "Task context and trace must be JSON objects."
        )
    if not isinstance(metadata, list):
        raise ExportError(
            "invalid_evaluator_input", "Evaluator metadata must be a JSON array."
        )
    required_names = task.get("required_environment_names")
    if not isinstance(required_names, list) or not all(
        isinstance(name, str) for name in required_names
    ):
        raise ExportError(
            "invalid_evaluator_input", "Runtime environment inventory is invalid."
        )
    secrets = [os.environ[name] for name in required_names if name in os.environ]
    evaluator_root = metadata_path.parent / "evaluators"
    loaded = []
    for item in metadata:
        if not isinstance(item, dict):
            raise ExportError(
                "invalid_evaluator_input", "Evaluator metadata entry is invalid."
            )
        script_path = _get_script_path(evaluator_root, item.get("script_path"))
        evaluator = MaterializedEvaluator(
            name=item["name"],
            version=EvaluatorVersionResponse.model_validate(item["version"]),
            params=item["params"],
            script=script_path.read_bytes() if script_path is not None else None,
            source_sha256=item["source_sha256"],
        )
        loaded.append((evaluator, load_evaluator(evaluator, script_path=script_path)))
    selector_data = task.get("primary_reward")
    if not isinstance(selector_data, dict):
        raise ExportError(
            "invalid_evaluator_input", "Primary reward selector is invalid."
        )
    selector = RewardSelector(**selector_data)
    context = SessionWithNodesResponse.model_validate(task.get("context"))
    trace_format = task.get("trace_format")
    session = convert_trace(
        trace,
        format=trace_format,
        context=context,
        secret_values=secrets,
    )
    outcome = evaluate_session(loaded, selector, session, secret_values=secrets)
    return {"ok": True, "reward": outcome.reward, "metrics": outcome.metrics}


def main() -> int:
    """Evaluate one request and write a bounded machine-readable result."""
    if len(sys.argv) != 4:
        _write_result(
            {
                "ok": False,
                "code": "invalid_evaluator_input",
                "message": "Invalid evaluator worker arguments.",
            }
        )
        return 2
    task_path, trace_path, metadata_path = map(Path, sys.argv[1:])
    secrets: list[str] = []
    try:
        task = _load_json(task_path, max_bytes=_MAX_SESSION_BYTES)
        if isinstance(task, dict):
            names = task.get("required_environment_names", [])
            if isinstance(names, list):
                secrets = [
                    os.environ[name]
                    for name in names
                    if isinstance(name, str) and name in os.environ
                ]
        result = _run(task_path, trace_path, metadata_path)
    except ExportError as error:
        sanitizer = EphemeralSanitizer(secrets)
        _write_result(
            {
                "ok": False,
                "code": error.code,
                "message": str(sanitizer.sanitize(error.message)),
            }
        )
        return 1
    except Exception as error:
        sanitizer = EphemeralSanitizer(secrets)
        _write_result(
            {
                "ok": False,
                "code": "evaluator_failed",
                "message": str(sanitizer.sanitize(str(error))),
            }
        )
        return 1
    _write_result(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
