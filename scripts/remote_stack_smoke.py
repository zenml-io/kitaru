"""Remote-stack smoke helper for the release smoke harness.

The shell harness owns login, option parsing, structured result recording, and
cleanup. This helper owns the Python pieces that are awkward to write safely in
shell: stack-shape classification and one flow execution with readback checks.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any, Protocol

from kitaru import KitaruClient
from kitaru._client._models import ExecutionStatus
from kitaru._remote_smoke_flow import (
    build_remote_smoke_marker,
    remote_stack_release_smoke,
)

_REMOTE_STORAGE_BACKENDS = {
    "abfs",
    "abfss",
    "az",
    "azure",
    "azure_blob",
    "azureml",
    "gcs",
    "s3",
}


class RemoteSmokeError(RuntimeError):
    """Raised when a remote smoke check fails."""


class FlowHandleLike(Protocol):
    """Subset of ``FlowHandle`` used by this helper."""

    @property
    def exec_id(self) -> str: ...

    @property
    def status(self) -> Any: ...

    def get(self) -> Any: ...


class RunnableFlowLike(Protocol):
    """Subset of a Kitaru flow wrapper used by this helper."""

    def run(self, *args: Any, **kwargs: Any) -> FlowHandleLike: ...


@dataclass(frozen=True)
class StackValidation:
    """Sanitized result of checking a stack's shape."""

    valid: bool
    reason: str
    evidence: dict[str, Any]


def _status_value(status: Any) -> str:
    """Return a stable public status string from enum-like values."""
    return str(getattr(status, "value", status)).lower()


def _is_successful_status(status: Any) -> bool:
    """Return whether a status-like object means successful completion."""
    if isinstance(status, ExecutionStatus):
        return status.is_successful
    return _status_value(status) == ExecutionStatus.COMPLETED.value


def _is_finished_status(status: Any) -> bool:
    """Return whether a status-like object is terminal."""
    if isinstance(status, ExecutionStatus):
        return status.is_finished
    return _status_value(status) in {
        ExecutionStatus.COMPLETED.value,
        ExecutionStatus.FAILED.value,
        ExecutionStatus.CANCELLED.value,
    }


def _component_backend(component: dict[str, Any], role: str) -> str | None:
    """Return the first backend for a component role from stack-show JSON."""
    if component.get("role") != role:
        return None
    backend = component.get("backend")
    if not isinstance(backend, str):
        return None
    normalized = backend.strip().lower()
    return normalized or None


def _component_backends(item: dict[str, Any], role: str) -> list[str]:
    """Return normalized component backends for one role."""
    components = item.get("components")
    if not isinstance(components, list):
        return []
    backends: list[str] = []
    for component in components:
        if not isinstance(component, dict):
            continue
        backend = _component_backend(component, role)
        if backend is not None:
            backends.append(backend)
    return backends


def validate_stack_show_payload(
    payload: dict[str, Any],
    *,
    category: str,
) -> StackValidation:
    """Validate sanitized ``kitaru stack show --output json`` payload shape."""
    if payload.get("command") != "stack.show":
        return StackValidation(
            valid=False,
            reason="expected command 'stack.show'",
            evidence={"category": category},
        )

    item = payload.get("item")
    if not isinstance(item, dict):
        return StackValidation(
            valid=False,
            reason="stack.show payload is missing object field 'item'",
            evidence={"category": category},
        )

    stack_type = item.get("stack_type")
    stack_type_value = stack_type.strip().lower() if isinstance(stack_type, str) else ""
    runner_backends = _component_backends(item, "runner")
    storage_backends = _component_backends(item, "storage")
    evidence = {
        "category": category,
        "stack_type": stack_type_value or None,
        "runner_backend": runner_backends[0] if runner_backends else None,
        "storage_backend": storage_backends[0] if storage_backends else None,
    }

    if category == "kubernetes":
        if stack_type_value != "kubernetes":
            return StackValidation(
                valid=False,
                reason="expected stack_type 'kubernetes'",
                evidence=evidence,
            )
        if "kubernetes" not in runner_backends:
            return StackValidation(
                valid=False,
                reason="expected a runner component with backend 'kubernetes'",
                evidence=evidence,
            )
        return StackValidation(valid=True, reason="", evidence=evidence)

    if category == "local-remote-artifact":
        if "local" not in runner_backends:
            return StackValidation(
                valid=False,
                reason="expected a runner component with backend 'local'",
                evidence=evidence,
            )
        remote_storage = next(
            (
                backend
                for backend in storage_backends
                if backend in _REMOTE_STORAGE_BACKENDS
            ),
            None,
        )
        if remote_storage is None:
            return StackValidation(
                valid=False,
                reason="expected a remote storage backend such as s3, gcs, or azure",
                evidence=evidence,
            )
        evidence = {**evidence, "storage_backend": remote_storage}
        return StackValidation(valid=True, reason="", evidence=evidence)

    return StackValidation(
        valid=False,
        reason=f"unsupported stack category {category!r}",
        evidence={"category": category},
    )


def load_stack_show_payload(raw_json: str) -> dict[str, Any]:
    """Parse a stack-show JSON document and require an object payload."""
    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise RemoteSmokeError(
            f"stack.show output was not valid JSON: {exc.msg}"
        ) from exc
    if not isinstance(payload, dict):
        raise RemoteSmokeError("stack.show output must be a JSON object")
    return payload


def wait_for_terminal_result(handle: FlowHandleLike, *, timeout_seconds: int) -> Any:
    """Poll a flow handle until it finishes, then return the persisted result."""
    deadline = time.monotonic() + timeout_seconds
    last_status: Any = "unknown"
    while time.monotonic() < deadline:
        last_status = handle.status
        if _is_finished_status(last_status):
            if not _is_successful_status(last_status):
                status_value = _status_value(last_status)
                raise RemoteSmokeError(
                    f"execution reached unsuccessful terminal status: {status_value}"
                )
            return handle.get()
        time.sleep(1)
    raise RemoteSmokeError(
        "execution timed out before terminal status "
        f"(last status: {_status_value(last_status)})"
    )


def _log_readback_command(exec_id: str) -> list[str]:
    """Build a child Python command that fetches logs and prints JSON evidence."""
    code = (
        "import json, sys; "
        "from kitaru import KitaruClient; "
        "logs = KitaruClient().executions.logs(sys.argv[1], source='step', limit=20); "
        "print(json.dumps({'count': len(logs)}))"
    )
    return [sys.executable, "-c", code, exec_id]


def run_log_readback_with_timeout(
    exec_id: str,
    *,
    log_timeout: int,
    command: list[str] | None = None,
) -> int:
    """Read execution logs in a killable child process and return entry count."""
    try:
        completed = subprocess.run(
            command or _log_readback_command(exec_id),
            capture_output=True,
            text=True,
            timeout=log_timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RemoteSmokeError(
            "execution log lookup exceeded the configured timeout"
        ) from exc

    if completed.returncode != 0:
        message = (completed.stderr or completed.stdout or "log process failed").strip()
        raise RemoteSmokeError(f"execution log lookup failed: {message}")

    try:
        result = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RemoteSmokeError(
            "execution log lookup failed: invalid JSON result"
        ) from exc
    count = result.get("count") if isinstance(result, dict) else None
    if not isinstance(count, int):
        raise RemoteSmokeError("execution log lookup failed: invalid result count")
    return count


def run_remote_flow_check(
    *,
    stack: str,
    category: str,
    image: str | None,
    execution_timeout: int,
    log_timeout: int,
    run_prefix: str,
    flow: RunnableFlowLike = remote_stack_release_smoke,
    client_factory: Any = KitaruClient,
    log_command: list[str] | None = None,
) -> dict[str, Any]:
    """Run the remote smoke flow and return sanitized evidence."""
    marker = build_remote_smoke_marker(run_prefix)
    run_kwargs: dict[str, Any] = {"stack": stack, "cache": False}
    if image:
        run_kwargs["image"] = image

    handle = flow.run(marker, **run_kwargs)
    exec_id = handle.exec_id
    result = wait_for_terminal_result(handle, timeout_seconds=execution_timeout)
    if result != marker:
        raise RemoteSmokeError("flow result marker did not match the submitted marker")

    client = client_factory()
    try:
        execution = client.executions.get(exec_id)
    except Exception as exc:
        raise RemoteSmokeError(f"execution lookup failed: {exc}") from exc
    execution_status = getattr(execution, "status", None)
    if not _is_successful_status(execution_status):
        raise RemoteSmokeError(
            "execution lookup did not report completed status "
            f"(got {_status_value(execution_status)})"
        )

    try:
        artifacts = client.artifacts.list(exec_id, limit=1)
    except Exception as exc:
        raise RemoteSmokeError(f"artifact listing failed: {exc}") from exc
    if not artifacts:
        raise RemoteSmokeError(
            "execution produced no artifacts visible to KitaruClient"
        )

    artifact = artifacts[0]
    artifact_id = getattr(artifact, "artifact_id", None)
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RemoteSmokeError("first artifact was missing a stable artifact_id")
    try:
        client.artifacts.get(artifact_id)
    except Exception as exc:
        raise RemoteSmokeError(f"artifact lookup failed: {exc}") from exc

    log_entry_count = run_log_readback_with_timeout(
        exec_id,
        log_timeout=log_timeout,
        command=log_command,
    )

    return {
        "artifact_count": len(artifacts),
        "artifact_get_succeeded": True,
        "category": category,
        "exec_id": exec_id,
        "flow_result_marker_matched": True,
        "log_entry_count": log_entry_count,
        "log_readback_succeeded": True,
        "status": _status_value(execution_status),
    }


def _emit_json(payload: dict[str, Any], *, stream: Any = sys.stdout) -> None:
    json.dump(payload, stream, indent=2, sort_keys=True)
    stream.write("\n")


def _validate_stack_command(args: argparse.Namespace) -> int:
    raw_json = sys.stdin.read()
    payload = load_stack_show_payload(raw_json)
    validation = validate_stack_show_payload(payload, category=args.category)
    result = {
        "category": args.category,
        "evidence": validation.evidence,
        "valid": validation.valid,
    }
    if validation.valid:
        _emit_json(result)
        return 0
    _emit_json({**result, "reason": validation.reason}, stream=sys.stderr)
    return 1


def _run_flow_command(args: argparse.Namespace) -> int:
    evidence = run_remote_flow_check(
        stack=args.stack,
        category=args.category,
        image=args.image,
        execution_timeout=args.timeout,
        log_timeout=args.log_timeout,
        run_prefix=args.run_prefix,
    )
    _emit_json({"evidence": evidence})
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the helper CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser(
        "validate-stack",
        help="Validate stack.show JSON from stdin for a remote smoke category.",
    )
    validate.add_argument(
        "--category",
        choices=("kubernetes", "local-remote-artifact"),
        required=True,
    )
    validate.set_defaults(func=_validate_stack_command)

    run_flow = subparsers.add_parser(
        "run-flow",
        help="Submit the private remote smoke flow and verify readback evidence.",
    )
    run_flow.add_argument("--stack", required=True)
    run_flow.add_argument(
        "--category",
        choices=("kubernetes", "local-remote-artifact"),
        required=True,
    )
    run_flow.add_argument("--image")
    run_flow.add_argument("--timeout", type=int, default=900)
    run_flow.add_argument("--log-timeout", type=int, default=60)
    run_flow.add_argument("--run-prefix", default="kitaru-remote-smoke")
    run_flow.set_defaults(func=_run_flow_command)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the helper CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except RemoteSmokeError as exc:
        _emit_json({"error": {"message": str(exc)}}, stream=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
