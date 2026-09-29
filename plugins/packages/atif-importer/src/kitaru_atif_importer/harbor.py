"""Collect local Harbor trajectories and selected trial results for import."""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

MAX_BYTES = 90 * 1024 * 1024
MAX_RECORDS = 10_000
_RESULT_FIELDS = (
    "id",
    "task_name",
    "trial_name",
    "task_checksum",
    "source",
    "agent_info",
    "verifier_result",
    "exception_info",
    "started_at",
    "finished_at",
    "agent_execution",
)


def collect_trajectories(
    directory: Path, *, max_bytes: int = MAX_BYTES
) -> dict[str, Any]:
    """Collect a job or trial directory without following external references.

    Configurations and agent-result metadata are excluded from Harbor results because
    they can contain credentials. Trajectory content is preserved unchanged.
    Each multi-step trajectory retains its step outcome and enclosing trial ID.

    Args:
        directory: Local Harbor job, trial, or collection of jobs.
        max_bytes: Maximum combined source-file bytes.

    Returns:
        An envelope accepted by the ATIF parser.

    Raises:
        ValueError: No trajectories, invalid files, symlinks, or excessive size.
    """
    root = directory.resolve()
    if not root.is_dir():
        raise ValueError("Harbor input must be a directory")
    paths: list[Path] = []
    for parent, directories, files in os.walk(root, followlinks=False):
        directories[:] = sorted(
            name for name in directories if not (Path(parent) / name).is_symlink()
        )
        if Path(parent).name == "agent" and "trajectory.json" in files:
            paths.append(Path(parent) / "trajectory.json")
            if len(paths) > MAX_RECORDS:
                raise ValueError(f"Harbor record limit exceeded ({MAX_RECORDS})")
    if not paths:
        raise ValueError("No agent/trajectory.json files found")

    remaining = max_bytes

    def read_object(path: Path) -> dict[str, Any]:
        nonlocal remaining
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            raise ValueError(f"Refusing symlink or external file: {path.name}")
        with path.open("rb") as stream:
            content = stream.read(remaining + 1)
        remaining -= len(content)
        if remaining < 0:
            raise ValueError(f"Harbor source size limit exceeded ({max_bytes} bytes)")
        try:
            value = json.loads(content)
        except (ValueError, UnicodeDecodeError) as exc:
            raise ValueError(f"Invalid JSON in {path.relative_to(root)}") from exc
        if not isinstance(value, dict):
            raise ValueError(f"Expected JSON object in {path.relative_to(root)}")
        return value

    records: list[dict[str, Any]] = []
    results: dict[Path, dict[str, Any]] = {}
    for path in sorted(paths):
        trajectory = read_object(path)
        trial = path.parent.parent
        step_name: str | None = None
        if trial.parent.name == "steps":
            step_name = trial.name
            trial = trial.parent.parent
        result_path = trial / "result.json"
        if result_path not in results:
            results[result_path] = (
                read_object(result_path) if result_path.exists() else {}
            )
        raw_result = results[result_path]
        result = {key: raw_result[key] for key in _RESULT_FIELDS if key in raw_result}
        if step_name is not None:
            matches = [
                step
                for step in (raw_result.get("step_results") or [])
                if isinstance(step, dict) and step.get("step_name") == step_name
            ]
            if len(matches) > 1:
                raise ValueError(f"Duplicate Harbor step result: {step_name}")
            result["step_name"] = step_name
            # Trial-level failure/timing must not be attributed to every step.
            for key in ("exception_info", "verifier_result", "agent_execution"):
                result.pop(key, None)
                if matches and key in matches[0]:
                    result[key] = matches[0][key]
            result.pop("started_at", None)
            result.pop("finished_at", None)
        trial_id = raw_result.get("id")
        source_id = (
            f"{trial_id}/{path.relative_to(trial).as_posix()}"
            if isinstance(trial_id, str) and trial_id
            else path.relative_to(root).as_posix()
        )
        records.append(
            {
                "trajectory": trajectory,
                "harbor_result": result,
                "source_id": source_id,
            }
        )
    return {"trajectories": records}


def main(argv: list[str] | None = None) -> int:
    """Write a Harbor import envelope to a new local file."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        envelope = collect_trajectories(args.directory)
        content = json.dumps(envelope, ensure_ascii=True, allow_nan=False).encode()
        if len(content) > MAX_BYTES:
            raise ValueError(
                "Serialized bundle exceeds size limit; select fewer trials"
            )
        # Exclusive creation protects previous bundles from accidental replacement.
        descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(content)
        except OSError:
            # Remove only a file created by this attempt so a retry can succeed.
            args.output.unlink(missing_ok=True)
            raise
    except (OSError, ValueError) as exc:
        print(f"Cannot prepare Harbor import: {exc}", file=sys.stderr)
        return 1
    print(f"Prepared {len(envelope['trajectories'])} trajectories in {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
