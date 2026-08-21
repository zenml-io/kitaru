"""Download a small, public DABstep development fixture for the V0 demo."""

import argparse
import json
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen

DATASET_ROOT = "https://huggingface.co/datasets/adyen/DABstep/resolve/main"
TREE_URL = "https://huggingface.co/api/datasets/adyen/DABstep/tree/main/data/context"
DEV_TASKS_URL = f"{DATASET_ROOT}/data/tasks/dev.jsonl?download=true"


def prepare_fixture(destination: Path, task_id: str) -> dict[str, Any]:
    """Create a public agent fixture and a separate private scorer fixture.

    Args:
        destination: Root directory for the disposable-demo inputs.
        task_id: Official DABstep development-task identifier.

    Returns:
        A small fixture manifest for the runbook.

    Raises:
        ValueError: The requested development task does not exist.
    """
    if destination.exists():
        raise ValueError(
            f"Refusing to overwrite existing fixture directory: {destination}. "
            "Choose a new destination."
        )
    tasks = _download_jsonl(DEV_TASKS_URL)
    task = next((item for item in tasks if _task_id(item) == task_id), None)
    if task is None:
        raise ValueError(f"DABstep development task {task_id!r} was not found")

    public_dir = destination / "public"
    private_dir = destination / "private"
    (public_dir / "context").mkdir(parents=True)
    private_dir.mkdir()

    for entry in _download_json(TREE_URL):
        path = entry.get("path")
        if not isinstance(path, str) or entry.get("type") != "file":
            continue
        relative_path = path.removeprefix("data/context/")
        target = public_dir / "context" / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(
            _download_bytes(f"{DATASET_ROOT}/{quote(path)}?download=true")
        )

    task_payload = {
        "task_id": task_id,
        "question": _task_text(task),
        "guidelines": task.get("guidelines", task.get("guideline", "")),
        "source": "adyen/DABstep:data/tasks/dev.jsonl",
    }
    (public_dir / "task.json").write_text(
        json.dumps(task_payload, indent=2) + "\n", encoding="utf-8"
    )
    (private_dir / "gold.json").write_text(
        json.dumps({"task_id": task_id, "answer": task.get("answer", "")}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    manifest = {
        "task_id": task_id,
        "public_dir": str(public_dir),
        "gold_path": str(private_dir / "gold.json"),
        "context_files": len(list((public_dir / "context").rglob("*"))),
    }
    (destination / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    return manifest


def _download_jsonl(url: str) -> list[dict[str, Any]]:
    lines = _download_bytes(url).decode().splitlines()
    return [json.loads(line) for line in lines if line]


def _download_json(url: str) -> list[dict[str, Any]]:
    value = json.loads(_download_bytes(url).decode())
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError(f"Expected a JSON list from {url}")
    return value


def _download_bytes(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "kitaru-dabstep-v0-example"})
    with urlopen(request, timeout=60) as response:
        return response.read()


def _task_id(task: dict[str, Any]) -> str:
    value = task.get("task_id", task.get("id"))
    return str(value) if value is not None else ""


def _task_text(task: dict[str, Any]) -> str:
    for key in ("question", "prompt", "task"):
        value = task.get(key)
        if isinstance(value, str) and value:
            return value
    raise ValueError("DABstep task has no question text")


def main() -> None:
    """Prepare one fixture from the public DABstep development split."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--task-id", default="1273")
    args = parser.parse_args()
    print(json.dumps(prepare_fixture(args.destination, args.task_id), indent=2))


if __name__ == "__main__":
    main()
