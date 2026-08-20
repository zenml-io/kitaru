"""Materialize the exact self-contained runtime bridge for generated targets."""

import hashlib
import importlib.metadata
from pathlib import Path

from .models import RuntimeBridgeReceipt

RUNTIME_BRIDGE_SCHEMA_VERSION = 1
_SOURCE_FILES = (
    ("_sanitize.py", "_sanitize.py"),
    ("evaluators.py", "evaluators.py"),
    ("models.py", "models.py"),
    ("trace.py", "trace.py"),
    ("_bridge_runtime.py", "runtime.py"),
)


def get_runtime_bridge_version() -> str:
    """Return the Kitaru version required by the generated runtime bridge."""
    return importlib.metadata.version("kitaru")


def materialize_runtime_bridge(destination: Path) -> RuntimeBridgeReceipt:
    """Write and identify the exact bridge bytes included in a generated bundle."""
    source_root = Path(__file__).parent
    destination.mkdir(parents=True, exist_ok=True)
    files: dict[str, str] = {}
    contents: list[tuple[str, bytes]] = [("__init__.py", b"")]
    contents.extend(
        (target_name, (source_root / source_name).read_bytes())
        for source_name, target_name in _SOURCE_FILES
    )
    aggregate = hashlib.sha256()
    for relative, content in sorted(contents):
        (destination / relative).write_bytes(content)
        digest = hashlib.sha256(content).hexdigest()
        files[relative] = digest
        aggregate.update(relative.encode("utf-8"))
        aggregate.update(b"\0")
        aggregate.update(content)
        aggregate.update(b"\n")
    return RuntimeBridgeReceipt(
        schema_version=RUNTIME_BRIDGE_SCHEMA_VERSION,
        sha256=aggregate.hexdigest(),
        originating_kitaru_version=get_runtime_bridge_version(),
        files=files,
    )
