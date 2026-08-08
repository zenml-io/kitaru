#!/usr/bin/env python3
#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Verify optional adapters from an installed Kitaru wheel."""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


class SmokeFailure(RuntimeError):
    """Raised when an installed adapter contract fails."""


def _run(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    """Run one bounded artifact command."""
    return subprocess.run(
        arguments,
        capture_output=True,
        text=True,
        timeout=120,
    )


def _expect(label: str, result: subprocess.CompletedProcess[str]) -> None:
    """Require one artifact command to succeed."""
    if result.returncode != 0:
        raise SmokeFailure(
            f"{label}: expected exit 0, got {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def _resolve_wheel(path: Path) -> Path:
    """Resolve one wheel file from a file or directory argument."""
    if path.is_file() and path.suffix == ".whl":
        return path.resolve()
    wheels = sorted(path.glob("*.whl")) if path.is_dir() else []
    if len(wheels) != 1:
        raise SmokeFailure(
            f"Expected exactly one wheel under {path}, found {len(wheels)}"
        )
    return wheels[0].resolve()


def _get_python(root: Path) -> Path:
    """Return the Python executable path for a virtual environment."""
    scripts = root / ("Scripts" if os.name == "nt" else "bin")
    return scripts / ("python.exe" if os.name == "nt" else "python")


def _create_environment(uv: str, root: Path) -> Path:
    """Create an isolated virtual environment."""
    result = _run([uv, "venv", "--python", sys.executable, str(root)])
    _expect(f"create environment {root.name}", result)
    return _get_python(root)


def _install(uv: str, python: Path, wheel: Path, *, extra: str | None = None) -> None:
    """Install the wheel with an optional extra."""
    requirement = str(wheel) if extra is None else f"{wheel}[{extra}]"
    result = _run([uv, "pip", "install", "--python", str(python), requirement])
    _expect(f"install wheel ({extra or 'base'})", result)


def _smoke_base(uv: str, root: Path, wheel: Path) -> None:
    """Verify the base wheel contains adapter code without its dependency."""
    python = _create_environment(uv, root)
    _install(uv, python, wheel)
    result = _run(
        [
            str(python),
            "-c",
            "import importlib.metadata, importlib.util; import kitaru.adapters; "
            "assert importlib.util.find_spec('pydantic_ai') is None; "
            "assert importlib.util.find_spec('kitaru.adapters.pydantic_ai') "
            "is not None; "
            "assert 'pydantic-ai' in "
            "importlib.metadata.metadata('kitaru').get_all('Provides-Extra')",
        ]
    )
    _expect("base adapter boundary", result)


def _smoke_pydantic_ai(uv: str, root: Path, wheel: Path) -> None:
    """Verify the PydanticAI extra exposes the documented adapter import."""
    python = _create_environment(uv, root)
    _install(uv, python, wheel, extra="pydantic-ai")
    result = _run(
        [
            str(python),
            "-c",
            "from pydantic_ai import Agent; "
            "from pydantic_ai.agent import WrapperAgent; "
            "from pydantic_ai.models.test import TestModel; "
            "from kitaru.adapters.pydantic_ai import KitaruAgent; "
            "wrapped = KitaruAgent(Agent(TestModel(call_tools=[]))); "
            "assert isinstance(wrapped, WrapperAgent)",
        ]
    )
    _expect("PydanticAI adapter import and construction", result)


def main() -> int:
    """Run clean base and PydanticAI wheel installation checks."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "wheel",
        nargs="?",
        type=Path,
        default=Path("dist"),
        help="Wheel file or directory containing exactly one wheel (default: dist)",
    )
    arguments = parser.parse_args()
    uv = shutil.which("uv")
    if uv is None:
        print("uv is required for the adapter wheel smoke", file=sys.stderr)
        return 1
    try:
        wheel = _resolve_wheel(arguments.wheel)
        with tempfile.TemporaryDirectory(prefix="kitaru-adapter-wheel-") as directory:
            root = Path(directory)
            _smoke_base(uv, root / "base", wheel)
            _smoke_pydantic_ai(uv, root / "pydantic-ai", wheel)
    except (SmokeFailure, subprocess.TimeoutExpired) as error:
        print(f"Adapter wheel smoke failed: {error}", file=sys.stderr)
        return 1
    print(f"Adapter wheel smoke passed: {wheel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
