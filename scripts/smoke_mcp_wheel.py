#!/usr/bin/env python3
#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Verify base and MCP contracts from one built Kitaru wheel."""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


class SmokeFailure(RuntimeError):
    """Raised when an installed-wheel contract fails."""


def _run(arguments: list[str], *, environment: dict[str, str] | None = None):
    return subprocess.run(
        arguments,
        env=environment,
        capture_output=True,
        text=True,
        timeout=120,
    )


def _expect(
    label: str, result: subprocess.CompletedProcess[str], code: int = 0
) -> None:
    if result.returncode != code:
        raise SmokeFailure(
            f"{label}: expected exit {code}, got {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def _resolve_wheel(path: Path) -> Path:
    if path.is_file() and path.suffix == ".whl":
        return path.resolve()
    wheels = sorted(path.glob("*.whl")) if path.is_dir() else []
    if len(wheels) != 1:
        raise SmokeFailure(
            f"Expected exactly one wheel under {path}, found {len(wheels)}"
        )
    return wheels[0].resolve()


def _environment_paths(root: Path) -> tuple[Path, Path]:
    scripts = root / ("Scripts" if os.name == "nt" else "bin")
    return scripts / ("python.exe" if os.name == "nt" else "python"), scripts


def _create_environment(uv: str, root: Path) -> tuple[Path, Path]:
    result = _run([uv, "venv", "--python", sys.executable, str(root)])
    _expect(f"create environment {root.name}", result)
    python, scripts = _environment_paths(root)
    return python, scripts


def _install(uv: str, python: Path, wheel: Path, *, extra: str | None = None) -> None:
    requirement = str(wheel) if extra is None else f"{wheel}[{extra}]"
    result = _run([uv, "pip", "install", "--python", str(python), requirement])
    _expect(f"install wheel ({extra or 'base'})", result)


def _smoke_base(uv: str, root: Path, wheel: Path) -> None:
    python, scripts = _create_environment(uv, root)
    _install(uv, python, wheel)
    console = scripts / ("kitaru-mcp.exe" if os.name == "nt" else "kitaru-mcp")
    imports = _run(
        [
            str(python),
            "-c",
            "import importlib.util; import kitaru; import kitaru.client; "
            "assert importlib.util.find_spec('mcp') is None",
        ]
    )
    _expect("base imports without MCP", imports)
    for argument in ("--help", "--version"):
        result = _run([str(console), argument])
        _expect(f"base {argument}", result)
    result = _run([str(console)])
    _expect("base missing-extra startup", result, code=2)
    if result.stdout or "pip install 'kitaru[mcp]'" not in result.stderr:
        raise SmokeFailure(
            "base missing-extra startup did not emit the stderr-only install hint"
        )


def _smoke_mcp(uv: str, root: Path, wheel: Path, repository: Path) -> None:
    python, scripts = _create_environment(uv, root)
    _install(uv, python, wheel, extra="mcp")
    console = scripts / ("kitaru-mcp.exe" if os.name == "nt" else "kitaru-mcp")
    result = _run(
        [str(python), str(repository / "scripts" / "probe_mcp_wheel.py"), str(console)]
    )
    _expect("installed MCP stdio protocol", result)
    if "read-only, standard, and destructive" not in result.stdout:
        raise SmokeFailure("installed MCP probe did not report all capability modes")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "wheel",
        nargs="?",
        type=Path,
        default=Path("dist"),
        help="Wheel file or directory containing exactly one wheel (default: dist)",
    )
    arguments = parser.parse_args()
    repository = Path(__file__).resolve().parents[1]
    uv = shutil.which("uv")
    if uv is None:
        print("uv is required for the MCP wheel smoke", file=sys.stderr)
        return 1
    try:
        wheel = _resolve_wheel(arguments.wheel)
        with tempfile.TemporaryDirectory(prefix="kitaru-mcp-wheel-") as directory:
            root = Path(directory)
            _smoke_base(uv, root / "base", wheel)
            _smoke_mcp(uv, root / "mcp", wheel, repository)
    except (SmokeFailure, subprocess.TimeoutExpired) as error:
        print(f"MCP wheel smoke failed: {error}", file=sys.stderr)
        return 1
    print(f"MCP wheel smoke passed: {wheel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
