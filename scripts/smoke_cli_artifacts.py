"""Smoke-test CLI optional dependencies from built wheel and sdist artifacts."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

_SUBPROCESS_TIMEOUT_SECONDS = 300
_SCRUBBED_ENVIRONMENT_VARIABLES = {
    "CONDA_DEFAULT_ENV",
    "CONDA_PREFIX",
    "PIPENV_ACTIVE",
    "POETRY_ACTIVE",
    "PYTHONHOME",
    "PYTHONPATH",
    "UV_ACTIVE",
    "UV_PROJECT_ENVIRONMENT",
    "VIRTUAL_ENV",
    "VIRTUAL_ENV_PROMPT",
}
_WORKER_IMPORTS = (
    "import kitaru; import kitaru.cli; from kitaru.worker import Worker, WorkerConfig"
)
_WORKER_ENVIRONMENT = {
    "KITARU_API_URL": "http://127.0.0.1:9",
    "KITARU_API_KEY": "artifact-smoke-placeholder",
}


class SmokeFailure(RuntimeError):
    """Report one failed artifact smoke assertion."""


def _isolated_environment(root: Path) -> dict[str, str]:
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("KITARU_")
        and name not in _SCRUBBED_ENVIRONMENT_VARIABLES
    }
    directories = {
        "HOME": root / "home",
        "XDG_CONFIG_HOME": root / "xdg" / "config",
        "XDG_CACHE_HOME": root / "xdg" / "cache",
        "XDG_DATA_HOME": root / "xdg" / "data",
        "XDG_STATE_HOME": root / "xdg" / "state",
    }
    if os.name == "nt":
        directories.update(
            {
                "APPDATA": root / "windows" / "roaming",
                "LOCALAPPDATA": root / "windows" / "local",
                "USERPROFILE": root / "home",
            }
        )
    for name, directory in directories.items():
        directory.mkdir(parents=True, exist_ok=True)
        environment[name] = str(directory)
    assert _SCRUBBED_ENVIRONMENT_VARIABLES.isdisjoint(environment)
    return environment


def _run(
    command: list[str | Path], *, environment: dict[str, str], cwd: Path
) -> subprocess.CompletedProcess[str]:
    parts = [str(part) for part in command]
    try:
        return subprocess.run(
            parts,
            cwd=cwd,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=_SUBPROCESS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as error:
        raise SmokeFailure(
            f"command timed out after {_SUBPROCESS_TIMEOUT_SECONDS} seconds: "
            f"{' '.join(parts)}"
        ) from error


def _output(result: subprocess.CompletedProcess[str]) -> str:
    output = result.stderr.strip() or result.stdout.strip()
    return f": {output}" if output else ""


def _expect_success(
    label: str, result: subprocess.CompletedProcess[str]
) -> subprocess.CompletedProcess[str]:
    if result.returncode != 0:
        raise SmokeFailure(
            f"{label}: expected exit 0, got {result.returncode}{_output(result)}"
        )
    return result


def _expect_json_command(
    label: str, result: subprocess.CompletedProcess[str], command: str
) -> None:
    _expect_success(label, result)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise SmokeFailure(
            f"{label}: expected JSON on stdout{_output(result)}"
        ) from error
    if payload.get("command") != command:
        raise SmokeFailure(f"{label}: expected command {command!r} in JSON output")


def _expect_error_kind(
    label: str,
    result: subprocess.CompletedProcess[str],
    expected_kind: str,
) -> None:
    if result.returncode != 2:
        raise SmokeFailure(
            f"{label}: expected error.kind {expected_kind!r} with exit 2, "
            f"got exit {result.returncode}{_output(result)}"
        )
    try:
        payload = json.loads(result.stderr)
        actual_kind = payload["error"]["kind"]
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        raise SmokeFailure(
            f"{label}: expected error.kind {expected_kind!r}, got invalid stderr"
            f"{_output(result)}"
        ) from error
    if actual_kind != expected_kind:
        raise SmokeFailure(
            f"{label}: expected error.kind {expected_kind!r}, got {actual_kind!r}"
        )


def _environment_python(environment_path: Path) -> Path:
    if os.name == "nt":
        return environment_path / "Scripts" / "python.exe"
    return environment_path / "bin" / "python"


def _environment_console(environment_path: Path) -> Path:
    if os.name == "nt":
        return environment_path / "Scripts" / "kitaru.exe"
    return environment_path / "bin" / "kitaru"


def _create_environment(
    uv: str,
    root: Path,
    artifact: Path,
    extras: str | None,
) -> tuple[Path, Path, dict[str, str]]:
    environment = _isolated_environment(root)
    environment_path = root / "venv"
    result = _run(
        [uv, "venv", "--python", sys.executable, str(environment_path)],
        environment=environment,
        cwd=root,
    )
    _expect_success(f"create {root.name} environment", result)

    requirement = artifact.as_uri()
    if extras:
        requirement = f"kitaru[{extras}] @ {requirement}"
    python = _environment_python(environment_path)
    result = _run(
        [uv, "pip", "install", "--python", str(python), requirement],
        environment=environment,
        cwd=root,
    )
    _expect_success(f"install {root.name} artifact", result)
    return python, _environment_console(environment_path), environment


def _build_artifacts(
    uv: str, repository: Path, root: Path, environment: dict[str, str]
) -> tuple[Path, Path]:
    artifact_directory = root / "artifacts"
    result = _run(
        [uv, "build", "--out-dir", str(artifact_directory)],
        environment=environment,
        cwd=repository,
    )
    _expect_success("build wheel and sdist", result)

    wheels = list(artifact_directory.glob("*.whl"))
    sdists = list(artifact_directory.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise SmokeFailure(
            "build wheel and sdist: expected exactly one .whl and one .tar.gz"
        )
    return wheels[0], sdists[0]


def _smoke_wheel_base(uv: str, root: Path, wheel: Path) -> None:
    python, console, environment = _create_environment(uv, root, wheel, extras=None)
    _expect_success(
        "wheel base imports",
        _run(
            [python, "-c", "import kitaru; import kitaru.cli"],
            environment=environment,
            cwd=root,
        ),
    )
    result = _run([console, "--help"], environment=environment, cwd=root)
    if result.returncode != 2 or result.stdout or "kitaru[cli]" not in result.stderr:
        raise SmokeFailure(
            "wheel base console: expected plain kitaru[cli] hint on stderr with exit 2"
            f"{_output(result)}"
        )


def _smoke_wheel_cli(uv: str, root: Path, wheel: Path) -> None:
    _, console, environment = _create_environment(uv, root, wheel, extras="cli")
    commands = {
        "root help": ["--help"],
        "status module": ["status", "--help"],
        "info module": ["info", "--help"],
        "doctor module": ["doctor", "--help"],
        "worker list module": ["worker", "list", "--help"],
        "worker get module": ["worker", "get", "--help"],
    }
    for label, arguments in commands.items():
        _expect_success(
            f"wheel cli {label}",
            _run([console, *arguments], environment=environment, cwd=root),
        )

    for command in ("version", "schema"):
        _expect_json_command(
            f"wheel cli JSON {command}",
            _run(
                [console, command, "--output", "json"],
                environment=environment,
                cwd=root,
            ),
            command,
        )

    worker_environment = environment | _WORKER_ENVIRONMENT
    result = _run(
        [console, "worker", "start", "--output", "json"],
        environment=worker_environment,
        cwd=root,
    )
    _expect_error_kind("wheel cli worker start", result, "invalid_configuration")


def _smoke_worker_artifact(uv: str, root: Path, artifact: Path, label: str) -> None:
    """Smoke-test one artifact installed with CLI and worker extras."""
    python, console, environment = _create_environment(
        uv, root, artifact, extras="cli,worker"
    )
    _expect_success(
        f"{label} worker imports",
        _run([python, "-c", _WORKER_IMPORTS], environment=environment, cwd=root),
    )
    _expect_json_command(
        f"{label} offline command",
        _run(
            [console, "version", "--output", "json"],
            environment=environment,
            cwd=root,
        ),
        "version",
    )
    result = _run(
        [console, "worker", "start", "--concurrency", "0", "--output", "json"],
        environment=environment | _WORKER_ENVIRONMENT,
        cwd=root,
    )
    _expect_error_kind(f"{label} worker local validation", result, "invalid_arguments")


def main() -> int:
    """Build artifacts and verify their optional dependency boundaries."""
    uv = shutil.which("uv")
    if uv is None:
        print("CLI artifact smoke failed: uv is not available", file=sys.stderr)
        return 1

    repository = Path(__file__).resolve().parents[1]
    try:
        with tempfile.TemporaryDirectory(prefix="kitaru-cli-artifacts-") as directory:
            temporary_root = Path(directory)
            build_environment = _isolated_environment(temporary_root / "build-state")
            wheel, sdist = _build_artifacts(
                uv, repository, temporary_root, build_environment
            )
            _smoke_wheel_base(uv, temporary_root / "wheel-base", wheel)
            _smoke_wheel_cli(uv, temporary_root / "wheel-cli", wheel)
            _smoke_worker_artifact(uv, temporary_root / "wheel-worker", wheel, "wheel")
            _smoke_worker_artifact(uv, temporary_root / "sdist-worker", sdist, "sdist")
    except SmokeFailure as error:
        print(f"CLI artifact smoke failed: {error}", file=sys.stderr)
        return 1

    print("CLI artifact smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
