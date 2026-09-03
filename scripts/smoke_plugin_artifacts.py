#!/usr/bin/env python3
"""Build and smoke-test default plugins from wheel artifacts."""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from zipfile import BadZipFile, ZipFile

from packaging.requirements import Requirement

if __package__:
    from scripts.release_units import default_requirements, load_inventory
else:
    from release_units import default_requirements, load_inventory

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
_REQUIRED_PROJECT_URLS = frozenset(
    {"Homepage", "Documentation", "Repository", "Issues", "Changelog"}
)


class SmokeFailure(RuntimeError):
    """Report one failed plugin artifact assertion."""


def _requirement_pins(requirement: str, name: str, version: str) -> bool:
    """Return whether a requirement pins exactly one project version."""
    parsed = Requirement(requirement)
    return _canonicalize_name(parsed.name) == _canonicalize_name(name) and (
        str(parsed.specifier) == f"=={version}"
    )


def _canonicalize_name(value: str) -> str:
    """Normalize a Python distribution name."""
    return re.sub(r"[-_.]+", "-", value).lower()


def _isolated_environment(root: Path) -> dict[str, str]:
    """Create process state that cannot inherit the workspace environment."""
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("KITARU_")
        and name not in _SCRUBBED_ENVIRONMENT_VARIABLES
    }
    directories = {
        "HOME": root / "home",
        "XDG_CACHE_HOME": root / "xdg" / "cache",
        "XDG_CONFIG_HOME": root / "xdg" / "config",
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
    return environment


def _run(
    command: list[str | Path], *, environment: dict[str, str], cwd: Path
) -> subprocess.CompletedProcess[str]:
    """Run one bounded subprocess."""
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
            f"Command timed out after {_SUBPROCESS_TIMEOUT_SECONDS} seconds: "
            f"{' '.join(parts)}"
        ) from error


def _expect_success(
    label: str, result: subprocess.CompletedProcess[str]
) -> subprocess.CompletedProcess[str]:
    """Require a successful subprocess result."""
    if result.returncode != 0:
        output = result.stderr.strip() or result.stdout.strip()
        suffix = f": {output}" if output else ""
        raise SmokeFailure(f"{label}: expected exit 0, got {result.returncode}{suffix}")
    return result


def _project_metadata(project: Path) -> tuple[str, str]:
    """Read one plugin project's distribution name and version."""
    pyproject = project / "pyproject.toml"
    try:
        metadata = tomllib.loads(pyproject.read_text())["project"]
        return str(metadata["name"]), str(metadata["version"])
    except (FileNotFoundError, KeyError, tomllib.TOMLDecodeError) as error:
        raise SmokeFailure(f"Invalid plugin project metadata: {pyproject}") from error


def _artifact_import_module(project: Path) -> str | None:
    """Read an optional standalone package import contract."""
    pyproject = project / "pyproject.toml"
    try:
        document = tomllib.loads(pyproject.read_text())
    except (FileNotFoundError, tomllib.TOMLDecodeError) as error:
        raise SmokeFailure(f"Invalid plugin project metadata: {pyproject}") from error
    value = (
        document.get("tool", {})
        .get("kitaru", {})
        .get("artifact", {})
        .get("import-module")
    )
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise SmokeFailure(f"Invalid artifact import module in {pyproject}")
    return value


def _resolve_projects(repository: Path, selected: list[Path]) -> list[Path]:
    """Resolve selected plugin projects or all workspace plugin projects."""
    projects = selected or sorted((repository / "plugins" / "packages").iterdir())
    resolved = [project.resolve() for project in projects if project.is_dir()]
    if not resolved:
        raise SmokeFailure("No plugin package projects were selected")
    return resolved


def _find_wheel(directory: Path, name: str, version: str) -> Path:
    """Find the wheel matching one exact distribution version."""
    wheel_prefix = _canonicalize_name(name).replace("-", "_")
    matches = sorted(directory.glob(f"{wheel_prefix}-{version}-*.whl"))
    if len(matches) != 1:
        raise SmokeFailure(
            f"Expected one wheel for {name}=={version}, found {len(matches)}"
        )
    return matches[0].resolve()


def _validate_wheel_metadata(wheel: Path, name: str, version: str) -> None:
    """Require the metadata that PyPI renders for one plugin wheel."""
    try:
        with ZipFile(wheel) as archive:
            metadata_files = [
                path
                for path in archive.namelist()
                if path.endswith(".dist-info/METADATA")
            ]
            if len(metadata_files) != 1:
                raise SmokeFailure(
                    f"{name}=={version}: expected one wheel METADATA file, "
                    f"found {len(metadata_files)}"
                )
            message = BytesParser(policy=default).parsebytes(
                archive.read(metadata_files[0])
            )
    except (BadZipFile, OSError) as error:
        raise SmokeFailure(f"{name}=={version}: invalid wheel: {error}") from error

    required_headers = {
        "Name": name,
        "Version": version,
        "Description-Content-Type": "text/markdown",
        "License-Expression": "Apache-2.0",
    }
    for header, expected in required_headers.items():
        if message.get(header) != expected:
            raise SmokeFailure(f"{name}=={version}: {header} must be {expected!r}")

    for header in ("Summary", "Author-email", "Keywords"):
        value = message.get(header)
        if value is None or not str(value).strip():
            raise SmokeFailure(f"{name}=={version}: missing {header}")
    if not message.get_all("Classifier"):
        raise SmokeFailure(f"{name}=={version}: missing Classifier")

    payload = message.get_payload()
    if not isinstance(payload, str) or not payload.strip():
        raise SmokeFailure(f"{name}=={version}: wheel description is empty")

    project_urls: set[str] = set()
    for value in message.get_all("Project-URL", []):
        label, separator, url = str(value).partition(",")
        if not separator or not label.strip() or not url.strip().startswith("https://"):
            raise SmokeFailure(f"{name}=={version}: invalid Project-URL {value!r}")
        project_urls.add(label.strip())
    missing_urls = sorted(_REQUIRED_PROJECT_URLS - project_urls)
    if missing_urls:
        raise SmokeFailure(f"{name}=={version}: missing Project-URL {missing_urls[0]}")


def _build_wheel(
    uv: str,
    repository: Path,
    project: Path,
    candidate_directory: Path,
    environment: dict[str, str],
) -> Path:
    """Build one project wheel into the candidate directory."""
    name, version = _project_metadata(project)
    result = _run(
        [
            uv,
            "build",
            "--wheel",
            "--project",
            project,
            "--out-dir",
            candidate_directory,
        ],
        environment=environment,
        cwd=repository,
    )
    _expect_success(f"build {name}=={version}", result)
    return _find_wheel(candidate_directory, name, version)


def _resolve_kitaru_wheel(
    uv: str,
    repository: Path,
    candidate_directory: Path,
    source: Path | None,
    environment: dict[str, str],
) -> Path:
    """Build or copy the candidate Kitaru wheel into the artifact directory."""
    _, version = _project_metadata(repository)
    if source is None:
        return _build_wheel(
            uv, repository, repository, candidate_directory, environment
        )
    candidates = [source] if source.is_file() else sorted(source.glob("kitaru-*.whl"))
    if len(candidates) != 1:
        raise SmokeFailure(
            f"Expected one candidate Kitaru wheel under {source}, "
            f"found {len(candidates)}"
        )
    destination = candidate_directory / candidates[0].name
    if candidates[0].resolve() != destination.resolve():
        shutil.copy2(candidates[0], destination)
    return _find_wheel(candidate_directory, "kitaru", version)


def _environment_python(environment_path: Path) -> Path:
    """Return the Python executable inside one virtual environment."""
    if os.name == "nt":
        return environment_path / "Scripts" / "python.exe"
    return environment_path / "bin" / "python"


def _remove_generated_ignore(candidate_directory: Path) -> None:
    """Remove uv's output marker so the tracked placeholder remains."""
    generated_ignore = candidate_directory / ".gitignore"
    if generated_ignore.exists() and generated_ignore.read_text().strip() == "*":
        generated_ignore.unlink()


def _smoke_candidate_wheels(
    uv: str,
    repository: Path,
    root: Path,
    kitaru_wheel: Path,
    plugin_wheels: list[Path],
    requirements: list[str],
    import_modules: list[str],
    environment: dict[str, str],
) -> None:
    """Install candidate wheels and probe their configured entrypoints."""
    root.mkdir(parents=True, exist_ok=True)
    environment_path = root / "venv"
    _expect_success(
        "create plugin artifact environment",
        _run(
            [uv, "venv", "--python", sys.executable, environment_path],
            environment=environment,
            cwd=root,
        ),
    )
    python = _environment_python(environment_path)
    kitaru_requirement = f"kitaru[server] @ {kitaru_wheel.as_uri()}"
    _expect_success(
        "install candidate Kitaru wheel",
        _run(
            [
                uv,
                "pip",
                "install",
                "--python",
                python,
                kitaru_requirement,
            ],
            environment=environment,
            cwd=root,
        ),
    )
    for import_module in import_modules:
        result = _run(
            [
                python,
                "-c",
                "import importlib.util,sys; "
                f"sys.exit(importlib.util.find_spec({import_module!r}) is not None)",
            ],
            environment=environment,
            cwd=root,
        )
        _expect_success(f"exclude {import_module} from the Kitaru wheel", result)
    _expect_success(
        "install candidate extension wheels",
        _run(
            [
                uv,
                "pip",
                "install",
                "--python",
                python,
                *(wheel.as_uri() for wheel in plugin_wheels),
            ],
            environment=environment,
            cwd=root,
        ),
    )
    _expect_success(
        "check plugin artifact environment",
        _run(
            [uv, "pip", "check", "--python", python],
            environment=environment,
            cwd=root,
        ),
    )
    probe = repository / "scripts" / "probe_plugin_artifacts.py"
    command: list[str | Path] = [python, probe]
    for requirement in requirements:
        command.extend(("--requirement", requirement))
    for import_module in import_modules:
        command.extend(("--module", import_module))
    _expect_success(
        "probe installed plugin artifacts",
        _run(command, environment=environment, cwd=root),
    )


def main() -> int:
    """Build plugin wheels and validate their installed contracts."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--validate-wheel",
        type=Path,
        help="Validate one existing plugin wheel and exit.",
    )
    parser.add_argument(
        "--distribution",
        help="Expected distribution name for --validate-wheel.",
    )
    parser.add_argument(
        "--version",
        help="Expected distribution version for --validate-wheel.",
    )
    parser.add_argument(
        "--package",
        action="append",
        default=[],
        type=Path,
        help="Plugin package directory to test. Repeat to select multiple packages.",
    )
    parser.add_argument(
        "--candidate-dir",
        dest="candidate_directory",
        type=Path,
        help="Directory that should retain the candidate wheels.",
    )
    parser.add_argument(
        "--kitaru-wheel",
        type=Path,
        help="Existing Kitaru wheel or directory containing one candidate wheel.",
    )
    arguments = parser.parse_args()

    if arguments.validate_wheel is not None:
        if arguments.distribution is None or arguments.version is None:
            parser.error("--validate-wheel requires --distribution and --version")
        try:
            _validate_wheel_metadata(
                arguments.validate_wheel,
                arguments.distribution,
                arguments.version,
            )
        except SmokeFailure as error:
            print(f"Plugin artifact smoke failed: {error}", file=sys.stderr)
            return 1
        print(
            "Validated plugin wheel metadata: "
            f"{arguments.distribution}=={arguments.version}"
        )
        return 0

    uv = shutil.which("uv")
    if uv is None:
        print("Plugin artifact smoke failed: uv is not available", file=sys.stderr)
        return 1
    repository = Path(__file__).resolve().parents[1]
    try:
        projects = _resolve_projects(repository, arguments.package)
        defaults = default_requirements(load_inventory(repository))
        with tempfile.TemporaryDirectory(
            prefix="kitaru-plugin-artifacts-"
        ) as directory:
            temporary_root = Path(directory)
            candidate_directory = (
                arguments.candidate_directory.resolve()
                if arguments.candidate_directory
                else temporary_root / "candidate-wheels"
            )
            candidate_directory.mkdir(parents=True, exist_ok=True)
            environment = _isolated_environment(temporary_root / "state")
            kitaru_wheel = _resolve_kitaru_wheel(
                uv,
                repository,
                candidate_directory,
                arguments.kitaru_wheel,
                environment,
            )
            plugin_wheels: list[Path] = []
            requirements: list[str] = []
            import_modules: list[str] = []
            for project in projects:
                name, version = _project_metadata(project)
                requirement = defaults.get(_canonicalize_name(name))
                import_module = _artifact_import_module(project)
                if requirement is None and import_module is None:
                    raise SmokeFailure(
                        f"{name} has neither a default pin nor an artifact import"
                    )
                if requirement is not None and not _requirement_pins(
                    requirement, name, version
                ):
                    raise SmokeFailure(
                        f"{name}=={version} does not match the release inventory"
                    )
                wheel = _build_wheel(
                    uv, repository, project, candidate_directory, environment
                )
                _validate_wheel_metadata(wheel, name, version)
                plugin_wheels.append(wheel)
                if requirement is not None:
                    requirements.append(requirement)
                if import_module is not None:
                    import_modules.append(import_module)
            _smoke_candidate_wheels(
                uv,
                repository,
                temporary_root / "installed",
                kitaru_wheel,
                plugin_wheels,
                requirements,
                import_modules,
                environment,
            )
            _remove_generated_ignore(candidate_directory)
    except (OSError, SmokeFailure) as error:
        print(f"Plugin artifact smoke failed: {error}", file=sys.stderr)
        return 1

    print("Plugin artifact smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
