"""Deterministic, download-free dependency planning for experiment exports."""

import fnmatch
import hashlib
import json
import posixpath
import re
import tomllib
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlsplit

from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.utils import canonicalize_name

from ._sanitize import EphemeralSanitizer
from .models import (
    DependencyPlan,
    DependencyRequirement,
    ExportError,
    SourceInventory,
)
from .source import source_file_bytes

_HASH_OPTION = re.compile(r"(?:^|\s)--hash=sha256:([0-9a-fA-F]{64})(?=\s|$)")
_GIT_COMMIT = re.compile(r"@[0-9a-fA-F]{40}(?:#|$)")
_SHA256_FRAGMENT = re.compile(r"(?:^|[&#])sha256=[0-9a-fA-F]{64}(?:$|[&#])")
_PYTHON_RUNTIME = "3.12"


def _read_text(
    source: SourceInventory,
    path: str,
    *,
    sanitizer: EphemeralSanitizer | None,
) -> str:
    try:
        content = source_file_bytes(source, path)
        text = content.decode("utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise ExportError(
            "invalid_dependency_metadata",
            f"Dependency metadata {path} must be readable UTF-8 text.",
        ) from error
    if sanitizer is not None:
        sanitizer.reject_bytes(
            content,
            code="protected_value_in_dependency",
            message=(
                "Protected runtime material appears in dependency metadata; "
                "export cannot rewrite executable dependency declarations safely."
            ),
        )
    return text


def _load_toml(content: str, path: str) -> dict[str, Any]:
    try:
        return tomllib.loads(content)
    except tomllib.TOMLDecodeError as error:
        raise ExportError(
            "invalid_dependency_metadata",
            f"Dependency metadata {path} is not valid TOML.",
        ) from error


def _logical_requirement_lines(content: str) -> tuple[str, ...]:
    logical: list[str] = []
    pending = ""
    for physical in content.splitlines():
        stripped = physical.strip()
        if not stripped or stripped.startswith("#"):
            continue
        pending = f"{pending} {stripped}".strip()
        if pending.endswith("\\"):
            pending = pending[:-1].rstrip()
            continue
        logical.append(pending)
        pending = ""
    if pending:
        raise ExportError(
            "invalid_dependency_metadata",
            "requirements.txt ends with an incomplete continuation.",
        )
    return tuple(logical)


def _validate_direct_url(requirement: Requirement) -> None:
    url = requirement.url
    if url is None:
        return
    parsed = urlsplit(url)
    if parsed.username is not None or parsed.password is not None:
        raise ExportError(
            "unsafe_dependency",
            "A direct dependency contains embedded credentials.",
        )
    if parsed.scheme.startswith("git+"):
        if _GIT_COMMIT.search(url) is None:
            raise ExportError(
                "unsafe_dependency",
                "VCS dependencies must select one immutable 40-character commit.",
            )
        return
    if (
        parsed.scheme not in {"https", "http"}
        or _SHA256_FRAGMENT.search(parsed.fragment) is None
    ):
        raise ExportError(
            "unsafe_dependency",
            "URL dependencies must use HTTP(S) and include an artifact SHA-256 hash.",
        )


def _parse_requirement(value: str) -> Requirement:
    without_hashes = _HASH_OPTION.sub("", value).strip()
    if without_hashes.startswith(("-", ".", "/")):
        raise ExportError(
            "unsupported_dependency_metadata",
            "Export v1 does not support requirement options or bare paths.",
        )
    try:
        requirement = Requirement(without_hashes)
    except InvalidRequirement as error:
        raise ExportError(
            "unsupported_dependency_metadata",
            "Export v1 supports standard PEP 508 requirement declarations only.",
        ) from error
    _validate_direct_url(requirement)
    return requirement


def _inside_snapshot(path: str, files: frozenset[str]) -> str:
    normalized = posixpath.normpath(path.replace("\\", "/"))
    relative = PurePosixPath(normalized)
    if relative.is_absolute() or normalized == ".." or normalized.startswith("../"):
        raise ExportError(
            "unsafe_dependency",
            "Relative dependency targets must remain inside the source root.",
        )
    project_file = (
        "pyproject.toml" if normalized == "." else f"{normalized}/pyproject.toml"
    )
    if normalized not in files and project_file not in files:
        raise ExportError(
            "unsafe_dependency",
            "A relative dependency target does not exist in the source snapshot.",
        )
    return relative.as_posix()


def _workspace_source_path(
    project_name: str,
    source: SourceInventory,
    files: frozenset[str],
    workspace: dict[str, Any],
    *,
    sanitizer: EphemeralSanitizer | None,
) -> str:
    members = workspace.get("members")
    if not isinstance(members, list) or not all(
        isinstance(member, str) and member for member in members
    ):
        raise ExportError(
            "unsupported_dependency_metadata",
            "Workspace dependencies require a standard tool.uv.workspace members list.",
        )
    matches: list[str] = []
    for path in sorted(files):
        if not path.endswith("/pyproject.toml"):
            continue
        directory = path.removesuffix("/pyproject.toml")
        if not any(fnmatch.fnmatchcase(directory, pattern) for pattern in members):
            continue
        child = _load_toml(
            _read_text(source, path, sanitizer=sanitizer),
            path,
        )
        child_name = child.get("project", {}).get("name")
        if (
            isinstance(child_name, str)
            and canonicalize_name(child_name) == project_name
        ):
            matches.append(directory)
    if len(matches) != 1:
        raise ExportError(
            "unsafe_dependency",
            "A workspace dependency must resolve to exactly one in-root project.",
        )
    return matches[0]


def _source_paths(
    pyproject: dict[str, Any],
    inventory: SourceInventory,
    files: frozenset[str],
    *,
    sanitizer: EphemeralSanitizer | None,
) -> dict[str, str]:
    tool = pyproject.get("tool", {})
    uv = tool.get("uv", {}) if isinstance(tool, dict) else {}
    sources = uv.get("sources", {}) if isinstance(uv, dict) else {}
    workspace = uv.get("workspace", {}) if isinstance(uv, dict) else {}
    if not isinstance(sources, dict):
        raise ExportError(
            "unsupported_dependency_metadata",
            "tool.uv.sources must be a table when present.",
        )
    resolved: dict[str, str] = {}
    for raw_name, source_config in sources.items():
        if not isinstance(raw_name, str) or not isinstance(source_config, dict):
            raise ExportError(
                "unsupported_dependency_metadata",
                "Export v1 supports standard table entries in tool.uv.sources.",
            )
        name = canonicalize_name(raw_name)
        path = source_config.get("path")
        is_workspace = source_config.get("workspace") is True
        if isinstance(path, str) and not is_workspace and len(source_config) == 1:
            resolved[name] = _inside_snapshot(path, files)
        elif is_workspace and set(source_config) == {"workspace"}:
            resolved[name] = _workspace_source_path(
                name,
                inventory,
                files,
                workspace if isinstance(workspace, dict) else {},
                sanitizer=sanitizer,
            )
        else:
            raise ExportError(
                "unsupported_dependency_metadata",
                "Export v1 supports only in-root path or workspace uv sources.",
            )
    return resolved


def _check_python_compatibility(pyproject: dict[str, Any]) -> None:
    project = pyproject.get("project")
    if not isinstance(project, dict):
        raise ExportError(
            "invalid_dependency_metadata",
            "pyproject.toml must contain a standard project table.",
        )
    requires_python = project.get("requires-python")
    if requires_python is None:
        return
    if not isinstance(requires_python, str):
        raise ExportError(
            "invalid_dependency_metadata",
            "project.requires-python must be a string.",
        )
    try:
        compatible = SpecifierSet(requires_python).contains(
            _PYTHON_RUNTIME, prereleases=True
        )
    except InvalidSpecifier as error:
        raise ExportError(
            "invalid_dependency_metadata",
            "project.requires-python is not a valid version constraint.",
        ) from error
    if not compatible:
        raise ExportError(
            "incompatible_python",
            "The agent project does not support the export runtime Python 3.12.",
        )


def _requirements_from_pyproject(
    pyproject: dict[str, Any],
    source: SourceInventory,
    files: frozenset[str],
    *,
    sanitizer: EphemeralSanitizer | None,
) -> list[tuple[str, str | None]]:
    _check_python_compatibility(pyproject)
    project = pyproject["project"]
    dependencies = project.get("dependencies", [])
    if not isinstance(dependencies, list) or not all(
        isinstance(requirement, str) for requirement in dependencies
    ):
        raise ExportError(
            "invalid_dependency_metadata",
            "project.dependencies must be a list of requirement strings.",
        )
    paths = _source_paths(
        pyproject,
        source,
        files,
        sanitizer=sanitizer,
    )
    result: list[tuple[str, str | None]] = []
    for value in dependencies:
        parsed = _parse_requirement(value)
        normalized = canonicalize_name(parsed.name)
        source_path = paths.get(normalized)
        if normalized in paths and parsed.url is not None:
            raise ExportError(
                "ambiguous_dependency_metadata",
                "A dependency cannot declare both a direct URL and a uv source.",
            )
        result.append((value.strip(), source_path))
    unused_sources = sorted(
        set(paths)
        - {canonicalize_name(_parse_requirement(v).name) for v in dependencies}
    )
    if unused_sources:
        raise ExportError(
            "unsupported_dependency_metadata",
            "Every uv source must correspond to a declared project dependency.",
        )
    return result


def _freeze_requirements(
    values: list[tuple[str, str | None]],
) -> tuple[DependencyRequirement, ...]:
    by_project: dict[str, tuple[str, str | None]] = {}
    for value, source_path in values:
        parsed = _parse_requirement(value)
        project = canonicalize_name(parsed.name)
        normalized_value = " ".join(value.split())
        previous = by_project.get(project)
        current = (normalized_value, source_path)
        if previous is not None and previous != current:
            raise ExportError(
                "dependency_conflict",
                "Multiple non-identical requirements target one normalized project.",
            )
        by_project[project] = current
    return tuple(
        DependencyRequirement(
            project=project,
            requirement=value,
            requirement_digest=hashlib.sha256(value.encode("utf-8")).hexdigest(),
            source_path=source_path,
        )
        for project, (value, source_path) in sorted(by_project.items())
    )


def classify_dependencies(
    source: SourceInventory,
    *,
    sanitizer: EphemeralSanitizer | None = None,
) -> DependencyPlan:
    """Classify one standard Python project without installing dependencies."""
    files = frozenset(file.path for file in source.files)
    has_pyproject = "pyproject.toml" in files
    has_lock = "uv.lock" in files
    has_requirements = "requirements.txt" in files
    if has_lock and not has_pyproject:
        raise ExportError(
            "invalid_dependency_metadata",
            "uv.lock requires a root pyproject.toml.",
        )
    if not has_pyproject and not has_requirements:
        raise ExportError(
            "missing_dependency_metadata",
            "Export v1 requires pyproject.toml or requirements.txt.",
        )
    if has_pyproject and has_requirements and not has_lock:
        raise ExportError(
            "ambiguous_dependency_metadata",
            "A project with both pyproject.toml and requirements.txt requires uv.lock "
            "to establish manifest precedence.",
        )

    values: list[tuple[str, str | None]]
    if has_pyproject:
        pyproject = _load_toml(
            _read_text(source, "pyproject.toml", sanitizer=sanitizer),
            "pyproject.toml",
        )
        values = _requirements_from_pyproject(
            pyproject,
            source,
            files,
            sanitizer=sanitizer,
        )
        if has_lock:
            lock = _load_toml(
                _read_text(source, "uv.lock", sanitizer=sanitizer),
                "uv.lock",
            )
            if not isinstance(lock.get("version"), int):
                raise ExportError(
                    "invalid_dependency_metadata",
                    "uv.lock does not declare a supported integer lock version.",
                )
            status = "locked"
            manifests = ("pyproject.toml", "uv.lock")
        else:
            status = "declared"
            manifests = ("pyproject.toml",)
    else:
        content = _read_text(
            source,
            "requirements.txt",
            sanitizer=sanitizer,
        )
        lines = _logical_requirement_lines(content)
        if not lines:
            raise ExportError(
                "invalid_dependency_metadata",
                "requirements.txt must declare at least one dependency.",
            )
        values = [(line, None) for line in lines]
        status = (
            "locked" if all(_HASH_OPTION.search(line) for line in lines) else "declared"
        )
        manifests = ("requirements.txt",)

    requirements = _freeze_requirements(values)
    digest_payload = {
        "manifests": manifests,
        "requirements": [item.requirement for item in requirements],
        "status": status,
    }
    requirement_digest = hashlib.sha256(
        json.dumps(
            digest_payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return DependencyPlan(
        status=status,
        manifests=manifests,
        requirements=requirements,
        requirement_digest=requirement_digest,
    )
