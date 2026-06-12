#!/usr/bin/env python3
"""Audit the public example coverage manifest without running examples."""

from __future__ import annotations

import re
import shlex
import sys
from pathlib import Path
from typing import Any, cast

import yaml

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "examples" / "example-coverage.yaml"
DOC_LIST_PATHS = (
    ROOT / "examples" / "README.md",
    ROOT / "docs" / "book" / "getting-started" / "examples.md",
)
ALLOWED_STATUSES = {
    "covered",
    "planned",
    "missing",
    "help_only",
    "import_contract",
    "smoke_only",
    "opt_in_extended",
    "manual_only",
    "not_applicable",
}
PROVIDER_STATUSES_REQUIRING_METADATA = {
    "covered",
    "planned",
    "smoke_only",
    "opt_in_extended",
    "manual_only",
}
ALLOWED_COST_CLASSES = {"none", "low", "medium", "high"}
EXAMPLE_PATH_RE = re.compile(
    r"(?:https://github\.com/zenml-io/kitaru/(?:tree|blob)/develop/)?"
    r"(examples/[A-Za-z0-9_./#-]+)"
)


def main() -> int:
    errors = audit_manifest()
    if errors:
        print("Example coverage audit failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("Example coverage audit passed.")
    return 0


def audit_manifest() -> list[str]:
    data = _load_yaml(MANIFEST_PATH)
    errors: list[str] = []

    examples = data.get("examples")
    if not isinstance(examples, list) or not examples:
        return ["examples/example-coverage.yaml must contain a non-empty examples list"]

    manifest_paths: dict[str, str] = {}
    manifest_ids: set[str] = set()
    excluded_paths = set(_explicit_exclusions(data))

    for index, entry in enumerate(examples):
        context = _entry_context(entry, index)
        if not isinstance(entry, dict):
            errors.append(f"entry {index} must be a mapping")
            continue
        entry = cast(dict[str, Any], entry)

        example_id = entry.get("id")
        if not isinstance(example_id, str) or not example_id:
            errors.append(f"{context}: id must be a non-empty string")
        elif example_id in manifest_ids:
            errors.append(f"{context}: duplicate id {example_id!r}")
        else:
            manifest_ids.add(example_id)

        path = entry.get("path")
        if not isinstance(path, str) or not path:
            errors.append(f"{context}: path must be a non-empty string")
        else:
            normalized_path = _normalize_example_path(path)
            if not (ROOT / normalized_path).exists():
                errors.append(f"{context}: path does not exist: {normalized_path}")
            manifest_paths[normalized_path] = context

        errors.extend(_audit_public_docs(entry, context))
        errors.extend(_audit_coverage(entry, context))
        errors.extend(_audit_release_policy(entry, context))

    for docs_path in _public_example_paths_from_docs():
        if docs_path in excluded_paths:
            continue
        if not _is_manifest_aware(docs_path, manifest_paths):
            errors.append(
                f"public docs list {docs_path}, but examples/example-coverage.yaml "
                "has no matching entry or explicit exclusion"
            )

    return errors


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise SystemExit(f"{path} must contain a YAML mapping")
    return data


def _entry_context(entry: object, index: int) -> str:
    if isinstance(entry, dict):
        entry = cast(dict[str, Any], entry)
        if isinstance(entry.get("id"), str):
            return f"entry {entry['id']!r}"
    return f"entry #{index}"


def _explicit_exclusions(data: dict[str, Any]) -> list[str]:
    exclusions = data.get("explicit_exclusions", [])
    if not isinstance(exclusions, list):
        return []
    paths: list[str] = []
    for item in exclusions:
        if isinstance(item, str):
            paths.append(_normalize_example_path(item))
        elif isinstance(item, dict):
            item = cast(dict[str, Any], item)
            if isinstance(item.get("path"), str):
                paths.append(_normalize_example_path(item["path"]))
    return paths


def _audit_public_docs(entry: dict[str, Any], context: str) -> list[str]:
    errors: list[str] = []
    public_docs = entry.get("public_docs")
    if not isinstance(public_docs, list) or not public_docs:
        return [f"{context}: public_docs must list at least one public source"]
    for doc_path in public_docs:
        if not isinstance(doc_path, str) or not doc_path:
            errors.append(f"{context}: public_docs entries must be non-empty strings")
            continue
        if not (ROOT / doc_path).exists():
            errors.append(f"{context}: public docs path does not exist: {doc_path}")
    return errors


def _audit_coverage(entry: dict[str, Any], context: str) -> list[str]:
    coverage = entry.get("coverage")
    if not isinstance(coverage, dict):
        return [f"{context}: coverage must be a mapping"]

    errors: list[str] = []
    for section_name in ("deterministic_pytest", "local_smoke", "live_provider"):
        section = coverage.get(section_name)
        if not isinstance(section, dict):
            errors.append(f"{context}: coverage.{section_name} must be a mapping")
            continue
        status = section.get("status")
        if status not in ALLOWED_STATUSES:
            errors.append(
                f"{context}: coverage.{section_name}.status must be one of "
                f"{sorted(ALLOWED_STATUSES)}; got {status!r}"
            )

    deterministic = coverage.get("deterministic_pytest", {})
    if isinstance(deterministic, dict):
        errors.extend(_audit_test_file(deterministic, context))

    local_smoke = coverage.get("local_smoke", {})
    if isinstance(local_smoke, dict):
        errors.extend(_audit_command(local_smoke, context, "coverage.local_smoke"))

    live_provider = coverage.get("live_provider", {})
    if isinstance(live_provider, dict):
        errors.extend(_audit_live_provider(live_provider, context))
        errors.extend(_audit_command(live_provider, context, "coverage.live_provider"))

    return errors


def _audit_test_file(section: dict[str, Any], context: str) -> list[str]:
    status = section.get("status")
    test_file = section.get("test_file")
    if status == "covered":
        if not isinstance(test_file, str) or not test_file:
            return [f"{context}: covered deterministic_pytest must list test_file"]
        if not (ROOT / test_file).is_file():
            return [f"{context}: covered test_file does not exist: {test_file}"]
    return []


def _audit_command(section: dict[str, Any], context: str, field_name: str) -> list[str]:
    command = section.get("command")
    status = section.get("status")
    if command is None:
        if status in {
            "covered",
            "help_only",
            "import_contract",
            "smoke_only",
            "opt_in_extended",
        }:
            return [
                f"{context}: {field_name}.command is required for status {status!r}"
            ]
        return []
    if not isinstance(command, str) or not command.strip():
        return [f"{context}: {field_name}.command must be a non-empty string or null"]

    errors: list[str] = []
    command_paths = section.get("command_paths")
    if _requires_declared_command_paths(command) and not _is_non_empty_string_list(
        command_paths
    ):
        errors.append(
            f"{context}: {field_name}.command_paths must list the script path for "
            "cd ... && uv run python ... commands"
        )

    referenced_paths = set(_command_paths(command))
    declared_command_paths: list[str] = []
    if command_paths is not None:
        if not _is_non_empty_string_list(command_paths):
            errors.append(
                f"{context}: {field_name}.command_paths must be a non-empty list "
                "of repo-relative strings"
            )
        else:
            declared_command_paths = cast(list[str], command_paths)

    for command_path in sorted(referenced_paths):
        if not (ROOT / command_path).exists():
            errors.append(
                f"{context}: {field_name}.command references missing path: "
                f"{command_path}"
            )

    for command_path in sorted(declared_command_paths):
        if not (ROOT / command_path).is_file():
            errors.append(
                f"{context}: {field_name}.command_paths references missing script "
                f"file: {command_path}"
            )
    return errors


def _audit_live_provider(section: dict[str, Any], context: str) -> list[str]:
    status = section.get("status")
    if status == "not_applicable":
        if section.get("required") not in (False, None):
            return [f"{context}: not_applicable live_provider must not be required"]
        return []

    errors: list[str] = []
    if status in PROVIDER_STATUSES_REQUIRING_METADATA:
        provider = section.get("provider")
        if not isinstance(provider, str) or not provider:
            errors.append(
                f"{context}: live_provider with status {status!r} must list provider"
            )

        required_env = section.get("required_env")
        if not _is_non_empty_string_list(required_env):
            errors.append(
                f"{context}: live_provider with status {status!r} must list "
                "required_env"
            )

        timeout = section.get("timeout_seconds")
        if not isinstance(timeout, int) or timeout <= 0:
            errors.append(
                f"{context}: live_provider with status {status!r} must set "
                "positive timeout_seconds"
            )

        cost_class = section.get("cost_class")
        if cost_class not in ALLOWED_COST_CLASSES:
            errors.append(
                f"{context}: live_provider cost_class must be one of "
                f"{sorted(ALLOWED_COST_CLASSES)}; got {cost_class!r}"
            )

    cost_class = section.get("cost_class")
    is_automated_expensive = (
        cost_class in {"medium", "high"} and status != "manual_only"
    )
    if status == "opt_in_extended" or is_automated_expensive:
        opt_in_env = section.get("opt_in_env")
        if not _is_non_empty_string_list(opt_in_env):
            errors.append(
                f"{context}: extended or automated medium/high-cost "
                "live_provider entries must list opt_in_env"
            )

    return errors


def _audit_release_policy(entry: dict[str, Any], context: str) -> list[str]:
    release_policy = entry.get("release_policy")
    if not isinstance(release_policy, dict):
        return [f"{context}: release_policy must be a mapping"]
    if "required_when_changed" not in release_policy:
        return [f"{context}: release_policy.required_when_changed must be explicit"]
    if not isinstance(release_policy["required_when_changed"], bool):
        return [
            f"{context}: release_policy.required_when_changed must be true or false"
        ]
    return []


def _is_non_empty_string_list(value: object) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(item, str) and bool(item) for item in value)
    )


def _requires_declared_command_paths(command: str) -> bool:
    return command.strip().startswith("cd ") and "uv run python" in command


def _public_example_paths_from_docs() -> set[str]:
    paths: set[str] = set()
    for doc_path in DOC_LIST_PATHS:
        text = doc_path.read_text(encoding="utf-8")
        for raw_path in EXAMPLE_PATH_RE.findall(text):
            path = _normalize_example_path(raw_path)
            if _should_ignore_public_path(path):
                continue
            paths.add(path)
    return paths


def _normalize_example_path(path: str) -> str:
    normalized = path.split("#", 1)[0].strip("`.,) ")
    normalized = normalized.rstrip("/") if normalized != "examples" else normalized
    if normalized.endswith("/README.md"):
        normalized = normalized[: -len("/README.md")]
    return normalized


def _should_ignore_public_path(path: str) -> bool:
    ignored_suffixes = {
        "hero.png",
        "__init__.py",
    }
    if path == "examples" or path.endswith("/README.md"):
        return True
    return any(path.endswith(suffix) for suffix in ignored_suffixes)


def _is_manifest_aware(docs_path: str, manifest_paths: dict[str, str]) -> bool:
    if docs_path in manifest_paths:
        return True
    docs_abs = ROOT / docs_path
    for manifest_path in manifest_paths:
        manifest_abs = ROOT / manifest_path
        if manifest_abs.is_dir() and docs_abs.is_relative_to(manifest_abs):
            return True
        if docs_abs.is_dir() and manifest_abs.is_relative_to(docs_abs):
            return True
    return False


def _command_paths(command: str) -> set[str]:
    paths: set[str] = set()
    try:
        parts = shlex.split(command)
    except ValueError:
        return paths

    for part in parts:
        cleaned = _normalize_example_path(part)
        if cleaned.startswith(("examples/", "scripts/")):
            paths.add(cleaned)
        elif cleaned.startswith("./scripts/"):
            paths.add(cleaned[2:])
    return paths


if __name__ == "__main__":
    raise SystemExit(main())
