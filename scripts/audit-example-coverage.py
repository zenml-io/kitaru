#!/usr/bin/env python3
"""Audit the public example coverage manifest without running examples."""

import re
import shlex
import sys
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "examples" / "example-coverage.yaml"
DOC_LIST_PATHS = (
    ROOT / "examples" / "README.md",
    ROOT / "docs" / "book" / "getting-started" / "quickstart.md",
    ROOT / "docs" / "book" / "adapters" / "pydantic-ai.md",
    ROOT / "docs" / "book" / "adapters" / "openai-agents.md",
    ROOT / "docs" / "book" / "adapters" / "langgraph.md",
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
STATUSES_REQUIRING_WAIVER = {"missing", "planned", "manual_only"}
ALLOWED_COST_CLASSES = {"none", "low", "medium", "high"}
ALLOWED_MANUAL_WORKFLOW_SUITES = {"provider-core", "provider-extended"}
EXPECTED_MANUAL_WORKFLOW_SCOPE = "full provider-extended marker group"
LLM_INTEGRATION_WORKFLOW_PATH = ".github/workflows/llm-integration.yml"
LLM_INTEGRATION_PROVIDER_INPUTS = (
    "include_openai",
    "include_anthropic",
    "include_google_adk",
)
EXAMPLE_PATH_RE = re.compile(r"examples/[A-Za-z0-9_./#-]+")
MARKDOWN_LINK_TARGET_RE = re.compile(r"\]\(([^)\s]+)")
SHELL_PLACEHOLDER_ASSIGNMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=<[^<>\s]+>$")


def main() -> int:
    """Run the example coverage audit and return a process exit code."""
    errors = audit_manifest()
    if errors:
        print("Example coverage audit failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("Example coverage audit passed.")
    return 0


def audit_manifest() -> list[str]:
    """Check the example coverage manifest and return the problems found."""
    data = _load_yaml(MANIFEST_PATH)
    errors: list[str] = []

    examples = data.get("examples")
    if not isinstance(examples, list) or not examples:
        return ["examples/example-coverage.yaml must contain a non-empty examples list"]

    manifest_paths: dict[str, str] = {}
    manifest_ids: set[str] = set()

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

    for docs_path in _public_example_paths_from_docs(examples):
        if not _is_manifest_aware(docs_path, manifest_paths):
            errors.append(
                f"public docs list {docs_path}, but examples/example-coverage.yaml "
                "has no matching entry"
            )

    return errors


def _load_yaml(path: Path) -> dict[str, Any]:
    import yaml

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


def _audit_public_docs(entry: dict[str, Any], context: str) -> list[str]:
    errors: list[str] = []
    public_docs = entry.get("public_docs")
    if not isinstance(public_docs, list) or not public_docs:
        return [f"{context}: public_docs must list at least one public source"]
    example_path = entry.get("path")
    normalized_example_path = (
        _normalize_example_path(example_path) if isinstance(example_path, str) else None
    )
    for doc_path in public_docs:
        if not isinstance(doc_path, str) or not doc_path:
            errors.append(f"{context}: public_docs entries must be non-empty strings")
            continue
        if not (ROOT / doc_path).is_file():
            errors.append(f"{context}: public docs path is not a file: {doc_path}")
            continue
        if normalized_example_path is None:
            continue
        if _doc_is_within_example(ROOT / doc_path, normalized_example_path):
            continue
        linked_paths = _example_paths_from_doc(ROOT / doc_path)
        if not any(
            _is_manifest_aware(linked_path, {normalized_example_path: context})
            for linked_path in linked_paths
        ):
            errors.append(
                f"{context}: public docs path {doc_path} does not link to "
                f"{normalized_example_path}"
            )
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
        else:
            errors.extend(_audit_waiver(section, context, f"coverage.{section_name}"))

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


def _audit_waiver(section: dict[str, Any], context: str, field_name: str) -> list[str]:
    status = section.get("status")
    if status not in STATUSES_REQUIRING_WAIVER:
        return []

    waiver = section.get("waiver")
    if not isinstance(waiver, dict):
        return [
            f"{context}: {field_name} with status {status!r} must include "
            "waiver.reason and waiver.reviewer_action"
        ]

    errors: list[str] = []
    for key in ("reason", "reviewer_action"):
        value = waiver.get(key)
        if not isinstance(value, str) or not value.strip():
            errors.append(
                f"{context}: {field_name} with status {status!r} must include "
                f"waiver.{key}"
            )
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
    try:
        command_parts = shlex.split(command)
    except ValueError:
        command_parts = []
    if any(SHELL_PLACEHOLDER_ASSIGNMENT_RE.fullmatch(part) for part in command_parts):
        errors.append(
            f"{context}: {field_name}.command contains an executable shell "
            "placeholder such as NAME=<value>; declare the variable in required_env "
            "instead"
        )
    command_paths = section.get("command_paths")
    if _requires_declared_command_paths(command) and not _is_non_empty_string_list(
        command_paths
    ):
        errors.append(
            f"{context}: {field_name}.command_paths must list the script path for "
            "cd ... && uv run python ... commands"
        )
    if _is_llm_integration_manual_command(command) and not _command_sets_input(
        command, "kitaru_ref"
    ):
        errors.append(
            f"{context}: {field_name}.command must pass "
            "-f kitaru_ref=<RELEASE_REF_OR_SHA> for llm-integration.yml"
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

    manual_workflow = section.get("manual_github_workflow")
    if manual_workflow is not None:
        if not isinstance(manual_workflow, dict):
            errors.append(f"{context}: manual_github_workflow must be a mapping")
        else:
            errors.extend(
                _audit_manual_github_workflow(
                    cast(dict[str, Any], manual_workflow), context
                )
            )

    return errors


def _audit_manual_github_workflow(workflow: dict[str, Any], context: str) -> list[str]:
    errors: list[str] = []
    workflow_path = workflow.get("workflow")
    if not isinstance(workflow_path, str) or not workflow_path:
        errors.append(f"{context}: manual_github_workflow.workflow is required")
    elif not (ROOT / workflow_path).is_file():
        errors.append(
            f"{context}: manual_github_workflow.workflow does not exist: "
            f"{workflow_path}"
        )

    suite = workflow.get("suite")
    if suite not in ALLOWED_MANUAL_WORKFLOW_SUITES:
        errors.append(
            f"{context}: manual_github_workflow.suite must be one of "
            f"{sorted(ALLOWED_MANUAL_WORKFLOW_SUITES)}; got {suite!r}"
        )

    if workflow_path == LLM_INTEGRATION_WORKFLOW_PATH:
        kitaru_ref = workflow.get("kitaru_ref")
        if not isinstance(kitaru_ref, str) or not kitaru_ref.strip():
            errors.append(
                f"{context}: manual_github_workflow.kitaru_ref must be explicitly "
                "set for llm-integration.yml"
            )

    marker_expression = workflow.get("marker_expression")
    if not isinstance(marker_expression, str) or not marker_expression.strip():
        errors.append(
            f"{context}: manual_github_workflow.marker_expression is required"
        )
    elif suite == "provider-extended" and "provider_extended" not in marker_expression:
        errors.append(
            f"{context}: provider-extended manual_github_workflow must use a "
            "provider_extended marker_expression"
        )

    scope = workflow.get("scope")
    if suite == "provider-extended" and scope != EXPECTED_MANUAL_WORKFLOW_SCOPE:
        errors.append(
            f"{context}: provider-extended manual_github_workflow.scope must be "
            f"{EXPECTED_MANUAL_WORKFLOW_SCOPE!r} to show the dispatch runs "
            "the full group"
        )

    provider_input_values: dict[str, bool] = {}
    for flag_name in LLM_INTEGRATION_PROVIDER_INPUTS:
        flag_value = workflow.get(flag_name)
        if workflow_path == LLM_INTEGRATION_WORKFLOW_PATH and not isinstance(
            flag_value, bool
        ):
            errors.append(
                f"{context}: manual_github_workflow.{flag_name} must be explicitly "
                "set to true or false for llm-integration.yml"
            )
            continue
        if flag_value is not None and not isinstance(flag_value, bool):
            errors.append(
                f"{context}: manual_github_workflow.{flag_name} must be true or false"
            )
            continue
        if isinstance(flag_value, bool):
            provider_input_values[flag_name] = flag_value

    if workflow_path == LLM_INTEGRATION_WORKFLOW_PATH:
        enabled_provider_inputs = [
            flag_name
            for flag_name, flag_value in provider_input_values.items()
            if flag_value
        ]
        if len(enabled_provider_inputs) != 1:
            errors.append(
                f"{context}: llm-integration.yml manual_github_workflow must enable "
                "exactly one provider input; got "
                f"{enabled_provider_inputs or 'none'}"
            )

    test_file = workflow.get("test_file")
    test_names = workflow.get("test_names")
    if not isinstance(test_file, str) or not test_file:
        errors.append(f"{context}: manual_github_workflow.test_file is required")
        return errors
    test_path = ROOT / test_file
    if not test_path.is_file():
        errors.append(
            f"{context}: manual_github_workflow.test_file does not exist: {test_file}"
        )
        return errors
    if not _is_non_empty_string_list(test_names):
        errors.append(
            f"{context}: manual_github_workflow.test_names must list at least one test"
        )
        return errors

    available_tests = set(
        re.findall(
            r"^def (test_[A-Za-z0-9_]+)\(",
            test_path.read_text(encoding="utf-8"),
            re.MULTILINE,
        )
    )
    for test_name in cast(list[str], test_names):
        if test_name not in available_tests:
            errors.append(
                f"{context}: manual_github_workflow.test_names references missing "
                f"test {test_name!r} in {test_file}"
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


def _is_llm_integration_manual_command(command: str) -> bool:
    parts = shlex.split(command)
    return (
        "gh" in parts
        and "workflow" in parts
        and "run" in parts
        and ("llm-integration.yml" in parts or LLM_INTEGRATION_WORKFLOW_PATH in parts)
    )


def _command_sets_input(command: str, input_name: str) -> bool:
    parts = shlex.split(command)
    for index, part in enumerate(parts):
        if part in {"-f", "--field"}:
            next_part = parts[index + 1] if index + 1 < len(parts) else ""
            if next_part.startswith(f"{input_name}="):
                return True
        if part.startswith(f"-f{input_name}="):
            return True
        if part.startswith(f"--field={input_name}="):
            return True
    return False


def _public_example_paths_from_docs(examples: list[object]) -> set[str]:
    paths: set[str] = set()
    doc_paths = set(DOC_LIST_PATHS)
    for entry in examples:
        if not isinstance(entry, dict):
            continue
        public_docs = entry.get("public_docs")
        if not isinstance(public_docs, list):
            continue
        doc_paths.update(
            ROOT / doc_path for doc_path in public_docs if isinstance(doc_path, str)
        )
    for doc_path in doc_paths:
        if doc_path.is_file():
            paths.update(_example_paths_from_doc(doc_path))
    return paths


def _example_paths_from_doc(doc_path: Path) -> set[str]:
    paths: set[str] = set()
    text = doc_path.read_text(encoding="utf-8")
    for raw_path in EXAMPLE_PATH_RE.findall(text):
        path = _normalize_example_path(raw_path)
        if not _should_ignore_public_path(path):
            paths.add(path)
    for raw_target in MARKDOWN_LINK_TARGET_RE.findall(text):
        if "://" in raw_target:
            continue
        target = raw_target.split("#", 1)[0]
        try:
            relative_path = (
                (doc_path.parent / target).resolve().relative_to(ROOT.resolve())
            )
        except ValueError:
            continue
        path = _normalize_example_path(relative_path.as_posix())
        if path.startswith("examples/") and not _should_ignore_public_path(path):
            paths.add(path)
    return paths


def _doc_is_within_example(doc_path: Path, example_path: str) -> bool:
    example_abs = ROOT / example_path
    example_root = example_abs if example_abs.is_dir() else example_abs.parent
    return doc_path.resolve().is_relative_to(example_root.resolve())


def _normalize_example_path(path: str) -> str:
    normalized = path.split("#", 1)[0].strip("`.,) ")
    normalized = normalized.rstrip("/")
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
    return paths


if __name__ == "__main__":
    raise SystemExit(main())
