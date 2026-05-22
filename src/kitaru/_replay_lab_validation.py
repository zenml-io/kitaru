"""Validation helpers for Replay Lab manifests and descriptors."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from kitaru._replay_lab_models import (
    DEFAULT_EXPECTED_ARTIFACTS,
    EVALUATOR_ERROR_POLICIES,
    EVALUATOR_PRECEDENCE_POLICIES,
    CandidateDescriptor,
    EvaluatorDescriptor,
    ReplayLabCase,
    ReplayLabManifest,
)


def load_manifest(
    manifest: Mapping[str, Any] | None = None,
    *,
    manifest_path: str | Path | None = None,
) -> ReplayLabManifest:
    """Load and validate a Replay Lab manifest from an object or a path."""
    if manifest is None and manifest_path is None:
        raise ValueError("Provide either `manifest` or `manifest_path`.")
    if manifest is not None and manifest_path is not None:
        raise ValueError("Provide only one of `manifest` or `manifest_path`.")

    raw = dict(manifest) if manifest is not None else _read_manifest_file(manifest_path)
    return validate_manifest(raw)


def validate_manifest(raw: Mapping[str, Any]) -> ReplayLabManifest:
    """Validate a raw v0 cohort manifest mapping."""
    name = _required_str(raw, "name", aliases=("cohort_name",))
    description = _optional_str(raw.get("description"), default="")
    default_from_checkpoint = _required_str(
        raw,
        "default_from_checkpoint",
        aliases=("default_replay_checkpoint", "from_checkpoint", "checkpoint"),
    )

    expected_artifacts = _validate_string_list(
        raw.get("expected_artifacts", list(DEFAULT_EXPECTED_ARTIFACTS)),
        field_name="expected_artifacts",
    )
    if not expected_artifacts:
        expected_artifacts = list(DEFAULT_EXPECTED_ARTIFACTS)

    cases = _validate_cases(raw, default_from_checkpoint=default_from_checkpoint)
    if not cases:
        raise ValueError("Manifest must include at least one case or execution ID.")

    return ReplayLabManifest(
        name=name,
        description=description,
        default_from_checkpoint=default_from_checkpoint,
        cases=cases,
        expected_artifacts=expected_artifacts,
    )


def validate_candidate_descriptor(raw: Mapping[str, Any]) -> CandidateDescriptor:
    """Validate the v0 candidate descriptor shape."""
    candidate_id = _required_str(raw, "id")
    label = _required_str(raw, "label")
    flow_inputs = _optional_mapping(raw.get("flow_inputs"), field_name="flow_inputs")
    checkpoint_overrides = _optional_mapping(
        raw.get("checkpoint_overrides"), field_name="checkpoint_overrides"
    )
    notes = raw.get("notes")
    if notes is not None and not isinstance(notes, str):
        raise ValueError("Candidate `notes` must be a string when provided.")

    unsupported = set(raw) - {
        "id",
        "label",
        "flow_inputs",
        "checkpoint_overrides",
        "notes",
    }
    if unsupported:
        names = ", ".join(sorted(unsupported))
        raise ValueError(f"Unsupported candidate descriptor field(s): {names}.")

    return CandidateDescriptor(
        id=candidate_id,
        label=label,
        flow_inputs=flow_inputs,
        checkpoint_overrides=checkpoint_overrides,
        notes=notes,
    )


def validate_candidate_descriptors(
    raw_candidates: Sequence[Mapping[str, Any]],
) -> list[CandidateDescriptor]:
    """Validate the canonical plural candidate descriptor list."""
    if not isinstance(raw_candidates, Sequence) or isinstance(
        raw_candidates, str | bytes
    ):
        raise ValueError("`candidate_descriptors` must be a list of objects.")
    candidates = []
    for index, raw in enumerate(raw_candidates, start=1):
        if not isinstance(raw, Mapping):
            raise ValueError(f"`candidate_descriptors[{index}]` must be an object.")
        candidates.append(validate_candidate_descriptor(raw))
    if not candidates:
        raise ValueError("Provide at least one candidate descriptor.")
    seen_ids: set[str] = set()
    for candidate in candidates:
        if candidate.id in seen_ids:
            raise ValueError(f"Duplicate candidate descriptor id `{candidate.id}`.")
        seen_ids.add(candidate.id)
    return candidates


def validate_evaluator_descriptor(raw: Mapping[str, Any]) -> EvaluatorDescriptor:
    """Validate a serializable evaluator descriptor for a trusted local caller."""
    target = _required_str(raw, "target")
    if ":" not in target:
        raise ValueError("Evaluator `target` must use module:function format.")
    module_name, function_name = target.split(":", 1)
    if not module_name or not function_name:
        raise ValueError("Evaluator `target` must use module:function format.")
    if "/" in module_name or "\\" in module_name or module_name.endswith(".py"):
        raise ValueError("Evaluator `target` must be a module:function reference.")
    evaluator_id = raw.get("id", raw.get("evaluator_id"))
    if evaluator_id is not None and (
        not isinstance(evaluator_id, str) or not evaluator_id.strip()
    ):
        raise ValueError("Evaluator `id` must be a non-empty string when provided.")
    on_error = validate_optional_policy(
        raw.get("on_error", "warn"),
        field_name="on_error",
        allowed=EVALUATOR_ERROR_POLICIES,
    )
    precedence = validate_optional_policy(
        raw.get("precedence", "override"),
        field_name="precedence",
        allowed=EVALUATOR_PRECEDENCE_POLICIES,
    )
    unsupported = set(raw) - {"target", "id", "evaluator_id", "on_error", "precedence"}
    if unsupported:
        names = ", ".join(sorted(unsupported))
        raise ValueError(f"Unsupported evaluator descriptor field(s): {names}.")
    return EvaluatorDescriptor(
        target=target,
        id=evaluator_id.strip() if isinstance(evaluator_id, str) else None,
        on_error=on_error,
        precedence=precedence,
    )


def _read_manifest_file(path_like: str | Path | None) -> dict[str, Any]:
    if path_like is None:
        raise ValueError("`manifest_path` cannot be None.")
    path = Path(path_like)
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml
        except ImportError:
            raise ValueError("YAML manifests require PyYAML to be installed.") from None
        raw = yaml.safe_load(text)
    else:
        raw = json.loads(text)
    if not isinstance(raw, dict):
        raise ValueError("Manifest file must contain an object at the top level.")
    return raw


def _validate_cases(
    raw: Mapping[str, Any],
    *,
    default_from_checkpoint: str,
) -> list[ReplayLabCase]:
    raw_cases = raw.get("cases")
    if raw_cases is None and raw.get("observed_execution_ids") is not None:
        raw_cases = [
            {"case_id": str(index + 1), "exec_id": exec_id, "reason": "Selected case"}
            for index, exec_id in enumerate(raw["observed_execution_ids"])
        ]
    if not isinstance(raw_cases, list):
        raise ValueError("Manifest `cases` must be a list.")

    cases: list[ReplayLabCase] = []
    seen_case_ids: set[str] = set()
    for index, item in enumerate(raw_cases, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Manifest case #{index} must be an object.")
        case_id = _required_str(item, "case_id", aliases=("id",))
        if case_id in seen_case_ids:
            raise ValueError(f"Duplicate manifest case_id `{case_id}`.")
        seen_case_ids.add(case_id)
        exec_id = _required_str(
            item, "exec_id", aliases=("execution_id", "observed_exec_id")
        )
        reason = _reason_text(item.get("reason", item.get("reasons")))
        from_checkpoint = item.get("from_checkpoint", default_from_checkpoint)
        if not isinstance(from_checkpoint, str) or not from_checkpoint.strip():
            raise ValueError(
                f"Manifest case `{case_id}` has invalid `from_checkpoint`."
            )
        labels = _labels(item.get("labels", {}), case_id=case_id)
        cases.append(
            ReplayLabCase(
                case_id=case_id,
                exec_id=exec_id,
                reason=reason,
                from_checkpoint=from_checkpoint.strip(),
                labels=labels,
            )
        )
    return cases


def _reason_text(value: Any) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        text = "; ".join(item.strip() for item in value if item.strip())
        if text:
            return text
    raise ValueError("Manifest case `reason` must be a string or list of strings.")


def _labels(value: Any, *, case_id: str) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"Manifest case `{case_id}` labels must be an object.")
    return {str(key): str(label_value) for key, label_value in value.items()}


def _required_str(
    mapping: Mapping[str, Any],
    key: str,
    *,
    aliases: Sequence[str] = (),
) -> str:
    value = None
    matched_key = key
    for candidate_key in (key, *aliases):
        if candidate_key in mapping:
            value = mapping[candidate_key]
            matched_key = candidate_key
            break
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"`{matched_key}` must be a non-empty string.")
    return value.strip()


def _optional_str(value: Any, *, default: str) -> str:
    if value is None:
        return default
    if not isinstance(value, str):
        raise ValueError("Optional string field must be a string when provided.")
    return value


def _optional_mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"Candidate `{field_name}` must be an object when provided.")
    return dict(value)


def validate_optional_policy(value: Any, *, field_name: str, allowed: set[str]) -> str:
    if not isinstance(value, str) or value not in allowed:
        allowed_text = ", ".join(sorted(allowed))
        raise ValueError(f"`{field_name}` must be one of: {allowed_text}.")
    return value


def validate_optional_string_list(value: Any, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{field_name} must be a list of strings when provided.")
    return [item for item in value]


def _validate_string_list(value: Any, *, field_name: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"Manifest `{field_name}` must be a list of strings.")
    if not all(isinstance(item, str) and item.strip() for item in value):
        raise ValueError(
            f"Manifest `{field_name}` must be a list of non-empty strings."
        )
    return [item.strip() for item in value]
