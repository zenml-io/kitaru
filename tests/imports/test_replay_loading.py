"""Trusted loading tests for immutable imported replay evidence."""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

import kitaru.imports._replay_loading as loading_module
from kitaru._agent_registration import RegisteredAgentVersionBinding
from kitaru._config._agents import _AgentVersionManifest
from kitaru._import_contract import (
    IMPORT_AGENT_NAME_KEY,
    IMPORT_ATTRIBUTION_KEY,
    IMPORT_INTEGRITY_KEY,
    IMPORT_RAW_EVIDENCE_ARTIFACT_ID_KEY,
    IMPORT_RAW_EVIDENCE_DIGEST_KEY,
    IMPORT_RAW_EVIDENCE_SCHEMA_VERSION_KEY,
    IMPORT_REPLAY_BUNDLE_ARTIFACT_ID_KEY,
    IMPORT_REPLAY_BUNDLE_DIGEST_KEY,
    IMPORT_REPLAY_BUNDLE_SCHEMA_VERSION_KEY,
    IMPORT_REPLAY_PROFILE_VERSION_KEY,
    IMPORT_REPLAY_READINESS_KEY,
    IMPORT_SCHEMA_VERSION_KEY,
    IMPORT_SNAPSHOT_KIND_KEY,
    IMPORT_SOURCE_AGENT_VERSION_ID_KEY,
    IMPORT_SOURCE_FINGERPRINT_KEY,
    IMPORT_SOURCE_PIPELINE_ID_KEY,
    IMPORT_SOURCE_PROJECT_ID_KEY,
    IMPORT_SOURCE_PROVIDER_KEY,
    IMPORT_SOURCE_TRACE_ID_KEY,
    IMPORT_STATUS_KEY,
    IMPORTED_EXECUTION_ENVIRONMENT_KEY,
)
from kitaru.imports import (
    EvidenceRedactionStatus,
    ImportedReplayEvidenceError,
    ImportedReplayUnsupportedReason,
    ProviderVersionStamp,
    ProviderVersionStampKind,
    SourceAttribution,
    SourceAttributionStatus,
    TraceIntegrity,
    build_pydantic_ai_replay_evidence,
    build_raw_imported_evidence,
    load_imported_replay_evidence,
    read_langfuse_jsonl_records,
    sha256_canonical_json,
)
from kitaru.imports._normalization import normalize_langfuse_records

FIXTURE = Path(__file__).parent / "fixtures" / "langfuse_replay_evidence.jsonl"
EXECUTION_ID = "imported-execution"
PROJECT_ID = "agent-project"
PIPELINE_ID = "pipeline-id"
GIT_SHA = "0123456789abcdef0123456789abcdef01234567"


class _Artifact:
    def __init__(self, artifact_id: str, name: str, payload: dict[str, Any]) -> None:
        self.id = artifact_id
        self.name = name
        self.project_id = PROJECT_ID
        self._payload = payload

    def load(self) -> dict[str, Any]:
        return deepcopy(self._payload)


class _Client:
    def __init__(
        self,
        run: Any,
        *,
        raw_payload: dict[str, Any],
        replay_payload: dict[str, Any],
    ) -> None:
        self.run = run
        self.artifacts = {
            "raw-artifact": _Artifact(
                "raw-artifact",
                f"kitaru-import-{EXECUTION_ID}::raw_evidence",
                raw_payload,
            ),
            "replay-artifact": _Artifact(
                "replay-artifact",
                f"kitaru-import-{EXECUTION_ID}::replay_bundle",
                replay_payload,
            ),
        }

    def get_pipeline_run(self, **_kwargs: Any) -> Any:
        return self.run

    def get_artifact_version(self, *, name_id_or_prefix: str, **_kwargs: Any) -> Any:
        return self.artifacts[name_id_or_prefix]


def _binding() -> RegisteredAgentVersionBinding:
    manifest = _AgentVersionManifest(
        schema_version=1,
        agent_version_id=PIPELINE_ID,
        pipeline_id=PIPELINE_ID,
        pipeline_name="support_agent__av_test",
        fingerprint="f" * 64,
        git_sha=GIT_SHA,
        git_dirty=False,
        working_tree_hash=None,
        configuration_hash="c" * 64,
        worldview_hash="w" * 64,
        entrypoint="support_agent:agent",
        registered_at="2026-07-19T00:00:00+00:00",
        source="registration",
    )
    return RegisteredAgentVersionBinding(
        project_id=PROJECT_ID,
        manifest=manifest,
        agent_name="support-agent",
        project_name="Support agent",
        aliases=("stable",),
    )


def _evidence() -> tuple[Any, Any]:
    records = [
        record
        for record in read_langfuse_jsonl_records(FIXTURE)
        if record.row["traceId"] == "trace-alias"
    ]
    normalized = normalize_langfuse_records(
        records,
        project_id="source-project",
    )[0]
    raw = build_raw_imported_evidence(
        source=normalized.trace.source,
        records=normalized.records,
    )
    replay = build_pydantic_ai_replay_evidence(
        normalized.trace,
        raw_evidence=raw,
    )
    return raw, replay


def _case(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, _Client, Mock]:
    raw, replay = _evidence()
    source = raw.source
    environment = {
        IMPORTED_EXECUTION_ENVIRONMENT_KEY: True,
        IMPORT_SCHEMA_VERSION_KEY: 5,
        IMPORT_AGENT_NAME_KEY: "support-agent",
        IMPORT_SOURCE_AGENT_VERSION_ID_KEY: PIPELINE_ID,
        IMPORT_SOURCE_PIPELINE_ID_KEY: PIPELINE_ID,
        IMPORT_SOURCE_FINGERPRINT_KEY: "f" * 64,
        IMPORT_SOURCE_PROVIDER_KEY: source.provider,
        IMPORT_SOURCE_PROJECT_ID_KEY: source.project_id,
        IMPORT_SOURCE_TRACE_ID_KEY: source.trace_id,
        IMPORT_RAW_EVIDENCE_DIGEST_KEY: raw.raw_content_sha256,
        IMPORT_REPLAY_BUNDLE_DIGEST_KEY: replay.bundle.bundle_digest,
    }
    attribution = SourceAttribution(
        status=SourceAttributionStatus.SOURCE_VERIFIED,
        stamps=(
            ProviderVersionStamp(
                kind=ProviderVersionStampKind.GIT_SHA,
                value=GIT_SHA,
                source_field="metadata.git_sha",
            ),
        ),
    )
    metadata = {
        **environment,
        IMPORT_STATUS_KEY: "complete",
        IMPORT_SNAPSHOT_KIND_KEY: "imported_observed",
        IMPORT_INTEGRITY_KEY: TraceIntegrity.COMPLETE.value,
        IMPORT_ATTRIBUTION_KEY: attribution.model_dump(mode="json"),
        IMPORT_RAW_EVIDENCE_ARTIFACT_ID_KEY: "raw-artifact",
        IMPORT_RAW_EVIDENCE_SCHEMA_VERSION_KEY: raw.schema_version,
        IMPORT_REPLAY_BUNDLE_ARTIFACT_ID_KEY: "replay-artifact",
        IMPORT_REPLAY_BUNDLE_SCHEMA_VERSION_KEY: replay.bundle.schema_version,
        IMPORT_REPLAY_PROFILE_VERSION_KEY: replay.bundle.profile_version,
        IMPORT_REPLAY_READINESS_KEY: replay.readiness.model_dump(mode="json"),
    }
    run = SimpleNamespace(
        id=EXECUTION_ID,
        project_id=PROJECT_ID,
        orchestrator_environment=environment,
        run_metadata=metadata,
        snapshot=SimpleNamespace(
            project_id=PROJECT_ID,
            pipeline_id=PIPELINE_ID,
        ),
    )
    client = _Client(
        run,
        raw_payload=raw.model_dump(mode="json"),
        replay_payload=replay.bundle.model_dump(mode="json"),
    )
    resolver = Mock(return_value=_binding())
    monkeypatch.setattr(
        loading_module,
        "resolve_registered_agent_version",
        resolver,
    )
    return replay, client, resolver


def _assert_reason(
    reason: ImportedReplayUnsupportedReason,
    *,
    client: _Client,
) -> None:
    with pytest.raises(ImportedReplayEvidenceError) as exc_info:
        load_imported_replay_evidence(EXECUTION_ID, client=client)
    assert exc_info.value.reason is reason


def test_loads_verified_schema_v5_evidence_as_frozen_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replay, client, resolver = _case(monkeypatch)

    prepared = load_imported_replay_evidence(EXECUTION_ID, client=client)

    assert prepared.identity.execution_id == EXECUTION_ID
    assert prepared.identity.project_id == PROJECT_ID
    assert prepared.identity.source_agent_version_id == PIPELINE_ID
    assert prepared.identity.raw_evidence.artifact_id == "raw-artifact"
    assert prepared.replay_bundle == replay.bundle
    assert prepared.readiness == replay.readiness
    resolver.assert_called_once_with(
        client,
        agent=PROJECT_ID,
        version=PIPELINE_ID,
    )
    with pytest.raises(ValidationError):
        prepared.identity.execution_id = "changed"  # ty: ignore[invalid-assignment]


def test_legacy_import_fails_before_source_binding_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, client, resolver = _case(monkeypatch)
    client.run.orchestrator_environment[IMPORT_SCHEMA_VERSION_KEY] = 4

    _assert_reason(ImportedReplayUnsupportedReason.LEGACY_IMPORT, client=client)

    resolver.assert_not_called()


def test_swapped_artifact_references_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, client, _ = _case(monkeypatch)
    metadata = client.run.run_metadata
    metadata[IMPORT_RAW_EVIDENCE_ARTIFACT_ID_KEY] = "replay-artifact"
    metadata[IMPORT_REPLAY_BUNDLE_ARTIFACT_ID_KEY] = "raw-artifact"

    _assert_reason(
        ImportedReplayUnsupportedReason.ARTIFACT_ROLE_MISMATCH,
        client=client,
    )


def test_redacted_evidence_fails_before_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, client, _ = _case(monkeypatch)
    client.artifacts["raw-artifact"]._payload["redaction_status"] = (
        EvidenceRedactionStatus.PARTIALLY_REDACTED.value
    )

    _assert_reason(
        ImportedReplayUnsupportedReason.EVIDENCE_REDACTED,
        client=client,
    )


def test_missing_root_input_fails_before_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, client, _ = _case(monkeypatch)
    payload = client.artifacts["replay-artifact"]._payload
    payload["root_input_present"] = False
    payload["root_input"] = None
    payload["bundle_digest"] = sha256_canonical_json(
        {key: value for key, value in payload.items() if key != "bundle_digest"}
    )
    client.run.orchestrator_environment[IMPORT_REPLAY_BUNDLE_DIGEST_KEY] = payload[
        "bundle_digest"
    ]

    _assert_reason(
        ImportedReplayUnsupportedReason.ROOT_INPUT_MISSING,
        client=client,
    )


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (
            lambda client: client.run.run_metadata.__setitem__(
                IMPORT_RAW_EVIDENCE_SCHEMA_VERSION_KEY, 2
            ),
            ImportedReplayUnsupportedReason.ARTIFACT_SCHEMA_MISMATCH,
        ),
        (
            lambda client: client.run.orchestrator_environment.__setitem__(
                IMPORT_RAW_EVIDENCE_DIGEST_KEY, "0" * 64
            ),
            ImportedReplayUnsupportedReason.ARTIFACT_HASH_MISMATCH,
        ),
        (
            lambda client: client.artifacts.pop("raw-artifact"),
            ImportedReplayUnsupportedReason.RAW_EVIDENCE_UNAVAILABLE,
        ),
    ],
)
def test_schema_hash_and_missing_artifact_drift_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    mutation: Any,
    reason: ImportedReplayUnsupportedReason,
) -> None:
    _, client, _ = _case(monkeypatch)
    mutation(client)

    _assert_reason(reason, client=client)
