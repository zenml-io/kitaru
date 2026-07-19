"""Tests for external trace import CLI commands."""

from __future__ import annotations

import json
from dataclasses import replace
from unittest.mock import Mock, patch

import pytest

from kitaru.cli import app
from kitaru.imports import (
    CapabilityReadiness,
    ImportOutcomeStatus,
    LangfuseFetchProvenance,
    LangfuseImportResult,
    ProviderVersionStamp,
    ProviderVersionStampKind,
    ReplayCapability,
    ReplayDiagnostic,
    ReplayDiagnosticCode,
    ReplayReadinessStatus,
    ReplayReadinessSummary,
    SourceAttribution,
    SourceAttributionStatus,
    TraceImportOutcome,
    TraceIntegrity,
)


def _readiness() -> ReplayReadinessSummary:
    return ReplayReadinessSummary(
        root_input_candidate_rerun=CapabilityReadiness(
            capability=ReplayCapability.ROOT_INPUT_CANDIDATE_RERUN,
            status=ReplayReadinessStatus.READY,
        ),
        model_message_reconstruction=CapabilityReadiness(
            capability=ReplayCapability.MODEL_MESSAGE_RECONSTRUCTION,
            status=ReplayReadinessStatus.UNKNOWN,
        ),
        tool_result_boundary_reconstruction=CapabilityReadiness(
            capability=ReplayCapability.TOOL_RESULT_BOUNDARY_RECONSTRUCTION,
            status=ReplayReadinessStatus.UNSUPPORTED,
            diagnostics=(
                ReplayDiagnostic(
                    code=ReplayDiagnosticCode.TOOL_CALL_WITHOUT_RESULT,
                    observation_id="observation-one",
                ),
            ),
        ),
        recorded_response_matching=CapabilityReadiness(
            capability=ReplayCapability.RECORDED_RESPONSE_MATCHING,
            status=ReplayReadinessStatus.UNKNOWN,
        ),
        candidate_tool_compatibility=CapabilityReadiness(
            capability=ReplayCapability.CANDIDATE_TOOL_COMPATIBILITY,
            status=ReplayReadinessStatus.UNKNOWN,
            diagnostics=(
                ReplayDiagnostic(
                    code=ReplayDiagnosticCode.CANDIDATE_TOOL_CONTRACT_UNKNOWN
                ),
            ),
        ),
    )


def _result(
    *,
    dry_run: bool = True,
    status: ImportOutcomeStatus = ImportOutcomeStatus.WOULD_CREATE,
    existing_execution_id: str | None = None,
    reason: str | None = None,
    resolution: str | None = None,
    cohort_tag: str | None = "customer-a",
    with_fetch_provenance: bool = False,
) -> LangfuseImportResult:
    return LangfuseImportResult(
        dry_run=dry_run,
        source_project_id="source-project",
        agent_name="support-agent",
        agent_id="agent-project-id",
        agent_version_id="agent-version-id",
        pipeline_id="agent-version-id",
        pipeline_name="support_agent__av_test",
        requested_version="prod",
        requested_alias="prod",
        cohort_tag=cohort_tag,
        project_name="support-agent",
        project_id="project-id",
        stack_name="cloud-stack",
        stack_id="stack-id",
        stack_was_explicit=True,
        artifact_store_type="s3",
        artifact_store_is_local=False,
        artifact_store_is_remotely_accessible=True,
        total_trace_count=3,
        selected_trace_count=1,
        fetch_provenance=(
            LangfuseFetchProvenance(
                api_resource="observations_v2",
                base_url="https://cloud.langfuse.com",
                field_groups=("core", "io"),
                page_count=2,
            )
            if with_fetch_provenance
            else None
        ),
        outcomes=(
            TraceImportOutcome(
                trace_id="trace-one",
                integrity=TraceIntegrity.COMPLETE,
                observation_count=4,
                status=status,
                attribution=SourceAttribution(
                    status=SourceAttributionStatus.SOURCE_VERIFIED,
                    stamps=(
                        ProviderVersionStamp(
                            kind=ProviderVersionStampKind.TRACE_VERSION,
                            value="prod",
                            source_field="trace.version",
                        ),
                    ),
                ),
                raw_evidence_digest="a" * 64,
                raw_evidence_artifact_id=(None if dry_run else "raw-artifact-id"),
                raw_evidence_schema_version=(None if dry_run else 1),
                replay_bundle_digest="b" * 64,
                replay_bundle_artifact_id=(None if dry_run else "replay-artifact-id"),
                replay_bundle_schema_version=(None if dry_run else 1),
                replay_readiness=_readiness(),
                execution_id=("execution-one" if not dry_run else None),
                existing_execution_id=existing_execution_id,
                reason=reason,
                resolution=resolution,
            ),
        ),
    )


def test_langfuse_import_help_describes_path_and_uri_sources(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        app(["import", "langfuse", "--help"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    compact_output = " ".join(output.replace("│", " ").split())
    assert "langfuse://trace/TRACE_ID" in compact_output
    assert "Required for JSONL; optional" in compact_output


def test_langfuse_import_defaults_to_read_only_preview(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()
    fake_client.imports.langfuse.return_value = _result()

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent",
                "support-agent",
                "--agent-version",
                "prod",
                "--stack",
                "cloud-stack",
                "--trace-id",
                "trace-two",
                "--trace-id",
                "trace-one",
                "--limit",
                "1",
                "--allow-fragmented",
                "--max-workers",
                "2",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.imports.langfuse.assert_called_once_with(
        "export.jsonl",
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        stack="cloud-stack",
        trace_ids=["trace-two", "trace-one"],
        limit=1,
        dry_run=True,
        confirm_data_storage=False,
        allow_fragmented=True,
        max_workers=2,
        cohort_tag=None,
    )
    output = capsys.readouterr().out
    assert "Langfuse trace import" in output
    assert "Preview" in output
    assert "support_agent__av_test" in output
    assert "Trace: trace-one" in output
    assert "would_create" in output
    assert "source_verified" in output
    assert "root_input_candidate_rerun=ready" in output
    assert "a" * 64 in output


@pytest.mark.parametrize(
    ("write_args", "dry_run", "status"),
    [
        ([], True, ImportOutcomeStatus.WOULD_CREATE),
        (
            ["--write", "--confirm-data-storage"],
            False,
            ImportOutcomeStatus.CREATED,
        ),
    ],
)
def test_langfuse_uri_forwards_for_preview_and_write(
    write_args: list[str],
    dry_run: bool,
    status: ImportOutcomeStatus,
) -> None:
    fake_client = Mock()
    fake_client.imports.langfuse.return_value = _result(
        dry_run=dry_run,
        status=status,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "langfuse://trace/trace-one",
                "--agent",
                "support-agent",
                "--agent-version",
                "prod",
                "--stack",
                "cloud-stack",
                *write_args,
            ]
        )

    assert exc_info.value.code == 0
    fake_client.imports.langfuse.assert_called_once_with(
        "langfuse://trace/trace-one",
        source_project_id=None,
        agent="support-agent",
        version="prod",
        stack="cloud-stack",
        trace_ids=None,
        limit=None,
        dry_run=dry_run,
        confirm_data_storage=not dry_run,
        allow_fragmented=False,
        max_workers=1,
        cohort_tag=None,
    )


def test_langfuse_jsonl_requires_source_project_id(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--agent",
                "support-agent",
                "--agent-version",
                "prod",
            ]
        )

    assert exc_info.value.code == 1
    fake_client.imports.langfuse.assert_not_called()
    assert "--source-project-id" in capsys.readouterr().err


def test_langfuse_import_write_requires_storage_confirmation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent",
                "support-agent",
                "--agent-version",
                "prod",
                "--write",
            ]
        )

    assert exc_info.value.code == 1
    fake_client.imports.langfuse.assert_not_called()
    assert "--confirm-data-storage" in capsys.readouterr().err


def test_langfuse_import_confirmation_requires_write(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent",
                "support-agent",
                "--agent-version",
                "prod",
                "--confirm-data-storage",
            ]
        )

    assert exc_info.value.code == 1
    fake_client.imports.langfuse.assert_not_called()
    assert "requires `--write`" in capsys.readouterr().err


def test_langfuse_import_write_forwards_explicit_consent(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()
    fake_client.imports.langfuse.return_value = _result(
        dry_run=False,
        status=ImportOutcomeStatus.CREATED,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent",
                "support-agent",
                "--agent-version",
                "prod",
                "--cohort-tag",
                "customer-a",
                "--write",
                "--confirm-data-storage",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.imports.langfuse.assert_called_once_with(
        "export.jsonl",
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        stack=None,
        trace_ids=None,
        limit=None,
        dry_run=False,
        confirm_data_storage=True,
        allow_fragmented=False,
        max_workers=1,
        cohort_tag="customer-a",
    )
    assert "No --stack was specified" in capsys.readouterr().err


def test_langfuse_import_json_emits_one_structured_result(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()
    fake_client.imports.langfuse.return_value = _result(with_fetch_provenance=True)

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent",
                "support-agent",
                "--agent-version",
                "prod",
                "--output",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "import.langfuse"
    assert payload["item"]["dry_run"] is True
    assert payload["item"]["flow_name"] == "support_agent__av_test"
    assert payload["item"]["counts"] == {"would_create": 1}
    assert payload["item"]["attribution_counts"] == {"source_verified": 1}
    assert payload["item"]["cohort_tag"] == "customer-a"
    assert payload["item"]["fetch_provenance"] == {
        "api_resource": "observations_v2",
        "base_url": "https://cloud.langfuse.com",
        "base_url_source": "default",
        "field_groups": ["core", "io"],
        "page_count": 2,
    }
    assert payload["item"]["agent"] == {
        "id": "agent-project-id",
        "name": "support-agent",
    }
    assert payload["item"]["agent_version"] == {
        "id": "agent-version-id",
        "pipeline_id": "agent-version-id",
        "pipeline_name": "support_agent__av_test",
        "requested_version": "prod",
        "requested_alias": "prod",
    }
    assert payload["item"]["project"] == {
        "id": "project-id",
        "name": "support-agent",
    }
    assert payload["item"]["stack"] == {
        "id": "stack-id",
        "name": "cloud-stack",
        "explicitly_selected": True,
    }
    assert payload["item"]["artifact_store"] == {
        "type": "s3",
        "is_local": False,
        "is_remotely_accessible": True,
    }
    assert payload["item"]["outcomes"] == [
        {
            "trace_id": "trace-one",
            "integrity": "complete",
            "observation_count": 4,
            "status": "would_create",
            "attribution": {
                "status": "source_verified",
                "stamps": [
                    {
                        "kind": "trace_version",
                        "value": "prod",
                        "source_field": "trace.version",
                    }
                ],
                "diagnostics": [],
            },
            "raw_evidence_digest": "a" * 64,
            "raw_evidence_artifact_id": None,
            "raw_evidence_schema_version": None,
            "replay_bundle_digest": "b" * 64,
            "replay_bundle_artifact_id": None,
            "replay_bundle_schema_version": None,
            "replay_readiness": _readiness().model_dump(mode="json"),
            "execution_id": None,
            "existing_execution_id": None,
            "reason": None,
            "resolution": None,
        }
    ]


def test_langfuse_import_partial_failure_exits_nonzero_after_rendering(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()
    fake_client.imports.langfuse.return_value = _result(
        status=ImportOutcomeStatus.REJECTED
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent",
                "support-agent",
                "--agent-version",
                "prod",
            ]
        )

    assert exc_info.value.code == 1
    assert "rejected" in capsys.readouterr().out


def test_langfuse_import_write_prints_copyable_execution_id(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()
    result = _result(
        dry_run=False,
        status=ImportOutcomeStatus.CREATED,
    )
    first_execution_id = "2820d2f5-1d7c-4b5a-92de-876e3b268e5a"
    second_execution_id = "8fc60a84-7f84-4a9c-85de-0668fd135daa"
    first_outcome = replace(result.outcomes[0], execution_id=first_execution_id)
    second_outcome = replace(
        first_outcome,
        trace_id="trace-two",
        execution_id=second_execution_id,
    )
    fake_client.imports.langfuse.return_value = replace(
        result,
        selected_trace_count=2,
        outcomes=(first_outcome, second_outcome),
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent",
                "support-agent",
                "--agent-version",
                "prod",
                "--write",
                "--confirm-data-storage",
            ]
        )

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert output.count("Trace: trace-one") == 1
    assert output.count("Trace: trace-two") == 1
    assert "Execution ID" in output
    assert first_execution_id in output
    assert second_execution_id in output
    assert "Trace outcomes" not in output


def test_langfuse_import_write_json_keeps_warning_out_of_payload(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()
    fake_client.imports.langfuse.return_value = _result(
        dry_run=False,
        status=ImportOutcomeStatus.CREATED,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent",
                "support-agent",
                "--agent-version",
                "prod",
                "--write",
                "--confirm-data-storage",
                "--output",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["item"]["outcomes"][0]["execution_id"] == "execution-one"
    assert "No --stack was specified" in captured.err


def test_langfuse_import_conflict_prints_existing_execution_and_next_action(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()
    fake_client.imports.langfuse.return_value = _result(
        status=ImportOutcomeStatus.CONFLICT,
        existing_execution_id="2820d2f5-1d7c-4b5a-92de-876e3b268e5a",
        reason="Already imported with a different source AgentVersion.",
        resolution="Retry with the original source AgentVersion.",
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent",
                "support-agent",
                "--agent-version",
                "prod",
            ]
        )

    assert exc_info.value.code == 1
    output = capsys.readouterr().out
    assert "Trace: trace-one" in output
    assert "Existing execution" in output
    assert "2820d2f5-1d7c-4b5a-92de-876e3b268e5a" in output
    assert "Problem" in output
    assert "Already imported with a different source AgentVersion." in output
    assert "Next action" in output
    assert "Retry with the original source AgentVersion." in output
