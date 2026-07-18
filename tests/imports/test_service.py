"""Tests for the public Langfuse batch-import service."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from kitaru._agent_registration import RegisteredAgentVersionBinding
from kitaru._config._agents import _AgentVersionManifest
from kitaru.errors import KitaruUsageError
from kitaru.imports import (
    ImportedExecutionResult,
    ImportedTraceConflictError,
    ImportedTraceWriteError,
    ImportOutcomeStatus,
    LangfuseSourceRecord,
    SourceAttributionStatus,
    import_langfuse_jsonl,
)

# Captured before the autouse fixture stubs it out on the module.
from kitaru.imports._service import _load_artifact_store as _real_load_artifact_store
from kitaru.imports._writer import ImportedTracePlan

FIXTURE = Path(__file__).parent / "fixtures" / "langfuse_observations.jsonl"
EVIDENCE_FIXTURE = Path(__file__).parent / "fixtures" / "langfuse_replay_evidence.jsonl"


def _manifest() -> _AgentVersionManifest:
    return _AgentVersionManifest(
        schema_version=1,
        agent_version_id="pipeline-id",
        pipeline_id="pipeline-id",
        pipeline_name="support_agent__av_test",
        fingerprint="sha256:fingerprint",
        git_sha="0123456789abcdef0123456789abcdef01234567",
        git_dirty=False,
        working_tree_hash=None,
        configuration_hash="sha256:configuration",
        worldview_hash="sha256:worldview",
        entrypoint="tests.test_imported_trace_sdk:REGISTERED_IMPORT_AGENT",
        registered_at="2026-07-18T10:00:00Z",
        source="registration",
    )


def _client(*, remote_server: bool = False) -> MagicMock:
    client = MagicMock()
    client.active_project = SimpleNamespace(id=UUID(int=1), name="default")
    stack = SimpleNamespace(
        id=UUID(int=2),
        project_id=UUID(int=1),
        name="active-stack",
        components={
            "artifact_store": [
                SimpleNamespace(flavor_name="local", name="local-artifacts")
            ]
        },
    )
    client.active_stack_model = stack
    client.get_stack.return_value = stack
    client.zen_store.is_local_store.return_value = not remote_server
    return client


@pytest.fixture(autouse=True)
def _stub_artifact_store_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "kitaru.imports._service.resolve_registered_agent_version",
        lambda client, *, agent, version: RegisteredAgentVersionBinding(
            project_id=str(client.active_project.id),
            manifest=_manifest(),
            agent_name=agent,
            project_name=str(client.active_project.name),
            aliases=("prod",),
            requested_alias=version if version == "prod" else None,
        ),
    )
    monkeypatch.setattr(
        "kitaru.imports._service._load_artifact_store",
        lambda component: SimpleNamespace(
            config=SimpleNamespace(is_local=component.flavor_name == "local")
        ),
    )


def test_dry_run_plans_without_persisting(monkeypatch: pytest.MonkeyPatch) -> None:
    planned: list[str] = []

    def plan(
        trace,
        *,
        binding,
        raw_evidence,
        replay_evidence,
        cohort_tag,
        client,
        stack_id,
    ):
        del (
            binding,
            raw_evidence,
            replay_evidence,
            cohort_tag,
            client,
            stack_id,
        )
        planned.append(trace.source.trace_id)
        return ImportedTracePlan.CREATE

    def fail_persist(*args, **kwargs):
        del args, kwargs
        raise AssertionError("dry-run must not persist")

    monkeypatch.setattr("kitaru.imports._service.plan_imported_trace", plan)
    monkeypatch.setattr("kitaru.imports._service.persist_imported_trace", fail_persist)

    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        trace_ids=["trace-complete"],
        dry_run=True,
        client=_client(),
    )

    assert planned == ["trace-complete"]
    assert result.dry_run is True
    assert result.total_trace_count == 3
    assert result.selected_trace_count == 1
    assert result.outcomes[0].status is ImportOutcomeStatus.WOULD_CREATE
    assert result.outcomes[0].execution_id is None
    assert result.flow_name == "support_agent__av_test"
    assert result.pipeline_name == "support_agent__av_test"
    assert result.outcomes[0].raw_evidence_artifact_id is None
    assert result.outcomes[0].raw_evidence_schema_version is None
    assert result.outcomes[0].replay_bundle_artifact_id is None
    assert result.outcomes[0].replay_bundle_schema_version is None
    assert "raw trace rows and normalized replay evidence" in result.storage_warning
    assert result.pipeline_id == "pipeline-id"
    assert result.stack_name == "active-stack"
    assert result.stack_was_explicit is False
    assert "No stack was specified" in result.storage_warning


def test_explicit_stack_is_resolved_once_and_forwarded(monkeypatch) -> None:
    client = _client()
    selected = SimpleNamespace(
        id=UUID(int=3),
        project_id=client.active_project.id,
        name="cloud-stack",
        components={
            "artifact_store": [SimpleNamespace(flavor_name="s3", name="artifacts")]
        },
    )
    client.get_stack.return_value = selected
    artifact_store = SimpleNamespace(config=SimpleNamespace(is_local=False))
    monkeypatch.setattr(
        "kitaru.imports._service._load_artifact_store",
        lambda component: artifact_store,
    )
    persisted = MagicMock(
        return_value=ImportedExecutionResult(
            execution_id="execution-one",
            created=True,
            resumed=False,
            observation_count=6,
            raw_evidence_artifact_id="raw-artifact-id",
            raw_evidence_schema_version=1,
            replay_bundle_artifact_id="replay-artifact-id",
            replay_bundle_schema_version=1,
        )
    )
    monkeypatch.setattr("kitaru.imports._service.persist_imported_trace", persisted)

    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        trace_ids=["trace-complete"],
        stack="cloud-stack",
        cohort_tag="customer-a",
        dry_run=False,
        confirm_data_storage=True,
        client=client,
    )

    client.get_stack.assert_called_once_with(
        "cloud-stack", allow_name_prefix_match=False, hydrate=True
    )
    client.activate_stack.assert_not_called()
    assert persisted.call_args.kwargs["stack_id"] == selected.id
    assert persisted.call_args.kwargs["artifact_store"] is artifact_store
    assert persisted.call_args.kwargs["binding"].pipeline_id == "pipeline-id"
    assert persisted.call_args.kwargs["cohort_tag"] == "customer-a"
    assert result.cohort_tag == "customer-a"
    assert (
        persisted.call_args.kwargs["raw_evidence"].raw_content_sha256
        == result.outcomes[0].raw_evidence_digest
    )
    assert (
        persisted.call_args.kwargs["replay_evidence"].bundle.bundle_digest
        == result.outcomes[0].replay_bundle_digest
    )
    assert result.outcomes[0].raw_evidence_artifact_id == "raw-artifact-id"
    assert result.outcomes[0].raw_evidence_schema_version == 1
    assert result.outcomes[0].replay_bundle_artifact_id == "replay-artifact-id"
    assert result.outcomes[0].replay_bundle_schema_version == 1
    assert result.stack_name == "cloud-stack"
    assert result.artifact_store_type == "s3"
    assert result.artifact_store_is_local is False
    assert result.storage_warning.startswith("Writing imported traces")
    assert "No stack was specified" not in result.storage_warning


def test_remote_server_with_local_artifact_store_warns(monkeypatch) -> None:
    client = _client(remote_server=True)
    monkeypatch.setattr(
        "kitaru.imports._service._load_artifact_store",
        lambda component: SimpleNamespace(config=SimpleNamespace(is_local=True)),
    )
    monkeypatch.setattr(
        "kitaru.imports._service.plan_imported_trace",
        lambda *args, **kwargs: "create",
    )

    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        trace_ids=["trace-complete"],
        client=client,
    )

    assert result.artifact_store_is_local is True
    assert result.artifact_store_is_remotely_accessible is False
    assert "execution metadata will be stored on the remote server" in (
        result.storage_warning
    )
    assert "artifact payloads will remain on this machine" in result.storage_warning


def test_write_requires_explicit_data_storage_confirmation() -> None:
    with pytest.raises(KitaruUsageError, match="confirm_data_storage=True"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent="support-agent",
            version="prod",
            dry_run=False,
            client=_client(),
        )


def test_trace_selection_reports_missing_ids() -> None:
    with pytest.raises(KitaruUsageError, match="missing-trace"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent="support-agent",
            version="prod",
            trace_ids=["trace-complete", "missing-trace"],
            client=_client(),
        )


def test_fragmented_trace_is_rejected_by_default() -> None:
    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        trace_ids=["trace-fragmented"],
        client=_client(),
    )

    assert result.outcomes[0].status is ImportOutcomeStatus.REJECTED
    assert "fragmented" in (result.outcomes[0].reason or "")


def test_incomplete_trace_is_reported_as_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "kitaru.imports._service.read_langfuse_jsonl_records",
        lambda path: iter(
            [
                LangfuseSourceRecord(
                    raw_text=(
                        '{"id":"unfinished","traceId":"trace-incomplete",'
                        '"type":"AGENT","startTime":"2026-07-15T10:00:00Z"}\n'
                    ),
                    row={
                        "id": "unfinished",
                        "traceId": "trace-incomplete",
                        "type": "AGENT",
                        "startTime": "2026-07-15T10:00:00Z",
                    },
                    line_number=1,
                    source_order=0,
                )
            ]
        ),
    )

    result = import_langfuse_jsonl(
        "unused.jsonl",
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        client=_client(),
    )

    assert result.outcomes[0].status is ImportOutcomeStatus.REJECTED
    assert "terminal status" in (result.outcomes[0].reason or "")


def test_limit_applies_after_explicit_trace_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "kitaru.imports._service.plan_imported_trace",
        lambda *args, **kwargs: "create",
    )

    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        trace_ids=["trace-root-omitted", "trace-complete"],
        limit=1,
        client=_client(),
    )

    assert [outcome.trace_id for outcome in result.outcomes] == ["trace-root-omitted"]


def test_exact_selection_ignores_invalid_unselected_trace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        {
            "id": "good-observation",
            "traceId": "trace-good",
            "type": "SPAN",
            "startTime": "2026-07-15T10:00:00Z",
            "endTime": "2026-07-15T10:00:01Z",
        },
        {
            "id": "bad-observation",
            "traceId": "trace-bad",
            "type": "UNSUPPORTED",
            "startTime": "not-a-timestamp",
        },
    ]
    monkeypatch.setattr(
        "kitaru.imports._service.read_langfuse_jsonl_records",
        lambda path: iter(
            LangfuseSourceRecord(
                raw_text=json.dumps(row, separators=(",", ":")) + "\n",
                row=row,
                line_number=index,
                source_order=index - 1,
            )
            for index, row in enumerate(rows, start=1)
        ),
    )
    monkeypatch.setattr(
        "kitaru.imports._service.plan_imported_trace",
        lambda *args, **kwargs: "create",
    )

    result = import_langfuse_jsonl(
        "unused.jsonl",
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        trace_ids=["trace-good"],
        client=_client(),
    )

    assert result.total_trace_count == 2
    assert [outcome.trace_id for outcome in result.outcomes] == ["trace-good"]
    assert result.outcomes[0].status is ImportOutcomeStatus.WOULD_CREATE


def test_actual_import_collects_per_trace_outcomes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def persist(trace, **kwargs):
        del kwargs
        if trace.source.trace_id == "trace-root-omitted":
            return ImportedExecutionResult(
                execution_id="execution-existing",
                created=False,
                resumed=False,
                observation_count=len(trace.observations),
            )
        if trace.source.trace_id == "trace-fragmented":
            raise ImportedTraceConflictError("synthetic conflict")
        return ImportedExecutionResult(
            execution_id="execution-created",
            created=True,
            resumed=False,
            observation_count=len(trace.observations),
        )

    monkeypatch.setattr("kitaru.imports._service.persist_imported_trace", persist)

    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        dry_run=False,
        confirm_data_storage=True,
        allow_fragmented=True,
        max_workers=2,
        client=_client(),
    )

    assert [outcome.status for outcome in result.outcomes] == [
        ImportOutcomeStatus.CREATED,
        ImportOutcomeStatus.UNCHANGED,
        ImportOutcomeStatus.CONFLICT,
    ]
    assert result.counts == {"conflict": 1, "created": 1, "unchanged": 1}


def test_conflict_outcome_preserves_existing_execution_and_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def conflict(*args, **kwargs):
        del args, kwargs
        raise ImportedTraceConflictError(
            "already imported with another label",
            existing_execution_id="execution-existing",
            resolution="Retry with the original agent name.",
        )

    monkeypatch.setattr("kitaru.imports._service.persist_imported_trace", conflict)

    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        trace_ids=["trace-complete"],
        dry_run=False,
        confirm_data_storage=True,
        client=_client(),
    )

    outcome = result.outcomes[0]
    assert outcome.status is ImportOutcomeStatus.CONFLICT
    assert outcome.existing_execution_id == "execution-existing"
    assert outcome.reason == "already imported with another label"
    assert outcome.resolution == "Retry with the original agent name."


def test_rejects_invalid_options() -> None:
    with pytest.raises(KitaruUsageError, match="limit"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent="support-agent",
            version="prod",
            limit=0,
            client=_client(),
        )
    for workers in (0, 9):
        with pytest.raises(KitaruUsageError, match="max_workers"):
            import_langfuse_jsonl(
                FIXTURE,
                source_project_id="source-project",
                agent="support-agent",
                version="prod",
                max_workers=workers,
                client=_client(),
            )
    with pytest.raises(KitaruUsageError, match="source_project_id"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id=" ",
            agent="support-agent",
            version="prod",
            client=_client(),
        )
    with pytest.raises(KitaruUsageError, match="agent"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent=" ",
            version="prod",
            client=_client(),
        )


def test_backend_failure_is_isolated_to_one_trace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def persist(trace, **kwargs):
        del kwargs
        if trace.source.trace_id == "trace-root-omitted":
            raise RuntimeError("synthetic backend timeout")
        return ImportedExecutionResult(
            execution_id="execution-created",
            created=True,
            resumed=False,
            observation_count=len(trace.observations),
        )

    monkeypatch.setattr("kitaru.imports._service.persist_imported_trace", persist)

    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        dry_run=False,
        confirm_data_storage=True,
        allow_fragmented=True,
        client=_client(),
    )

    outcomes = {outcome.trace_id: outcome for outcome in result.outcomes}
    failed = outcomes["trace-root-omitted"]
    assert failed.status is ImportOutcomeStatus.FAILED
    assert failed.reason == "synthetic backend timeout"
    assert failed.execution_id is None
    assert all(
        outcome.status is ImportOutcomeStatus.CREATED
        for trace_id, outcome in outcomes.items()
        if trace_id != "trace-root-omitted"
    )


def test_interrupted_write_reports_the_execution_it_left_behind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def persist(*args, **kwargs):
        del args, kwargs
        raise ImportedTraceWriteError(
            "synthetic mid-write failure",
            execution_id="execution-orphan",
        )

    monkeypatch.setattr("kitaru.imports._service.persist_imported_trace", persist)

    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        trace_ids=["trace-complete"],
        dry_run=False,
        confirm_data_storage=True,
        client=_client(),
    )

    outcome = result.outcomes[0]
    assert outcome.status is ImportOutcomeStatus.FAILED
    assert outcome.execution_id == "execution-orphan"
    assert "synthetic mid-write failure" in (outcome.reason or "")


def test_stack_must_contain_exactly_one_artifact_store() -> None:
    client = _client()
    client.active_stack_model.components["artifact_store"] = []

    with pytest.raises(KitaruUsageError, match="exactly one artifact store"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent="support-agent",
            version="prod",
            client=client,
        )


def test_blank_stack_name_is_rejected() -> None:
    with pytest.raises(KitaruUsageError, match="non-empty name or ID"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent="support-agent",
            version="prod",
            stack="   ",
            client=_client(),
        )


def test_unresolvable_stack_is_rejected() -> None:
    client = _client()
    client.get_stack.side_effect = RuntimeError("stack lookup exploded")

    with pytest.raises(KitaruUsageError, match="Could not resolve stack"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent="support-agent",
            version="prod",
            stack="missing-stack",
            client=client,
        )


def test_cohort_tag_is_validated_before_reading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    read = MagicMock(side_effect=AssertionError("invalid tag must fail before reading"))
    monkeypatch.setattr("kitaru.imports._service.read_langfuse_jsonl_records", read)

    with pytest.raises(KitaruUsageError, match="cohort_tag"):
        import_langfuse_jsonl(
            "unused.jsonl",
            source_project_id="source-project",
            agent="support-agent",
            version="prod",
            cohort_tag="contains spaces",
            client=_client(),
        )

    read.assert_not_called()


def test_stack_without_project_identity_fails_closed() -> None:
    client = _client()
    del client.get_stack.return_value.project_id

    with pytest.raises(KitaruUsageError, match="no verifiable Agent Project"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent="support-agent",
            version="prod",
            stack="unscoped-stack",
            client=client,
        )


def test_result_reports_actual_project_name_not_agent_name() -> None:
    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        trace_ids=["trace-complete"],
        client=_client(),
    )

    assert result.agent_name == "support-agent"
    assert result.project_name == "default"


def test_explicit_stack_rejects_cross_project_artifact_scope() -> None:
    client = _client()
    client.get_stack.return_value.project_id = UUID(int=99)

    with pytest.raises(KitaruUsageError, match="different Agent Project"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent="support-agent",
            version="prod",
            stack="other-project-stack",
            client=client,
        )


def test_binding_resolves_before_source_rows_are_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    client = _client()
    binding = RegisteredAgentVersionBinding(
        project_id=str(client.active_project.id),
        manifest=_manifest(),
        agent_name="support-agent",
        aliases=("prod",),
        requested_alias="prod",
    )
    monkeypatch.setattr(
        "kitaru.imports._service.resolve_registered_agent_version",
        lambda *_args, **_kwargs: events.append("binding") or binding,
    )
    from kitaru.imports._langfuse import (
        read_langfuse_jsonl_records as actual_reader,
    )

    def read(path):
        events.append("read")
        return actual_reader(path)

    monkeypatch.setattr("kitaru.imports._service.read_langfuse_jsonl_records", read)

    import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        trace_ids=["trace-complete"],
        client=client,
    )

    assert events == ["binding", "read"]


@pytest.mark.parametrize("dry_run", [True, False])
def test_source_version_conflict_rejects_submission_without_writes(
    dry_run: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = MagicMock(side_effect=AssertionError("conflict must not plan writes"))
    persist = MagicMock(side_effect=AssertionError("conflict must not persist"))
    monkeypatch.setattr("kitaru.imports._service.plan_imported_trace", plan)
    monkeypatch.setattr("kitaru.imports._service.persist_imported_trace", persist)

    result = import_langfuse_jsonl(
        EVIDENCE_FIXTURE,
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        trace_ids=["trace-alias", "trace-conflict", "trace-unstamped"],
        dry_run=dry_run,
        confirm_data_storage=not dry_run,
        client=_client(),
    )

    assert [outcome.trace_id for outcome in result.outcomes] == [
        "trace-alias",
        "trace-conflict",
        "trace-unstamped",
    ]
    assert [outcome.status for outcome in result.outcomes] == [
        ImportOutcomeStatus.REJECTED,
        ImportOutcomeStatus.CONFLICT,
        ImportOutcomeStatus.REJECTED,
    ]
    attributions = [outcome.attribution for outcome in result.outcomes]
    assert all(attribution is not None for attribution in attributions)
    assert [
        attribution.status for attribution in attributions if attribution is not None
    ] == [
        SourceAttributionStatus.SOURCE_VERIFIED,
        SourceAttributionStatus.CONFLICT,
        SourceAttributionStatus.CALLER_ATTRIBUTED,
    ]
    assert result.attribution_counts == {
        "caller_attributed": 1,
        "conflict": 1,
        "source_verified": 1,
    }
    plan.assert_not_called()
    persist.assert_not_called()


def test_failed_artifact_store_load_is_a_usage_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "kitaru.imports._service.StackComponent",
        SimpleNamespace(
            from_model=MagicMock(side_effect=RuntimeError("flavor not installed"))
        ),
    )
    component = SimpleNamespace(flavor_name="s3", name="artifacts")

    with pytest.raises(KitaruUsageError, match="Could not load artifact store"):
        _real_load_artifact_store(component)
