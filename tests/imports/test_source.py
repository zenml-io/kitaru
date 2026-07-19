"""Tests for shared Langfuse source resolution."""

import json
import tomllib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import kitaru.imports as public_imports
from kitaru.imports import (
    LangfuseImportError,
    LangfuseSourceKind,
    resolve_langfuse_source,
)
from kitaru.imports import _source as source_module

FIXTURE = Path(__file__).parent / "fixtures" / "langfuse_observations.jsonl"


def _row(
    observation_id: str,
    *,
    trace_id: str = "trace-one",
    project_id: str = "project-one",
    start_time: str = "2026-07-19T10:00:00Z",
    input_value: object = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "id": observation_id,
        "traceId": trace_id,
        "projectId": project_id,
        "type": "SPAN",
        "startTime": start_time,
        "endTime": "2026-07-19T10:00:01Z",
    }
    if input_value is not None:
        row["input"] = input_value
    return row


class _SDKObservation:
    def __init__(self, row: dict[str, object]) -> None:
        self._row = row

    def model_dump(
        self,
        *,
        mode: str,
        by_alias: bool,
        exclude_none: bool,
    ) -> dict[str, object]:
        assert (mode, by_alias, exclude_none) == ("json", True, True)
        return self._row


def _install_fake_client(
    monkeypatch: pytest.MonkeyPatch,
    pages: dict[str | None, tuple[list[object], str | None] | Exception],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    requests: list[dict[str, object]] = []
    client_options: list[dict[str, object]] = []

    class Observations:
        def get_many(self, **kwargs):
            requests.append(kwargs)
            page = pages[kwargs["cursor"]]
            if isinstance(page, Exception):
                raise page
            data, cursor = page
            return SimpleNamespace(
                data=data,
                meta=SimpleNamespace(cursor=cursor),
            )

    class FakeLangfuse:
        def __init__(self, **kwargs):
            client_options.append(kwargs)
            self.api = SimpleNamespace(observations=Observations())

    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    monkeypatch.setattr(
        source_module,
        "_load_langfuse_client_type",
        lambda: FakeLangfuse,
    )
    return requests, client_options


def test_jsonl_resolution_preserves_exact_rows_and_requires_project_id() -> None:
    resolved = resolve_langfuse_source(
        FIXTURE,
        source_project_id="source-project",
    )

    assert resolved.kind is LangfuseSourceKind.JSONL
    assert resolved.authoritative_project_id == "source-project"
    assert resolved.selected_trace_id is None
    assert resolved.fetch_provenance is None
    fixture_lines = FIXTURE.read_text(encoding="utf-8").splitlines(keepends=True)
    assert next(iter(resolved.records)).raw_text == fixture_lines[0]

    with pytest.raises(LangfuseImportError, match=r"required.*JSONL"):
        resolve_langfuse_source(FIXTURE)


@pytest.mark.parametrize(
    "source",
    [
        "https://example.com/trace/trace-one",
        "langfuse://project/trace-one",
        "langfuse://trace/",
        "langfuse://trace/one/two",
        "langfuse://trace/one%2Ftwo",
        "langfuse://trace@evil/trace-one",
        "langfuse://trace/ trace-one",
        "langfuse://trace/trace-one?limit=1",
        "langfuse://trace/trace-one#fragment",
        "langfuse://trace/%20",
    ],
)
def test_malformed_uris_are_rejected_before_sdk_loading(
    source: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = MagicMock(side_effect=AssertionError("must not load SDK"))
    monkeypatch.setattr(source_module, "_load_langfuse_client_type", loader)

    with pytest.raises(LangfuseImportError, match=r"Langfuse.*URI|scheme"):
        resolve_langfuse_source(source)

    loader.assert_not_called()


def test_uri_trace_selection_must_match_before_network_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch = MagicMock(side_effect=AssertionError("must not fetch"))
    monkeypatch.setattr(source_module, "_fetch_langfuse_trace", fetch)

    with pytest.raises(LangfuseImportError, match="selects trace"):
        resolve_langfuse_source(
            "langfuse://trace/trace-one",
            trace_ids=["trace-two"],
        )

    fetch.assert_not_called()


def test_jsonl_resolution_is_lazy(monkeypatch: pytest.MonkeyPatch) -> None:
    consumed = False

    def records(path: str | Path):
        nonlocal consumed
        del path
        consumed = True
        yield from ()

    monkeypatch.setattr(source_module, "read_langfuse_jsonl_records", records)

    resolved = resolve_langfuse_source(
        "export.jsonl",
        source_project_id="source-project",
    )

    assert consumed is False
    assert list(resolved.records) == []
    assert consumed is True


def test_jsonl_resolution_never_loads_optional_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = MagicMock(side_effect=AssertionError("JSONL must not load SDK"))
    monkeypatch.setattr(source_module, "_load_langfuse_client_type", loader)

    resolved = resolve_langfuse_source(
        FIXTURE,
        source_project_id="source-project",
    )

    assert resolved.kind is LangfuseSourceKind.JSONL
    loader.assert_not_called()


def test_uri_resolution_requires_credentials_before_loading_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = MagicMock(side_effect=AssertionError("must not load SDK"))
    monkeypatch.setattr(source_module, "_load_langfuse_client_type", loader)
    monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")

    with pytest.raises(LangfuseImportError, match="LANGFUSE_PUBLIC_KEY"):
        resolve_langfuse_source("langfuse://trace/trace-one")

    loader.assert_not_called()


def test_base_url_and_legacy_host_must_not_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    monkeypatch.setenv("LANGFUSE_BASE_URL", "https://base.example")
    monkeypatch.setenv("LANGFUSE_HOST", "https://host.example")
    loader = MagicMock(side_effect=AssertionError("must not load SDK"))
    monkeypatch.setattr(source_module, "_load_langfuse_client_type", loader)

    with pytest.raises(LangfuseImportError, match="different values"):
        resolve_langfuse_source("langfuse://trace/trace-one")

    loader.assert_not_called()


def test_current_base_url_variable_takes_precedence_when_values_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LANGFUSE_BASE_URL", "https://langfuse.example/")
    monkeypatch.setenv("LANGFUSE_HOST", "https://langfuse.example")

    assert source_module._langfuse_base_url() == (
        "https://langfuse.example",
        "LANGFUSE_BASE_URL",
    )


@pytest.mark.parametrize(
    "base_url",
    [
        "ftp://langfuse.example",
        "https://user:password@langfuse.example",
        "https://langfuse.example?token=secret",
        "https://langfuse.example#fragment",
    ],
)
def test_base_url_provenance_rejects_secret_or_unsupported_shapes(
    base_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    monkeypatch.setenv("LANGFUSE_BASE_URL", base_url)
    loader = MagicMock(side_effect=AssertionError("must not load SDK"))
    monkeypatch.setattr(source_module, "_load_langfuse_client_type", loader)

    with pytest.raises(LangfuseImportError, match=r"HTTP\(S\).*without credentials"):
        resolve_langfuse_source("langfuse://trace/trace-one")

    loader.assert_not_called()


def test_uri_fetch_paginates_and_canonicalizes_records_deterministically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    later = _row(
        "observation-b",
        start_time="2026-07-19T10:00:01Z",
        input_value={"z": 1, "a": 2},
    )
    earlier = _row(
        "observation-a",
        start_time="2026-07-19T10:00:00Z",
    )
    requests, client_options = _install_fake_client(
        monkeypatch,
        {
            None: ([_SDKObservation(later)], "next-page"),
            "next-page": ([earlier], None),
        },
    )
    monkeypatch.setenv("LANGFUSE_HOST", "https://self-hosted.example/")
    monkeypatch.delenv("LANGFUSE_BASE_URL", raising=False)

    first = resolve_langfuse_source("langfuse://trace/trace-one")
    second = resolve_langfuse_source("langfuse://trace/trace-one")

    assert first == second
    assert first.kind is LangfuseSourceKind.TRACE_URI
    assert first.authoritative_project_id == "project-one"
    assert first.selected_trace_id == "trace-one"
    assert [record.row["id"] for record in first.records] == [
        "observation-a",
        "observation-b",
    ]
    assert [record.source_order for record in first.records] == [0, 1]
    assert [record.line_number for record in first.records] == [1, 2]
    assert list(first.records)[1].raw_text == (
        json.dumps(
            later,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n"
    )
    assert first.fetch_provenance is not None
    assert first.fetch_provenance.api_resource == "observations_v2"
    assert first.fetch_provenance.base_url == "https://self-hosted.example"
    assert first.fetch_provenance.page_count == 2
    assert first.fetch_provenance.base_url_source == "LANGFUSE_HOST"
    assert first.fetch_provenance.field_groups == (
        "core",
        "basic",
        "time",
        "io",
        "metadata",
        "model",
        "usage",
        "prompt",
        "metrics",
        "trace_context",
    )
    assert [request["cursor"] for request in requests] == [
        None,
        "next-page",
        None,
        "next-page",
    ]
    assert all(request["trace_id"] == "trace-one" for request in requests)
    assert all(
        request["fields"]
        == "core,basic,time,io,metadata,model,usage,prompt,metrics,trace_context"
        for request in requests
    )
    assert all(
        request["limit"] == source_module._OBSERVATION_PAGE_LIMIT
        for request in requests
    )
    assert all(request["parse_io_as_json"] is False for request in requests)
    assert client_options == [
        {
            "public_key": "pk-test",
            "secret_key": "sk-test",
            "base_url": "https://self-hosted.example",
            "tracing_enabled": False,
        },
        {
            "public_key": "pk-test",
            "secret_key": "sk-test",
            "base_url": "https://self-hosted.example",
            "tracing_enabled": False,
        },
    ]


@pytest.mark.parametrize(
    ("pages", "source_project_id", "message"),
    [
        ({None: ([], None)}, None, "no observations"),
        (
            {None: ([_row("a", trace_id="other")], None)},
            None,
            "different or mixed trace",
        ),
        (
            {
                None: (
                    [
                        _row("a", project_id="project-one"),
                        _row("b", project_id="project-two"),
                    ],
                    None,
                )
            },
            None,
            "mixed project IDs",
        ),
        (
            {None: ([_row("a")], None)},
            "declared-project",
            "does not match",
        ),
        (
            {None: RuntimeError("provider failure")},
            None,
            "request failed",
        ),
    ],
)
def test_uri_fetch_rejects_incomplete_or_inconsistent_results(
    pages: dict[str | None, tuple[list[object], str | None] | Exception],
    source_project_id: str | None,
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_client(monkeypatch, pages)

    with pytest.raises(LangfuseImportError, match=message):
        resolve_langfuse_source(
            "langfuse://trace/trace-one",
            source_project_id=source_project_id,
        )


def test_canonical_api_rows_ignore_mapping_key_order() -> None:
    assert source_module._canonical_row({"b": 2, "a": 1}) == (
        source_module._canonical_row({"a": 1, "b": 2})
    )


def test_uri_fetch_rejects_duplicate_observations_across_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    duplicate = _row("observation-a")
    _install_fake_client(
        monkeypatch,
        {
            None: ([duplicate], "next"),
            "next": ([dict(reversed(list(duplicate.items())))], None),
        },
    )

    with pytest.raises(LangfuseImportError, match="duplicate observation IDs"):
        resolve_langfuse_source("langfuse://trace/trace-one")


def test_uri_fetch_bounds_page_and_observation_accumulation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(source_module, "_MAX_OBSERVATION_PAGES", 1)
    requests, _options = _install_fake_client(
        monkeypatch,
        {
            None: ([_row("a")], "next"),
            "next": ([_row("b")], None),
        },
    )

    with pytest.raises(LangfuseImportError, match="page limit"):
        resolve_langfuse_source("langfuse://trace/trace-one")

    assert len(requests) == 1

    monkeypatch.setattr(source_module, "_MAX_OBSERVATION_PAGES", 100)
    monkeypatch.setattr(source_module, "_MAX_TRACE_OBSERVATIONS", 1)
    _install_fake_client(
        monkeypatch,
        {None: ([_row("a"), _row("b")], None)},
    )

    with pytest.raises(LangfuseImportError, match="observation limit"):
        resolve_langfuse_source("langfuse://trace/trace-one")


def test_uri_fetch_rejects_per_observation_and_cumulative_byte_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(source_module, "_MAX_OBSERVATION_CANONICAL_BYTES", 64)
    _install_fake_client(
        monkeypatch,
        {None: ([_row("oversized", input_value={"payload": "x" * 100})], None)},
    )

    with pytest.raises(LangfuseImportError, match="per-observation canonical-byte"):
        resolve_langfuse_source("langfuse://trace/trace-one")

    monkeypatch.setattr(source_module, "_MAX_OBSERVATION_CANONICAL_BYTES", 10_000)
    first = _row("a")
    second = _row("b")
    first_size = len(source_module._canonical_row(first).encode("utf-8"))
    monkeypatch.setattr(
        source_module,
        "_MAX_TRACE_CANONICAL_BYTES",
        first_size,
    )
    _install_fake_client(
        monkeypatch,
        {None: ([first, second], None)},
    )

    with pytest.raises(LangfuseImportError, match="cumulative canonical-byte"):
        resolve_langfuse_source("langfuse://trace/trace-one")


def test_uri_fetch_canonicalizes_each_retained_observation_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows: list[object] = [_row("a"), _row("b")]
    _install_fake_client(monkeypatch, {None: (rows, None)})
    original = source_module._canonical_row
    calls: list[str] = []

    def canonical(row: dict[str, object]) -> str:
        calls.append(str(row["id"]))
        return original(row)

    monkeypatch.setattr(source_module, "_canonical_row", canonical)

    resolved = resolve_langfuse_source("langfuse://trace/trace-one")

    assert len(list(resolved.records)) == 2
    assert calls == ["a", "b"]


def test_uri_fetch_rejects_repeated_pagination_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_client(
        monkeypatch,
        {
            None: ([_row("a")], "repeated"),
            "repeated": ([_row("b")], "repeated"),
        },
    )

    with pytest.raises(LangfuseImportError, match="repeated cursor"):
        resolve_langfuse_source("langfuse://trace/trace-one")


def test_optional_dependency_contract_accepts_minimum_supported_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client_type = type("FakeLangfuse", (), {})
    monkeypatch.setattr(
        source_module,
        "import_module",
        lambda name: SimpleNamespace(Langfuse=client_type),
    )
    monkeypatch.setattr(source_module, "version", lambda name: "4.7.0")

    assert source_module._load_langfuse_client_type() is client_type


@pytest.mark.parametrize(
    "installed_version",
    ["4.6.9", "5.0.0", "invalid", "4.7.0garbage", "4.7.0rc", "4.7.0.1"],
)
def test_optional_dependency_contract_rejects_unsupported_versions(
    installed_version: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        source_module,
        "import_module",
        lambda name: SimpleNamespace(Langfuse=object),
    )
    monkeypatch.setattr(source_module, "version", lambda name: installed_version)

    with pytest.raises(LangfuseImportError, match=r"langfuse>=4\.7\.0,<5"):
        source_module._load_langfuse_client_type()


def test_missing_optional_dependency_has_installation_guidance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing(name: str) -> object:
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(source_module, "import_module", missing)

    with pytest.raises(LangfuseImportError, match=r"kitaru\[langfuse\]"):
        source_module._load_langfuse_client_type()


def test_project_dependency_pins_supported_langfuse_major() -> None:
    pyproject = tomllib.loads(
        (Path(__file__).parents[2] / "pyproject.toml").read_text(encoding="utf-8")
    )

    assert pyproject["project"]["optional-dependencies"]["langfuse"] == [
        "langfuse>=4.7.0,<5"
    ]
    assert "langfuse>=4.7.0,<5" in pyproject["dependency-groups"]["dev"]


def test_source_resolution_contract_is_publicly_exported() -> None:
    expected = {
        "LangfuseFetchProvenance",
        "LangfuseSourceKind",
        "ResolvedLangfuseSource",
        "import_langfuse",
        "resolve_langfuse_source",
    }

    assert expected <= set(public_imports.__all__)
    assert all(hasattr(public_imports, name) for name in expected)
