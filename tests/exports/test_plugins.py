#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Installed exporter discovery contracts."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from kitaru.exports.models import ExportError, ExportManifest
from kitaru.exports.plugin import (
    EXPORTER_ENTRY_POINT_GROUP,
    ExporterMetadata,
    ExporterOptions,
    resolve_exporter,
)


class FakeExporter:
    """Provide the smallest valid exporter implementation for discovery tests."""

    def __init__(self, metadata: ExporterMetadata) -> None:
        self.metadata = metadata

    def preflight(self, *_args: Any, **_kwargs: Any) -> None:
        pass

    def render(self, *_args: Any, **_kwargs: Any) -> ExportManifest:
        raise AssertionError("render is not used by discovery tests")


@dataclass(frozen=True)
class FakeDistribution:
    name: str
    version: str


class FakeEntryPoint:
    def __init__(
        self,
        *,
        group: str = EXPORTER_ENTRY_POINT_GROUP,
        name: str = "harbor",
        distribution_name: str = "kitaru-harbor-exporter",
        distribution_version: str = "1.2.3",
        loaded: Any = None,
        load_error: BaseException | None = None,
    ) -> None:
        self.group = group
        self.name = name
        self.value = "fake:exporter"
        self.dist = FakeDistribution(distribution_name, distribution_version)
        self._loaded = loaded
        self._load_error = load_error
        self.load_count = 0

    def load(self) -> Any:
        self.load_count += 1
        if self._load_error is not None:
            raise self._load_error
        return self._loaded


def _metadata(**changes: Any) -> ExporterMetadata:
    values = {
        "contract_version": 1,
        "distribution_name": "kitaru-harbor-exporter",
        "distribution_version": "1.2.3",
        "format": "harbor",
        "target_version": "0.20.0",
    }
    values.update(changes)
    return ExporterMetadata(**values)


def _entry_point(**changes: Any) -> FakeEntryPoint:
    metadata = changes.pop("metadata", _metadata())
    return FakeEntryPoint(loaded=lambda: FakeExporter(metadata), **changes)


def test_resolve_exporter_loads_only_the_selected_provider() -> None:
    selected = _entry_point()
    other = _entry_point(
        name="verifiers-v1",
        distribution_name="kitaru-verifiers-exporter",
        distribution_version="4.5.6",
        metadata=_metadata(
            distribution_name="kitaru-verifiers-exporter",
            distribution_version="4.5.6",
            format="verifiers-v1",
            target_version="0.3.0",
        ),
    )

    exporter = resolve_exporter("harbor", entry_points=(other, selected))

    assert exporter.metadata == _metadata()
    assert selected.load_count == 1
    assert other.load_count == 0


def test_resolve_exporter_reports_missing_package() -> None:
    with pytest.raises(ExportError) as raised:
        resolve_exporter("harbor", entry_points=())

    assert raised.value.code == "exporter_not_installed"
    assert "kitaru-harbor-exporter" in raised.value.message
    assert "uv add kitaru-harbor-exporter" in raised.value.message


def test_resolve_exporter_reports_other_contract_group_as_incompatible() -> None:
    provider = _entry_point(group="kitaru.exporters.v2")

    with pytest.raises(ExportError) as raised:
        resolve_exporter("harbor", entry_points=(provider,))

    assert raised.value.code == "exporter_incompatible"
    assert "kitaru.exporters.v2" in raised.value.message
    assert provider.load_count == 0


def test_resolve_exporter_rejects_ambiguous_providers_without_loading() -> None:
    first = _entry_point()
    second = _entry_point(
        distribution_name="other-harbor-exporter",
        distribution_version="9.0.0",
        metadata=_metadata(
            distribution_name="other-harbor-exporter",
            distribution_version="9.0.0",
        ),
    )

    with pytest.raises(ExportError) as raised:
        resolve_exporter("harbor", entry_points=(second, first))

    assert raised.value.code == "exporter_ambiguous"
    assert "kitaru-harbor-exporter==1.2.3" in raised.value.message
    assert "other-harbor-exporter==9.0.0" in raised.value.message
    assert first.load_count == second.load_count == 0


@pytest.mark.parametrize(
    ("loaded", "load_error"),
    [
        (object(), None),
        (lambda: object(), None),
        (None, RuntimeError("private import detail")),
    ],
)
def test_resolve_exporter_maps_malformed_or_failing_loads(
    loaded: Any, load_error: BaseException | None
) -> None:
    provider = FakeEntryPoint(loaded=loaded, load_error=load_error)

    with pytest.raises(ExportError) as raised:
        resolve_exporter("harbor", entry_points=(provider,))

    assert raised.value.code == "exporter_load_failed"
    assert "private import detail" not in raised.value.message


@pytest.mark.parametrize(
    "metadata",
    [
        _metadata(contract_version=2),
        _metadata(distribution_name="counterfeit-exporter"),
        _metadata(distribution_version="99.0.0"),
        _metadata(format="verifiers-v1"),
    ],
)
def test_resolve_exporter_rejects_incompatible_or_false_metadata(
    metadata: ExporterMetadata,
) -> None:
    provider = _entry_point(metadata=metadata)

    with pytest.raises(ExportError) as raised:
        resolve_exporter("harbor", entry_points=(provider,))

    assert raised.value.code == "exporter_incompatible"


def test_exporter_options_are_strict_and_target_neutral() -> None:
    options = ExporterOptions(
        required_environment_names=("TOKEN",),
        trace_format="atif",
        trace_path="/logs/trajectory.json",
    )

    assert options.required_environment_names == ("TOKEN",)
    assert "destination" not in ExporterOptions.model_fields
    assert "client" not in ExporterOptions.model_fields
    assert "source_root" not in ExporterOptions.model_fields
    assert Path not in options.__class__.__annotations__.values()


def test_malicious_distribution_metadata_cannot_create_unbounded_errors() -> None:
    provider = _entry_point(distribution_version="secret\n" + "x" * 10_000)

    with pytest.raises(ExportError) as raised:
        resolve_exporter("harbor", entry_points=(provider,))

    assert raised.value.code == "exporter_load_failed"
    assert "secret" not in raised.value.message
    assert len(raised.value.message) < 256
