#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for OpenTelemetry instrumentation."""

import logging
import os
from collections.abc import Generator, Sequence

import pytest

pytest.importorskip("opentelemetry")

from fastapi import FastAPI
from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk._logs.export import LogRecordExporter, LogRecordExportResult
from opentelemetry.sdk.metrics.export import (
    MetricExporter,
    MetricExportResult,
    MetricsData,
)
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

from conftest import local_settings
from kitaru.server.api import otel
from kitaru.server.api.config import APISettings


@pytest.fixture(autouse=True)
def clear_otel_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear every OTEL environment variable before each test."""
    for name in list(os.environ):
        if "OTEL" in name:
            monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def reset_otel_state() -> Generator[None, None, None]:
    """Shut down OpenTelemetry after each test so module state never leaks."""
    yield
    otel.shutdown_otel()


class _FakeSpanExporter(SpanExporter):
    """Span exporter that never touches the network."""

    def __init__(self, endpoint: str | None = None) -> None:
        """Record the endpoint it was configured with."""
        self.endpoint = endpoint

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        """Report success without sending anything."""
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        """Do nothing."""


class _FakeMetricExporter(MetricExporter):
    """Metric exporter that never touches the network."""

    def __init__(self, endpoint: str | None = None) -> None:
        """Record the endpoint it was configured with."""
        super().__init__()
        self.endpoint = endpoint

    def export(
        self,
        metrics_data: MetricsData,
        timeout_millis: float = 10_000,
        **kwargs: object,
    ) -> MetricExportResult:
        """Report success without sending anything."""
        return MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        """Report success without sending anything."""
        return True

    def shutdown(self, timeout_millis: float = 30_000, **kwargs: object) -> None:
        """Do nothing."""


class _FakeLogExporter(LogRecordExporter):
    """Log exporter that never touches the network."""

    def __init__(self, endpoint: str | None = None) -> None:
        """Record the endpoint it was configured with."""
        self.endpoint = endpoint

    def export(self, batch: Sequence[ReadableLogRecord]) -> LogRecordExportResult:
        """Report success without sending anything."""
        return LogRecordExportResult.SUCCESS

    def force_flush(self, timeout_millis: int = 10_000) -> bool:
        """Report success without sending anything."""
        return True

    def shutdown(self) -> None:
        """Do nothing."""


@pytest.fixture
def fake_exporters(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the OTLP exporter classes with network-free fakes."""
    monkeypatch.setattr(
        "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter",
        _FakeSpanExporter,
    )
    monkeypatch.setattr(
        "opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter",
        _FakeMetricExporter,
    )
    monkeypatch.setattr(
        "opentelemetry.exporter.otlp.proto.http._log_exporter.OTLPLogExporter",
        _FakeLogExporter,
    )


def _settings_with_endpoint() -> APISettings:
    return local_settings(OTEL_EXPORTER_OTLP_ENDPOINT="http://collector:4318")


def test_signal_endpoint_derived_from_base_endpoint() -> None:
    """Append the signal path to the base OTLP endpoint."""
    settings = _settings_with_endpoint()
    assert (
        otel._get_signal_endpoint(settings, True, None, "v1/traces")
        == "http://collector:4318/v1/traces"
    )


def test_signal_endpoint_handles_trailing_slash_on_base_endpoint() -> None:
    """Drop the base endpoint's trailing slash before appending the signal path."""
    settings = local_settings(OTEL_EXPORTER_OTLP_ENDPOINT="http://collector:4318/")
    assert (
        otel._get_signal_endpoint(settings, True, None, "v1/traces")
        == "http://collector:4318/v1/traces"
    )


def test_signal_endpoint_per_signal_override_wins() -> None:
    """Prefer the per-signal endpoint over one derived from the base endpoint."""
    settings = _settings_with_endpoint()
    assert (
        otel._get_signal_endpoint(
            settings, True, "http://traces-only:4318/v1/traces", "v1/traces"
        )
        == "http://traces-only:4318/v1/traces"
    )


def test_signal_endpoint_disabled_signal_resolves_to_none() -> None:
    """Resolve to no endpoint when the signal is disabled."""
    settings = _settings_with_endpoint()
    assert otel._get_signal_endpoint(settings, False, None, "v1/traces") is None


def test_signal_endpoint_no_endpoint_configured_resolves_to_none() -> None:
    """Resolve to no endpoint when neither the base nor the signal endpoint is set."""
    settings = local_settings()
    assert otel._get_signal_endpoint(settings, True, None, "v1/traces") is None


def test_otlp_endpoint_standard_env_var_is_honored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Read the base endpoint from the standard OTEL_EXPORTER_OTLP_ENDPOINT."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://standard:4318")
    settings = local_settings()
    assert settings.OTEL_EXPORTER_OTLP_ENDPOINT == "http://standard:4318"


def test_otlp_endpoint_prefixed_env_var_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prefer the KITARU_SERVER_-prefixed endpoint over the standard one."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://standard:4318")
    monkeypatch.setenv(
        "KITARU_SERVER_OTEL_EXPORTER_OTLP_ENDPOINT", "http://prefixed:4318"
    )
    settings = local_settings()
    assert settings.OTEL_EXPORTER_OTLP_ENDPOINT == "http://prefixed:4318"


def test_service_name_standard_env_var_is_honored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Read the service name from the standard OTEL_SERVICE_NAME."""
    monkeypatch.setenv("OTEL_SERVICE_NAME", "standard-name")
    settings = local_settings()
    assert settings.OTEL_SERVICE_NAME == "standard-name"


def test_service_name_prefixed_env_var_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prefer the KITARU_SERVER_-prefixed service name over the standard one."""
    monkeypatch.setenv("OTEL_SERVICE_NAME", "standard-name")
    monkeypatch.setenv("KITARU_SERVER_OTEL_SERVICE_NAME", "prefixed-name")
    settings = local_settings()
    assert settings.OTEL_SERVICE_NAME == "prefixed-name"


def test_configure_otel_without_endpoint_is_a_no_op() -> None:
    """Leave instrumentation untouched when no endpoint is configured."""
    otel.configure_otel(local_settings(), FastAPI())
    assert otel._configured is False
    assert otel._log_handler is None


def test_configure_otel_activates_instrumentation(fake_exporters: None) -> None:
    """Mark instrumentation active and attach the log handler to the root logger."""
    otel.configure_otel(_settings_with_endpoint(), FastAPI())

    assert otel._configured is True
    assert otel._log_handler is not None
    assert otel._log_handler in logging.getLogger().handlers


def test_configure_otel_is_idempotent(fake_exporters: None) -> None:
    """Configure instrumentation only once across repeated calls."""
    settings = _settings_with_endpoint()
    otel.configure_otel(settings, FastAPI())
    handler = otel._log_handler

    otel.configure_otel(settings, FastAPI())

    assert otel._log_handler is handler


def test_shutdown_otel_removes_handler_and_resets_state(fake_exporters: None) -> None:
    """Remove the log handler and reset every module global."""
    otel.configure_otel(_settings_with_endpoint(), FastAPI())
    handler = otel._log_handler
    assert handler is not None

    otel.shutdown_otel()

    assert otel._configured is False
    assert otel._log_handler is None
    assert handler not in logging.getLogger().handlers


def test_shutdown_otel_is_idempotent(fake_exporters: None) -> None:
    """Shut down cleanly a second time with nothing left to tear down."""
    otel.configure_otel(_settings_with_endpoint(), FastAPI())

    otel.shutdown_otel()
    otel.shutdown_otel()

    assert otel._configured is False
