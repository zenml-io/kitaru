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
"""OpenTelemetry instrumentation for the API server."""

import logging
import os
from collections.abc import Callable
from importlib.metadata import version
from typing import Any

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine

from kitaru.server.api.config import APISettings

logger = logging.getLogger(__name__)

_configured = False
_log_handler: logging.Handler | None = None
_providers: list[Any] = []
_uninstrument_callbacks: list[Callable[[], None]] = []


def _get_signal_endpoint(
    settings: APISettings, enabled: bool, endpoint: str | None, signal_path: str
) -> str | None:
    """Resolve the OTLP endpoint for one telemetry signal.

    Args:
        settings: API server settings.
        enabled: Whether the signal is enabled.
        endpoint: Per-signal endpoint override.
        signal_path: Path segment appended to the base endpoint.

    Returns:
        Resolved endpoint, or ``None`` when the signal has none.
    """
    if not enabled:
        return None
    if endpoint:
        return endpoint
    if settings.OTEL_EXPORTER_OTLP_ENDPOINT:
        return f"{settings.OTEL_EXPORTER_OTLP_ENDPOINT.rstrip('/')}/{signal_path}"
    return None


def _get_resource_attributes(settings: APISettings) -> dict[str, str]:
    """Build the OpenTelemetry resource attributes for this process.

    Args:
        settings: API server settings.

    Returns:
        Resource attributes keyed by their OpenTelemetry semantic name.
    """
    attributes = {
        "service.name": settings.OTEL_SERVICE_NAME,
        "service.version": version("kitaru"),
    }
    instance_id = os.environ.get("HOSTNAME")
    if instance_id:
        attributes["service.instance.id"] = instance_id
    return attributes


def _configure_traces(resource: Any, endpoint: str) -> bool:
    """Configure the global tracer provider and register it.

    Args:
        resource: OpenTelemetry resource describing this process.
        endpoint: OTLP traces endpoint.

    Returns:
        ``True`` when the tracer provider was configured.
    """
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        provider = TracerProvider(resource=resource)
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
        )
        trace.set_tracer_provider(provider)
        _providers.append(provider)
        return True
    except Exception:
        logger.exception("Failed to configure OpenTelemetry traces.")
        return False


def _configure_metrics(resource: Any, endpoint: str) -> bool:
    """Configure the global meter provider and register it.

    Args:
        resource: OpenTelemetry resource describing this process.
        endpoint: OTLP metrics endpoint.

    Returns:
        ``True`` when the meter provider was configured.
    """
    try:
        from opentelemetry import metrics
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter,
        )
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

        reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=endpoint))
        provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(provider)
        _providers.append(provider)
        return True
    except Exception:
        logger.exception("Failed to configure OpenTelemetry metrics.")
        return False


def _configure_logs(resource: Any, endpoint: str, settings: APISettings) -> bool:
    """Configure the global logger provider and attach a root log handler.

    Args:
        resource: OpenTelemetry resource describing this process.
        endpoint: OTLP logs endpoint.
        settings: API server settings.

    Returns:
        ``True`` when the logger provider was configured.
    """
    global _log_handler
    try:
        from opentelemetry._logs import set_logger_provider
        from opentelemetry.exporter.otlp.proto.http._log_exporter import (
            OTLPLogExporter,
        )
        from opentelemetry.instrumentation.logging.handler import LoggingHandler
        from opentelemetry.sdk._logs import LoggerProvider
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

        provider = LoggerProvider(resource=resource)
        provider.add_log_record_processor(
            BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint))
        )
        set_logger_provider(provider)
        _providers.append(provider)
        handler = LoggingHandler(
            level=logging.getLevelName(settings.LOG_LEVEL), logger_provider=provider
        )
        logging.getLogger().addHandler(handler)
        _log_handler = handler
        # uvicorn's loggers don't propagate by default, which would keep their
        # records out of the root logger's OTel handler.
        logging.getLogger("uvicorn").propagate = True
        logging.getLogger("uvicorn.access").propagate = True
        return True
    except Exception:
        logger.exception("Failed to configure OpenTelemetry logs.")
        return False


def configure_otel(settings: APISettings, app: FastAPI) -> None:
    """Configure OpenTelemetry tracing, metrics, and logging for the app.

    Args:
        settings: API server settings.
        app: FastAPI application to instrument.
    """
    global _configured
    if _configured:
        return

    traces_endpoint = _get_signal_endpoint(
        settings,
        settings.OTEL_TRACES_ENABLED,
        settings.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
        "v1/traces",
    )
    metrics_endpoint = _get_signal_endpoint(
        settings,
        settings.OTEL_METRICS_ENABLED,
        settings.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT,
        "v1/metrics",
    )
    logs_endpoint = _get_signal_endpoint(
        settings,
        settings.OTEL_LOGS_ENABLED,
        settings.OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
        "v1/logs",
    )
    if not traces_endpoint and not metrics_endpoint and not logs_endpoint:
        return

    try:
        from opentelemetry.sdk.resources import Resource
    except ImportError:
        logger.warning(
            "An OTLP endpoint is configured but the otel extra is not installed."
        )
        return

    resource = Resource.create(_get_resource_attributes(settings))

    # Metrics must be configured before the FastAPI instrumentation below so
    # its http.server.* metrics pick up the global meter provider.
    configured = False
    if traces_endpoint:
        configured = _configure_traces(resource, traces_endpoint) or configured
    if metrics_endpoint:
        configured = _configure_metrics(resource, metrics_endpoint) or configured
    if logs_endpoint:
        configured = _configure_logs(resource, logs_endpoint, settings) or configured

    if not configured:
        return

    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app, exclude_spans=["send", "receive"])
        _uninstrument_callbacks.append(
            lambda: FastAPIInstrumentor.uninstrument_app(app)
        )
    except ImportError:
        logger.debug("opentelemetry-instrumentation-fastapi is not installed.")

    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        instrumentor = HTTPXClientInstrumentor()
        instrumentor.instrument()
        _uninstrument_callbacks.append(instrumentor.uninstrument)
    except ImportError:
        logger.debug("opentelemetry-instrumentation-httpx is not installed.")

    _configured = True
    logger.info("OpenTelemetry instrumentation is enabled.")


def instrument_engine(engine: AsyncEngine) -> None:
    """Instrument a database engine for OpenTelemetry tracing.

    Args:
        engine: Async database engine to instrument.
    """
    if not _configured:
        return
    try:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

        instrumentor = SQLAlchemyInstrumentor()
        # Async engines route their queries through the underlying sync
        # engine, which is what the instrumentation hooks into.
        instrumentor.instrument(engine=engine.sync_engine)
        _uninstrument_callbacks.append(instrumentor.uninstrument)
    except ImportError:
        logger.debug("opentelemetry-instrumentation-sqlalchemy is not installed.")
    except Exception:
        logger.exception("Failed to instrument the database engine.")


def shutdown_otel() -> None:
    """Undo instrumentation and shut down every configured provider."""
    global _configured, _log_handler

    for callback in reversed(_uninstrument_callbacks):
        try:
            callback()
        except Exception:
            logger.exception("Failed to undo OpenTelemetry instrumentation.")
    _uninstrument_callbacks.clear()

    if _log_handler is not None:
        logging.getLogger().removeHandler(_log_handler)
        _log_handler.close()
        _log_handler = None

    for provider in reversed(_providers):
        try:
            provider.shutdown()
        except Exception:
            logger.exception("Failed to shut down an OpenTelemetry provider.")
    _providers.clear()

    _configured = False
