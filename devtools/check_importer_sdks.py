"""Smoke-test Kitaru trace importer SDKs against local servers or live APIs.

Run from the plugins workspace environment so every importer package and its
locked SDK are installed:

    uv run --project plugins --no-sync python devtools/check_importer_sdks.py

Provider coverage:
- ``mlflow``: starts a throwaway local MLflow tracking server backed by SQLite
  via ``uvx``, logs a trace with LLM and tool spans, fetches it through
  ``kitaru_mlflow_importer.api.fetch``, and parses it into an imported session.
- ``phoenix``: starts a throwaway local Arize Phoenix Docker container
  (``PHOENIX_ENABLE_AUTH=false``), exports an OTLP trace with OpenInference LLM
  and tool spans, fetches it through ``kitaru_phoenix_importer.api``, and
  parses it into an imported session.
- ``langfuse``: runs a read-only ``observations.get_many`` and ``api.fetch``
  probe when ``LANGFUSE_PUBLIC_KEY`` and ``LANGFUSE_SECRET_KEY`` are set;
  otherwise verifies SDK symbols and parser wiring locally.
- ``logfire``: runs a read-only ``AsyncLogfireQueryClient`` and ``api.fetch``
  probe when ``LOGFIRE_READ_TOKEN`` is set; otherwise exercises
  ``AsyncLogfireQueryClient``, ``fetch_trace``, and ``parse`` against a local
  NDJSON HTTP stub server.
- ``braintrust``: runs a read-only BTQL ``api.fetch`` probe when
  ``BRAINTRUST_API_KEY`` and ``BRAINTRUST_PROJECT_ID`` are set; otherwise
  exercises ``api.fetch`` and ``parse`` against a local BTQL HTTP stub server
  via ``BRAINTRUST_API_URL``.
"""

import argparse
import asyncio
import importlib
import importlib.metadata
import inspect
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import httpx

from kitaru.api_models.v1.session_node import NodeType
from kitaru.task.importer import ImportedNode, ImportedSession

ALL_PROVIDERS = ("mlflow", "phoenix", "langfuse", "logfire", "braintrust")
DEFAULT_PHOENIX_IMAGE = "arizephoenix/phoenix:latest"


def _import_plugin_module(name: str) -> Any:
    """Import a plugin workspace module, raising a clear hint when absent."""
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            f"Could not import {name!r}. Run this script from the plugins workspace "
            "environment: uv run --project plugins --no-sync python "
            "devtools/check_importer_sdks.py"
        ) from exc


def _get_free_port() -> int:
    """Reserve and return an unused localhost TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _flatten_nodes(nodes: list[ImportedNode]) -> list[ImportedNode]:
    """Flatten imported session nodes depth-first."""
    return [node for root in nodes for node in (root, *_flatten_nodes(root.children))]


def _summarize_parsed_items(items: list[Any]) -> dict[str, int]:
    """Summarize parsed importer items into session, failure, and node counts."""
    sessions = [item for item in items if isinstance(item, ImportedSession)]
    failures = [item for item in items if not isinstance(item, ImportedSession)]
    all_nodes = [node for session in sessions for node in _flatten_nodes(session.nodes)]
    return {
        "sessions": len(sessions),
        "failures": len(failures),
        "nodes": len(all_nodes),
        "llm_nodes": sum(
            1 for node in all_nodes if node.node_type is NodeType.LLM_CALL
        ),
        "tool_nodes": sum(
            1 for node in all_nodes if node.node_type is NodeType.TOOL_CALL
        ),
    }


def _wait_for_http(url: str, *, timeout_s: float = 90.0) -> None:
    """Poll an HTTP endpoint until it responds with a non-5xx status."""
    deadline = time.monotonic() + timeout_s
    last_error = "no response"
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=2.0)
            if response.status_code < 500:
                return
            last_error = f"HTTP {response.status_code}"
        except httpx.HTTPError as exc:
            last_error = repr(exc)
        time.sleep(0.5)
    raise TimeoutError(f"Timed out waiting for {url} ({last_error})")


def _start_stub_server(
    handler_cls: type[BaseHTTPRequestHandler],
) -> tuple[ThreadingHTTPServer, str]:
    """Start a localhost HTTP server on an ephemeral port in a daemon thread."""
    port = _get_free_port()
    server = ThreadingHTTPServer(("127.0.0.1", port), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{port}"


def _restore_env(previous: dict[str, str | None]) -> None:
    """Restore environment variables to their captured values."""
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


async def check_mlflow(
    *, mlflow_version: str | None = None, require_live: bool = False
) -> dict[str, Any]:
    """Run an end-to-end trace smoke against a throwaway local MLflow server."""
    del require_live
    mlflow = _import_plugin_module("mlflow")
    mlflow_api = _import_plugin_module("kitaru_mlflow_importer.api")
    mlflow_importer = _import_plugin_module("kitaru_mlflow_importer.importer")

    locked_version = importlib.metadata.version("mlflow-tracing")
    server_version = mlflow_version or locked_version
    port = _get_free_port()
    uri = f"http://127.0.0.1:{port}"
    saved_env = {
        "MLFLOW_TRACKING_URI": os.environ.get("MLFLOW_TRACKING_URI"),
        "MLFLOW_EXPERIMENT_ID": os.environ.get("MLFLOW_EXPERIMENT_ID"),
    }

    with tempfile.TemporaryDirectory(prefix="kitaru-mlflow-smoke-") as tmp:
        db_path = Path(tmp) / "mlflow.db"
        artifacts_dir = Path(tmp) / "mlartifacts"
        proc = await asyncio.create_subprocess_exec(
            "uvx",
            "--from",
            f"mlflow=={server_version}",
            "mlflow",
            "server",
            "--backend-store-uri",
            f"sqlite:///{db_path}",
            "--default-artifact-root",
            str(artifacts_dir),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--workers",
            "1",
            cwd=tmp,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await asyncio.to_thread(_wait_for_http, f"{uri}/health", timeout_s=90.0)
            os.environ["MLFLOW_TRACKING_URI"] = uri
            mlflow.set_tracking_uri(uri)
            experiment = mlflow.set_experiment("kitaru-smoke")
            experiment_id = str(experiment.experiment_id)
            os.environ["MLFLOW_EXPERIMENT_ID"] = experiment_id

            since = datetime.now(UTC) - timedelta(seconds=5)

            @mlflow.trace(name="lookup_order", span_type="TOOL")
            def lookup_order(order_id: str) -> dict[str, str]:
                return {"order_id": order_id, "status": "damaged"}

            @mlflow.trace(name="chat_completion", span_type="LLM")
            def chat_completion(prompt: str) -> str:
                return f"Refund queued for {prompt}"

            @mlflow.trace(name="support_agent")
            def support_agent(order_id: str) -> str:
                mlflow.update_current_trace(
                    metadata={"mlflow.trace.session": f"smoke-{uuid.uuid4().hex[:8]}"}
                )
                order = lookup_order(order_id)
                return chat_completion(order["order_id"])

            support_agent("ORD-1007")
            mlflow.flush_trace_async_logging()
            until = datetime.now(UTC) + timedelta(seconds=5)

            payloads = [
                payload
                async for payload in mlflow_api.fetch(
                    {
                        "experiment_ids": [experiment_id],
                        "since": since.isoformat(),
                        "until": until.isoformat(),
                    }
                )
            ]
            parsed = [
                item
                for payload in payloads
                for item in mlflow_importer.parse(payload, {})
            ]
            summary = _summarize_parsed_items(parsed)
            return {
                "provider": "mlflow",
                "version": locked_version,
                "mode": "live_local_server",
                "payloads": len(payloads),
                **summary,
                "ok": (
                    summary["sessions"] >= 1
                    and summary["failures"] == 0
                    and summary["llm_nodes"] >= 1
                    and summary["tool_nodes"] >= 1
                ),
            }
        finally:
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=10.0)
                except TimeoutError:
                    proc.kill()
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
            _restore_env(saved_env)


async def check_phoenix(
    *, phoenix_image: str = DEFAULT_PHOENIX_IMAGE, require_live: bool = False
) -> dict[str, Any]:
    """Run an end-to-end OTLP + REST smoke against a local Arize Phoenix container."""
    del require_live
    otlp_exporter_mod = _import_plugin_module(
        "opentelemetry.exporter.otlp.proto.http.trace_exporter"
    )
    otel_resources = _import_plugin_module("opentelemetry.sdk.resources")
    otel_trace_sdk = _import_plugin_module("opentelemetry.sdk.trace")
    otel_trace_export = _import_plugin_module("opentelemetry.sdk.trace.export")
    otel_trace = _import_plugin_module("opentelemetry.trace")
    phoenix_api = _import_plugin_module("kitaru_phoenix_importer.api")
    phoenix_importer = _import_plugin_module("kitaru_phoenix_importer.importer")
    _import_plugin_module("kitaru_phoenix_importer.adapter")

    version = importlib.metadata.version("arize-phoenix-client")
    port = _get_free_port()
    container = f"kitaru-phoenix-smoke-{uuid.uuid4().hex[:8]}"
    endpoint = f"http://127.0.0.1:{port}"
    project = "kitaru-smoke"
    saved_env = {
        "PHOENIX_COLLECTOR_ENDPOINT": os.environ.get("PHOENIX_COLLECTOR_ENDPOINT"),
        "PHOENIX_PROJECT_NAME": os.environ.get("PHOENIX_PROJECT_NAME"),
    }

    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            container,
            "-e",
            "PHOENIX_ENABLE_AUTH=false",
            "-p",
            f"127.0.0.1:{port}:6006",
            phoenix_image,
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        await asyncio.to_thread(_wait_for_http, f"{endpoint}/healthz", timeout_s=90.0)
        os.environ["PHOENIX_COLLECTOR_ENDPOINT"] = endpoint
        os.environ["PHOENIX_PROJECT_NAME"] = project

        since = datetime.now(UTC) - timedelta(seconds=5)
        provider = otel_trace_sdk.TracerProvider(
            resource=otel_resources.Resource.create(
                {"openinference.project.name": project}
            )
        )
        provider.add_span_processor(
            otel_trace_export.SimpleSpanProcessor(
                otlp_exporter_mod.OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces")
            )
        )
        tracer = provider.get_tracer("kitaru.smoke")
        with tracer.start_as_current_span("kitaru-run") as root_span:
            root_span.set_attribute("openinference.span.kind", "AGENT")
            root_span.set_attribute("session.id", f"smoke-{uuid.uuid4().hex[:8]}")
            root_span.set_attribute("input.value", "Order ORD-1007 arrived damaged.")
            with tracer.start_as_current_span("lookup_order") as tool_span:
                tool_span.set_attribute("openinference.span.kind", "TOOL")
                tool_span.set_attribute("tool.name", "lookup_order")
                tool_span.set_attribute("input.value", '{"order_id": "ORD-1007"}')
                tool_span.set_attribute("output.value", '{"status": "damaged"}')
            with tracer.start_as_current_span("chat") as llm_span:
                llm_span.set_attribute("openinference.span.kind", "LLM")
                llm_span.set_attribute("llm.model_name", "gpt-5-mini")
                llm_span.set_attribute("llm.token_count.prompt", 12)
                llm_span.set_attribute("llm.token_count.completion", 8)
                llm_span.set_attribute("output.value", "Refund queued.")
            root_span.set_attribute("output.value", "Refund queued.")
            trace_id = otel_trace.format_trace_id(root_span.get_span_context().trace_id)
        provider.force_flush()
        provider.shutdown()

        spans = await asyncio.wait_for(
            phoenix_api.wait_for_spans(trace_id, project=project), timeout=30.0
        )
        until = datetime.now(UTC) + timedelta(seconds=5)
        payloads = [
            payload
            async for payload in phoenix_api.fetch(
                {
                    "project": project,
                    "since": since.isoformat(),
                    "until": until.isoformat(),
                }
            )
        ]
        parsed = [
            item for payload in payloads for item in phoenix_importer.parse(payload, {})
        ]
        summary = _summarize_parsed_items(parsed)
        return {
            "provider": "phoenix",
            "version": version,
            "mode": "live_local_server",
            "fetched_spans": len(spans),
            "payloads": len(payloads),
            **summary,
            "ok": (
                len(spans) >= 3
                and summary["sessions"] >= 1
                and summary["failures"] == 0
                and summary["llm_nodes"] >= 1
                and summary["tool_nodes"] >= 1
            ),
        }
    finally:
        subprocess.run(
            ["docker", "rm", "-f", container],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        _restore_env(saved_env)


async def check_langfuse(*, require_live: bool = False) -> dict[str, Any]:
    """Run a read-only Langfuse API probe when credentials exist, or local checks."""
    langfuse = _import_plugin_module("langfuse")
    langfuse_api = _import_plugin_module("kitaru_langfuse_importer.api")
    langfuse_importer = _import_plugin_module("kitaru_langfuse_importer.importer")
    _import_plugin_module("kitaru_langfuse_importer.adapter")

    version = importlib.metadata.version("langfuse")
    has_creds = bool(
        os.environ.get("LANGFUSE_PUBLIC_KEY") and os.environ.get("LANGFUSE_SECRET_KEY")
    )
    if has_creds:
        client = langfuse.get_client()
        try:
            response = await client.async_api.observations.get_many(
                fields="core,basic",
                limit=1,
                request_options={"timeout_in_seconds": 60},
            )
            until = datetime.now(UTC)
            since = until - timedelta(minutes=1)
            payloads = [
                payload
                async for payload in langfuse_api.fetch(
                    {"since": since.isoformat(), "until": until.isoformat()}
                )
            ]
        finally:
            client.shutdown()
        return {
            "provider": "langfuse",
            "version": version,
            "mode": "live_cloud_readonly",
            "probe_observations": len(response.data),
            "window_payloads": len(payloads),
            "ok": True,
        }

    if require_live:
        return {
            "provider": "langfuse",
            "version": version,
            "mode": "missing_credentials",
            "error": "Set LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY for live mode.",
            "ok": False,
        }

    now = datetime.now(UTC).replace(microsecond=0)
    payload = json.dumps(
        {
            "id": "trace-smoke-1",
            "sessionId": "lf-smoke-1",
            "input": "Order ORD-1007 arrived damaged.",
            "output": "Refund queued.",
            "observations": [
                {
                    "id": "obs-llm-1",
                    "traceId": "trace-smoke-1",
                    "type": "GENERATION",
                    "name": "chat",
                    "model": "gpt-5-mini",
                    "startTime": now.isoformat().replace("+00:00", "Z"),
                    "endTime": (now + timedelta(seconds=1))
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "input": [{"role": "user", "content": "Order ORD-1007"}],
                    "output": {"role": "assistant", "content": "Refund queued."},
                }
            ],
        }
    ).encode("utf-8")
    summary = _summarize_parsed_items(
        list(langfuse_importer.parse(payload, {"source_instance": "langfuse-smoke"}))
    )
    return {
        "provider": "langfuse",
        "version": version,
        "mode": "local_contract",
        **summary,
        "ok": (
            callable(langfuse.get_client)
            and summary["sessions"] >= 1
            and summary["failures"] == 0
            and summary["llm_nodes"] >= 1
        ),
    }


async def check_logfire(*, require_live: bool = False) -> dict[str, Any]:
    """Run a read-only Logfire Query API probe when token exists, or a local stub."""
    logfire = _import_plugin_module("logfire")
    logfire_query_client = _import_plugin_module("logfire.query_client")
    logfire_api = _import_plugin_module("kitaru_logfire_importer.api")
    logfire_importer = _import_plugin_module("kitaru_logfire_importer.importer")
    _import_plugin_module("kitaru_logfire_importer.adapter")

    version = importlib.metadata.version("logfire")
    query_client_cls = logfire_query_client.AsyncLogfireQueryClient
    query_sig = inspect.signature(query_client_cls.query_json_rows)
    if not (
        callable(logfire.span)
        and callable(logfire.force_flush)
        and "min_timestamp" in query_sig.parameters
    ):
        return {
            "provider": "logfire",
            "version": version,
            "mode": "contract_error",
            "error": (
                "Expected logfire SDK symbols or query_json_rows signature drifted."
            ),
            "ok": False,
        }

    token = os.environ.get("LOGFIRE_READ_TOKEN", "").strip()
    if token:
        async with query_client_cls(token) as client:
            result = await client.query_json_rows("SELECT 1 AS ok")
        until = datetime.now(UTC)
        since = until - timedelta(minutes=1)
        payloads = [
            payload
            async for payload in logfire_api.fetch(
                {"since": since.isoformat(), "until": until.isoformat()}
            )
        ]
        return {
            "provider": "logfire",
            "version": version,
            "mode": "live_cloud_readonly",
            "probe_rows": len(result.get("rows", [])),
            "window_payloads": len(payloads),
            "ok": True,
        }

    if require_live:
        return {
            "provider": "logfire",
            "version": version,
            "mode": "missing_credentials",
            "error": "Set LOGFIRE_READ_TOKEN for live mode.",
            "ok": False,
        }

    trace_id = uuid.uuid4().hex
    now = datetime.now(UTC).replace(microsecond=0)
    rows = [
        {
            "project_id": "smoke-proj",
            "trace_id": trace_id,
            "span_id": "span-root",
            "parent_span_id": None,
            "span_name": "support-agent",
            "message": "support-agent",
            "kind": "span",
            "level": 9,
            "start_timestamp": now.isoformat().replace("+00:00", "Z"),
            "end_timestamp": (now + timedelta(seconds=1))
            .isoformat()
            .replace("+00:00", "Z"),
            "service_name": "support-agent",
            "otel_scope_name": "pydantic-ai",
            "attributes": {
                "gen_ai.conversation.id": "conv-smoke-1",
                "gen_ai.agent.name": "support-agent",
            },
        },
        {
            "project_id": "smoke-proj",
            "trace_id": trace_id,
            "span_id": "span-llm",
            "parent_span_id": "span-root",
            "span_name": "chat gpt-5-mini",
            "message": "chat gpt-5-mini",
            "kind": "span",
            "level": 9,
            "start_timestamp": now.isoformat().replace("+00:00", "Z"),
            "end_timestamp": (now + timedelta(seconds=1))
            .isoformat()
            .replace("+00:00", "Z"),
            "service_name": "support-agent",
            "otel_scope_name": "pydantic-ai",
            "attributes": {
                "gen_ai.operation.name": "chat",
                "gen_ai.request.model": "gpt-5-mini",
                "gen_ai.usage.input_tokens": 10,
                "gen_ai.usage.output_tokens": 5,
            },
        },
    ]

    class _LogfireStubHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            _ = self.rfile.read(length)
            body = (json.dumps({"type": "data", "rows": rows}) + "\n").encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            del format, args

    server, base_url = _start_stub_server(_LogfireStubHandler)
    saved_env = {"LOGFIRE_READ_TOKEN": os.environ.get("LOGFIRE_READ_TOKEN")}
    try:
        os.environ["LOGFIRE_READ_TOKEN"] = "pylf_v1_us_smoketesttoken"
        async with query_client_cls(
            read_token=os.environ["LOGFIRE_READ_TOKEN"], base_url=base_url
        ) as query_client:
            payload = await logfire_api.fetch_trace(
                trace_id, now - timedelta(seconds=5), client=query_client.client
            )
        summary = _summarize_parsed_items(list(logfire_importer.parse(payload, {})))
        return {
            "provider": "logfire",
            "version": version,
            "mode": "local_stub",
            "payloads": 1,
            **summary,
            "ok": (
                summary["sessions"] >= 1
                and summary["failures"] == 0
                and summary["llm_nodes"] >= 1
            ),
        }
    finally:
        server.shutdown()
        server.server_close()
        _restore_env(saved_env)


async def check_braintrust(*, require_live: bool = False) -> dict[str, Any]:
    """Run a read-only Braintrust BTQL probe when credentials exist, or a local stub."""
    braintrust = _import_plugin_module("braintrust")
    bt_api = _import_plugin_module("kitaru_braintrust_importer.api")
    bt_importer = _import_plugin_module("kitaru_braintrust_importer.importer")
    _import_plugin_module("kitaru_braintrust_importer.adapter")

    version = importlib.metadata.version("braintrust")
    if not (
        callable(braintrust.start_span)
        and callable(braintrust.current_logger)
        and callable(braintrust.flush)
        and braintrust.SpanImpl is not None
    ):
        return {
            "provider": "braintrust",
            "version": version,
            "mode": "contract_error",
            "error": "Expected braintrust SDK symbols drifted.",
            "ok": False,
        }

    api_key = os.environ.get("BRAINTRUST_API_KEY", "").strip()
    project_id = os.environ.get("BRAINTRUST_PROJECT_ID", "").strip()
    if api_key and project_id:
        until = datetime.now(UTC)
        since = until - timedelta(minutes=1)
        payloads = [
            payload
            async for payload in bt_api.fetch(
                {
                    "project_id": project_id,
                    "since": since.isoformat(),
                    "until": until.isoformat(),
                }
            )
        ]
        return {
            "provider": "braintrust",
            "version": version,
            "mode": "live_cloud_readonly",
            "window_payloads": len(payloads),
            "ok": True,
        }

    if require_live:
        return {
            "provider": "braintrust",
            "version": version,
            "mode": "missing_credentials",
            "error": "Set BRAINTRUST_API_KEY and BRAINTRUST_PROJECT_ID for live mode.",
            "ok": False,
        }

    now = datetime.now(UTC).replace(microsecond=0)
    ts = now.timestamp()
    events = [
        {
            "id": "evt-root",
            "project_id": "proj-smoke",
            "span_id": "root-1",
            "root_span_id": "root-1",
            "span_parents": [],
            "span_attributes": {"name": "kitaru-run", "type": "task"},
            "input": "Order ORD-1007 arrived damaged.",
            "output": "Refund queued.",
            "metadata": {"session_id": "bt-smoke-1"},
            "metrics": {"start": ts, "end": ts + 0.5},
            "created": now.isoformat().replace("+00:00", "Z"),
        },
        {
            "id": "evt-llm",
            "project_id": "proj-smoke",
            "span_id": "llm-1",
            "root_span_id": "root-1",
            "span_parents": ["root-1"],
            "span_attributes": {"name": "chat", "type": "llm"},
            "input": [{"role": "user", "content": "Order ORD-1007"}],
            "output": [{"message": {"role": "assistant", "content": "Refund queued."}}],
            "metadata": {"session_id": "bt-smoke-1", "model": "gpt-5-mini"},
            "metrics": {
                "start": ts + 0.1,
                "end": ts + 0.4,
                "prompt_tokens": 10,
                "completion_tokens": 5,
            },
            "created": now.isoformat().replace("+00:00", "Z"),
        },
    ]

    class _BraintrustStubHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            request_body = json.loads(raw.decode("utf-8"))
            query = request_body.get("query", "")
            data = [events[0]] if "is_root" in query else events
            body = json.dumps({"data": data}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            del format, args

    server, base_url = _start_stub_server(_BraintrustStubHandler)
    saved_env = {
        "BRAINTRUST_API_URL": os.environ.get("BRAINTRUST_API_URL"),
        "BRAINTRUST_API_KEY": os.environ.get("BRAINTRUST_API_KEY"),
    }
    try:
        os.environ["BRAINTRUST_API_URL"] = base_url
        os.environ["BRAINTRUST_API_KEY"] = "bt-smoke-key"
        payloads = [
            payload
            async for payload in bt_api.fetch(
                {
                    "project_id": "proj-smoke",
                    "since": (now - timedelta(seconds=5)).isoformat(),
                    "until": (now + timedelta(seconds=5)).isoformat(),
                }
            )
        ]
        parsed = [
            item for payload in payloads for item in bt_importer.parse(payload, {})
        ]
        summary = _summarize_parsed_items(parsed)
        return {
            "provider": "braintrust",
            "version": version,
            "mode": "local_stub",
            "payloads": len(payloads),
            **summary,
            "ok": (
                summary["sessions"] >= 1
                and summary["failures"] == 0
                and summary["llm_nodes"] >= 1
            ),
        }
    finally:
        server.shutdown()
        server.server_close()
        _restore_env(saved_env)


def _format_human_result(result: dict[str, Any]) -> str:
    """Format one provider check result as a concise human-readable status line."""
    status = "PASS" if result.get("ok") else "FAIL"
    provider = result.get("provider", "unknown")
    version = result.get("version", "?")
    mode = result.get("mode", "error")
    if not result.get("ok"):
        error = result.get("error", "check assertions did not pass")
        return f"[{status}] {provider} ({version}, {mode}): {error}"
    details: list[str] = []
    if "sessions" in result:
        details.append(
            f"sessions={result['sessions']} nodes={result['nodes']} "
            f"(llm={result['llm_nodes']}, tool={result['tool_nodes']})"
        )
    if "probe_observations" in result:
        details.append(
            f"probe_observations={result['probe_observations']} "
            f"window_payloads={result['window_payloads']}"
        )
    if "probe_rows" in result:
        details.append(
            f"probe_rows={result['probe_rows']} "
            f"window_payloads={result['window_payloads']}"
        )
    if (
        "window_payloads" in result
        and "probe_observations" not in result
        and "probe_rows" not in result
    ):
        details.append(f"window_payloads={result['window_payloads']}")
    suffix = f": {', '.join(details)}" if details else ""
    return f"[{status}] {provider} ({version}, {mode}){suffix}"


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Smoke-test Kitaru trace importer SDKs against local servers or live APIs."
        )
    )
    parser.add_argument(
        "providers",
        nargs="*",
        choices=ALL_PROVIDERS,
        default=list(ALL_PROVIDERS),
        help="Providers to check (default: all five).",
    )
    parser.add_argument(
        "--phoenix-image",
        default=DEFAULT_PHOENIX_IMAGE,
        help=(
            "Docker image for local Arize Phoenix checks "
            f"(default: {DEFAULT_PHOENIX_IMAGE})."
        ),
    )
    parser.add_argument(
        "--mlflow-version",
        default=None,
        help="MLflow server version for uvx (default: locked mlflow-tracing version).",
    )
    parser.add_argument(
        "--require-live",
        action="store_true",
        help=(
            "Fail instead of falling back to local stubs when cloud credentials "
            "are absent."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="emit_json",
        help=(
            "Print one JSON object per provider instead of human-readable status lines."
        ),
    )
    return parser


async def run_checks(args: argparse.Namespace) -> int:
    """Run requested importer smoke checks and return the process exit code."""
    runners: dict[str, Callable[[], Coroutine[Any, Any, dict[str, Any]]]] = {
        "mlflow": lambda: check_mlflow(
            mlflow_version=args.mlflow_version, require_live=args.require_live
        ),
        "phoenix": lambda: check_phoenix(
            phoenix_image=args.phoenix_image, require_live=args.require_live
        ),
        "langfuse": lambda: check_langfuse(require_live=args.require_live),
        "logfire": lambda: check_logfire(require_live=args.require_live),
        "braintrust": lambda: check_braintrust(require_live=args.require_live),
    }
    all_ok = True
    for name in args.providers:
        try:
            result = await runners[name]()
        except Exception as exc:
            result = {"provider": name, "ok": False, "error": repr(exc)}
        if args.emit_json:
            print(json.dumps(result), flush=True)
        else:
            print(_format_human_result(result), flush=True)
        if not result.get("ok"):
            all_ok = False
    return 0 if all_ok else 1


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and execute the importer SDK smoke checks."""
    args = _build_parser().parse_args(argv)
    return asyncio.run(run_checks(args))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
