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
"""Schemathesis fuzzing of the REST API against a live server.

Requests are generated from ``openapi/openapi.json`` in both schema-conformant
and schema-violating modes and sent to the real app on a disposable PostgreSQL
database. Run with ``just fuzz-api``, or:

    KITARU_FUZZ=1 uv run --extra server --group fuzz \
        pytest tests/server/test_fuzz_api.py -p no:randomly

Without the ``fuzz`` group installed the module skips itself at collection
rather than failing, so a plain ``uv run pytest`` reports nothing.

The assertion is only that the server never answers 5xx. One database is
shared by the whole session, so rows an earlier operation writes are visible
to a later one; that makes "was this input rejected?" depend on run order,
while "did the server crash?" stays well-posed. Schemathesis's
``negative_data_rejection`` check is therefore deliberately not run here.
"""

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest

# Skip before the schemas below are built: parametrizing them generates every
# coverage-phase case at collection, about 1.3 GB that a skip marker would
# still pay for.
if not os.environ.get("KITARU_FUZZ"):
    pytest.skip(
        "API fuzzing is opt-in; set KITARU_FUZZ=1 (needs docker compose up -d db)",
        allow_module_level=True,
    )

# The schemathesis dependency lives in the `fuzz` group, which the regular
# test matrix does not install, so guard the import before the module loads.
pytest.importorskip("schemathesis")

import requests
import schemathesis
from fuzz_server import FuzzServer
from hypothesis import HealthCheck, settings
from schemathesis import HookContext
from schemathesis.checks import not_a_server_error
from schemathesis.core.failures import Failure, FailureGroup, ServerError
from schemathesis.generation import GenerationMode
from schemathesis.specs.openapi.checks import response_schema_conformance

SPEC_PATH = Path(__file__).parents[2] / "openapi" / "openapi.json"

# Operations with a filed, reproduced defect, mapped to the issue that owns
# them. The fuzzer's job is to surface what is not yet known, so a crash
# already on the tracker would otherwise mask everything behind it in the same
# operation. Delete an entry as part of closing its issue; the run then covers
# the operation again. Empty is the healthy state: every operation is covered.
KNOWN_FAILURES: dict[tuple[str, str], str] = {}

# Proxies in front of a real deployment commonly reject request lines over
# 8 KB. Longer URLs only exercise uvicorn's HTTP parser, which refuses a
# request head once more than 16 KB of it is buffered unfinished. Whether that
# happens depends on how the bytes arrive, so the same input can pass on
# replay and Hypothesis reports it as flaky (issue #1538).
MAX_URL_BYTES = 8 * 1024

# Derandomized runs replay the same inputs every time, which is what a
# reproducible gate wants. Nightly exploration wants fresh inputs instead.
DERANDOMIZE = os.environ.get("KITARU_FUZZ_RANDOM") is None

FUZZ_SETTINGS = settings(
    max_examples=int(os.environ.get("KITARU_FUZZ_MAX_EXAMPLES", "25")),
    deadline=None,
    derandomize=DERANDOMIZE,
    suppress_health_check=list(HealthCheck),
)


def _is_url_within_limit(context: HookContext | None, case: schemathesis.Case) -> bool:
    """Return whether a case's encoded URL fits within ``MAX_URL_BYTES``."""
    _ = context
    # Measure against an HTTP placeholder because requests leaves the query off
    # the schema's own file:// base URL. The real base URL adds a few bytes.
    kwargs = case.as_transport_kwargs(base_url="http://127.0.0.1")
    prepared = requests.Request(
        method=kwargs["method"], url=kwargs["url"], params=kwargs.get("params")
    ).prepare()
    return len(prepared.url or "") <= MAX_URL_BYTES


def _load_schema(*modes: GenerationMode) -> Any:
    schema = schemathesis.openapi.from_path(str(SPEC_PATH))
    schema.config.generation.modes = list(modes)
    schema.hook("filter_case")(_is_url_within_limit)
    return schema


schema_hunt = _load_schema(GenerationMode.POSITIVE, GenerationMode.NEGATIVE)


@pytest.fixture(scope="session")
def fuzz_server() -> Iterator[FuzzServer]:
    """Provide one live server and disposable database for the whole session."""
    server = FuzzServer()
    server.start()
    yield server
    server.stop()


def _run(case: schemathesis.Case, server: FuzzServer, checks: list[Any]) -> None:
    known = KNOWN_FAILURES.get((case.method.upper(), case.path))
    if known is not None:
        pytest.skip(f"known defect: {known}")
    try:
        case.call_and_validate(
            base_url=server.base_url,
            headers=server.auth_headers,
            checks=checks,
            timeout=30,
        )
    except AssertionError:
        captured = server.capture.describe(case.method.upper(), case.path)
        if captured:
            pytest.fail(f"{case.method.upper()} {case.path}\n{captured}", pytrace=False)
        raise
    except FailureGroup as exc:
        if any(isinstance(failure, ServerError) for failure in exc.exceptions):
            captured = server.capture.describe(case.method.upper(), case.path)
            if captured:
                pytest.fail(
                    f"{case.method.upper()} {case.path}\n{captured}", pytrace=False
                )
        raise


def test_url_limit_measures_encoded_query() -> None:
    """Drop a case only when its encoded URL is longer than real proxies accept."""
    operation = schema_hunt["/api/v1/connections"]["GET"]
    # Each "[" encodes to three bytes, so the raw value alone stays under the
    # limit while the URL on the wire goes over it.
    nested = operation.Case(query={"filter": "[" * (MAX_URL_BYTES // 2)})
    short = operation.Case(query={"sort": "name:asc", "size": 5})

    assert not _is_url_within_limit(None, nested)
    assert _is_url_within_limit(None, short)
    assert _is_url_within_limit in schema_hunt.hooks.get_all_by_name("filter_case")


def test_run_reports_captured_grouped_failure() -> None:
    """Include the server exception when Schemathesis groups check failures."""

    class FailingCase:
        method = "POST"
        path = "/api/v1/users"

        def call_and_validate(self, **kwargs: Any) -> None:
            _ = kwargs
            raise FailureGroup(
                [ServerError(operation="POST /api/v1/users", status_code=500)]
            )

    class Capture:
        def describe(self, method: str, path: str) -> str:
            assert (method, path) == ("POST", "/api/v1/users")
            return "ValueError: password cannot be longer than 72 bytes"

    class Server:
        def __init__(self) -> None:
            self.base_url = "http://test"
            self.auth_headers: dict[str, str] = {}
            self.capture = Capture()

    with pytest.raises(
        pytest.fail.Exception, match="password cannot be longer than 72 bytes"
    ):
        _run(
            cast(schemathesis.Case, FailingCase()),
            cast(FuzzServer, Server()),
            [],
        )


def test_run_preserves_group_without_server_error() -> None:
    """Do not replace a non-server failure with stale captured details."""

    class FailingCase:
        method = "POST"
        path = "/api/v1/workers"

        def call_and_validate(self, **kwargs: Any) -> None:
            _ = kwargs
            raise FailureGroup(
                [
                    Failure(
                        operation="POST /api/v1/workers",
                        title="Schema mismatch",
                        message="Response violates the schema",
                    )
                ]
            )

    class Capture:
        def describe(self, method: str, path: str) -> str:
            raise AssertionError(f"unexpected capture lookup for {method} {path}")

    class Server:
        def __init__(self) -> None:
            self.base_url = "http://test"
            self.auth_headers: dict[str, str] = {}
            self.capture = Capture()

    with pytest.raises(FailureGroup):
        _run(
            cast(schemathesis.Case, FailingCase()),
            cast(FuzzServer, Server()),
            [],
        )


@schema_hunt.parametrize()
@FUZZ_SETTINGS
def test_no_server_error(case: schemathesis.Case, fuzz_server: FuzzServer) -> None:
    """Reject any input that makes the server answer 5xx."""
    _run(case, fuzz_server, [not_a_server_error])


@_load_schema(GenerationMode.POSITIVE).parametrize()
@FUZZ_SETTINGS
def test_response_matches_schema(
    case: schemathesis.Case, fuzz_server: FuzzServer
) -> None:
    """Reject a response whose body does not match its declared schema."""
    _run(case, fuzz_server, [not_a_server_error, response_schema_conformance])
