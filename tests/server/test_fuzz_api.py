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

    KITARU_FUZZ=1 uv run pytest tests/server/test_fuzz_api.py -p no:randomly

The assertion is only that the server never answers 5xx. One database is
shared by the whole session, so rows an earlier operation writes are visible
to a later one; that makes "was this input rejected?" depend on run order,
while "did the server crash?" stays well-posed. Schemathesis's
``negative_data_rejection`` check is therefore deliberately not run here.
"""

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import schemathesis
from fuzz_server import FuzzServer
from hypothesis import HealthCheck, settings
from schemathesis.checks import not_a_server_error
from schemathesis.generation import GenerationMode
from schemathesis.specs.openapi.checks import response_schema_conformance

pytestmark = pytest.mark.skipif(
    not os.environ.get("KITARU_FUZZ"),
    reason="API fuzzing is opt-in; set KITARU_FUZZ=1 (needs docker compose up -d db)",
)

SPEC_PATH = Path(__file__).parents[2] / "openapi" / "openapi.json"

# Operations with a filed, reproduced defect. The fuzzer's job is to surface
# what is not yet known, so a crash already on the tracker would otherwise
# mask everything behind it in the same operation. Delete an entry as part of
# closing its issue; the run then covers the operation again.
KNOWN_FAILURES: dict[tuple[str, str], str] = {
    ("POST", "/api/v1/device_authorization"): "#927 NUL byte reaches PostgreSQL",
    ("POST", "/api/v1/login"): "#927 NUL byte reaches PostgreSQL",
    ("POST", "/api/v1/imports"): "#927 NUL byte reaches PostgreSQL",
    ("POST", "/api/v1/importers"): "#927 NUL byte reaches PostgreSQL",
    (
        "GET",
        "/api/v1/evaluators/{evaluator_id}/versions/{version}",
    ): "#928 int32 overflow",
    (
        "PATCH",
        "/api/v1/evaluators/{evaluator_id}/versions/{version}",
    ): "#928 int32 overflow",
    (
        "GET",
        "/api/v1/importers/{importer_id}/versions/{version}",
    ): "#928 int32 overflow",
    (
        "PATCH",
        "/api/v1/importers/{importer_id}/versions/{version}",
    ): "#928 int32 overflow",
    ("POST", "/api/v1/api-keys"): "#931 idempotency key decryption",
    ("POST", "/api/v1/api-keys/{api_key_id}/rotate"): "#931 idempotency key decryption",
}

# Derandomized runs replay the same inputs every time, which is what a
# reproducible gate wants. Nightly exploration wants fresh inputs instead.
DERANDOMIZE = os.environ.get("KITARU_FUZZ_RANDOM") is None

FUZZ_SETTINGS = settings(
    max_examples=int(os.environ.get("KITARU_FUZZ_MAX_EXAMPLES", "25")),
    deadline=None,
    derandomize=DERANDOMIZE,
    database=None,
    suppress_health_check=list(HealthCheck),
)


def _load_schema(*modes: GenerationMode) -> Any:
    schema = schemathesis.openapi.from_path(str(SPEC_PATH))
    schema.config.generation.modes = list(modes)
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


@schema_hunt.parametrize()
@FUZZ_SETTINGS
def test_no_server_error(case: schemathesis.Case, fuzz_server: FuzzServer) -> None:
    """Reject any input that makes the server answer 5xx."""
    _run(case, fuzz_server, [not_a_server_error])


@pytest.mark.skipif(
    not os.environ.get("KITARU_FUZZ_SCHEMA_CONFORMANCE"),
    reason="blocked on #930: every hand-raised 422 violates its documented schema",
)
@_load_schema(GenerationMode.POSITIVE).parametrize()
@FUZZ_SETTINGS
def test_response_matches_schema(
    case: schemathesis.Case, fuzz_server: FuzzServer
) -> None:
    """Reject a response whose body does not match its declared schema."""
    _run(case, fuzz_server, [not_a_server_error, response_schema_conformance])
