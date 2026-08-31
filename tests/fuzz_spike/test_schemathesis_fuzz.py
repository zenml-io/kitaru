"""Schemathesis fuzzing of the real Kitaru API over a live in-process server.

Positive pass: schema-conformant inputs, checks not_a_server_error +
response_schema_conformance. Negative pass: schema-violating inputs, check
that they are rejected with 4xx (plus not_a_server_error).

Run with:
    KITARU_FUZZ=1 uv run pytest tests/fuzz_spike/ -p no:randomly -x
"""

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
import schemathesis
from hypothesis import HealthCheck, settings
from schemathesis.checks import not_a_server_error
from schemathesis.generation import GenerationMode
from schemathesis.specs.openapi.checks import (
    negative_data_rejection,
    response_schema_conformance,
)

from fuzz_harness import FuzzServer

pytestmark = pytest.mark.skipif(
    not os.environ.get("KITARU_FUZZ"),
    reason="fuzzing spike, run with KITARU_FUZZ=1",
)

SPEC_PATH = Path(__file__).parents[2] / "openapi" / "openapi.json"

MAX_EXAMPLES = int(os.environ.get("KITARU_FUZZ_MAX_EXAMPLES", "25"))

FUZZ_SETTINGS = settings(
    max_examples=MAX_EXAMPLES,
    deadline=None,
    derandomize=True,
    database=None,
    suppress_health_check=list(HealthCheck),
)

schema_positive = schemathesis.openapi.from_path(str(SPEC_PATH))
schema_positive.config.generation.modes = [GenerationMode.POSITIVE]

schema_negative = schemathesis.openapi.from_path(str(SPEC_PATH))
schema_negative.config.generation.modes = [GenerationMode.NEGATIVE]


@pytest.fixture(scope="session")
def fuzz_server() -> Iterator[FuzzServer]:
    server = FuzzServer()
    server.start()
    yield server
    server.stop()


@schema_positive.parametrize()
@FUZZ_SETTINGS
def test_fuzz_positive(case: schemathesis.Case, fuzz_server: FuzzServer) -> None:
    case.call_and_validate(
        base_url=fuzz_server.base_url,
        headers=fuzz_server.auth_headers,
        checks=[not_a_server_error, response_schema_conformance],
        timeout=30,
    )


@schema_negative.parametrize()
@FUZZ_SETTINGS
def test_fuzz_negative(case: schemathesis.Case, fuzz_server: FuzzServer) -> None:
    case.call_and_validate(
        base_url=fuzz_server.base_url,
        headers=fuzz_server.auth_headers,
        checks=[not_a_server_error, negative_data_rejection],
        timeout=30,
    )


schema_hunt = schemathesis.openapi.from_path(str(SPEC_PATH))
schema_hunt.config.generation.modes = [
    GenerationMode.POSITIVE,
    GenerationMode.NEGATIVE,
]

HUNT_SETTINGS = settings(
    max_examples=int(os.environ.get("KITARU_FUZZ_HUNT_EXAMPLES", "50")),
    deadline=None,
    derandomize=True,
    database=None,
    suppress_health_check=list(HealthCheck),
)


@schema_hunt.parametrize()
@HUNT_SETTINGS
def test_fuzz_500_hunt(case: schemathesis.Case, fuzz_server: FuzzServer) -> None:
    # The 422-shape schema gap (string `detail`) fails schema conformance on
    # nearly every operation and masks later examples there, so this pass
    # checks nothing but "no 500" to maximize crash-hunting depth.
    case.call_and_validate(
        base_url=fuzz_server.base_url,
        headers=fuzz_server.auth_headers,
        checks=[not_a_server_error],
        timeout=30,
    )
