"""Small stateful (link-inferred) Schemathesis run over the agents resources.

Run with:
    KITARU_FUZZ=1 uv run pytest tests/fuzz_spike/test_stateful_agents.py -q
"""

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import schemathesis
from hypothesis import HealthCheck, settings
from hypothesis.stateful import run_state_machine_as_test
from schemathesis.checks import not_a_server_error
from schemathesis.core.transport import Response
from schemathesis.generation import GenerationMode
from schemathesis.specs.openapi.checks import use_after_free

from fuzz_harness import FuzzServer

pytestmark = pytest.mark.skipif(
    not os.environ.get("KITARU_FUZZ"),
    reason="fuzzing spike, run with KITARU_FUZZ=1",
)

SPEC_PATH = Path(__file__).parents[2] / "openapi" / "openapi.json"

schema = schemathesis.openapi.from_path(str(SPEC_PATH))
schema.config.generation.modes = [GenerationMode.POSITIVE]
schema_agents = schema.include(tag_regex="^(agents|agent-versions)$")


@pytest.fixture(scope="session")
def fuzz_server() -> Iterator[FuzzServer]:
    server = FuzzServer()
    server.start()
    yield server
    server.stop()


def test_stateful_agents(fuzz_server: FuzzServer) -> None:
    base_machine = schema_agents.as_state_machine()

    class AgentsMachine(base_machine):  # type: ignore[misc, valid-type]
        def get_call_kwargs(self, case: schemathesis.Case) -> dict[str, Any]:
            return {
                "base_url": fuzz_server.base_url,
                "headers": fuzz_server.auth_headers,
                "timeout": 30,
            }

        def validate_response(
            self,
            response: Response,
            case: schemathesis.Case,
            additional_checks: Any = None,
            **kwargs: Any,
        ) -> None:
            # The spec declares only success + 422 per operation, so the
            # default status_code_conformance check trips on every legitimate
            # 404/409; restrict validation to the checks that carry signal.
            _ = additional_checks
            case.validate_response(
                response,
                # response_schema_conformance is excluded here: the known
                # string-detail-in-422 gap (finding F1) fires on nearly every
                # chain and would mask deeper stateful signal.
                checks=[not_a_server_error, use_after_free],
                transport_kwargs=kwargs,
            )

    run_state_machine_as_test(
        AgentsMachine,
        settings=settings(
            max_examples=30,
            deadline=None,
            stateful_step_count=8,
            suppress_health_check=list(HealthCheck),
        ),
    )
