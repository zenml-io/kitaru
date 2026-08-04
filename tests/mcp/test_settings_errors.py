#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Settings precedence, stable error mapping, and redaction tests."""

import httpx
import pytest
from pydantic import ValidationError

from kitaru.client.exceptions import APIError
from kitaru.mcp.errors import MCPOutputValidationError, MCPToolError, map_exception
from kitaru.mcp.settings import CapabilityMode, MCPSettings


def test_settings_default_and_explicit_over_environment_precedence() -> None:
    assert MCPSettings().mode is CapabilityMode.READ_ONLY
    settings = MCPSettings.from_environment(
        {
            "KITARU_MCP_MODE": "standard",
            "KITARU_MCP_TIMEOUT": "10",
            "KITARU_MCP_DEBUG": "true",
        },
        mode="destructive",
        timeout=20,
    )
    assert settings.mode is CapabilityMode.DESTRUCTIVE
    assert settings.timeout == 20
    assert settings.debug is True


def test_settings_reject_conflicts_nonfinite_and_invalid_bounds() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        MCPSettings(server_url="https://one", context_name="two")
    for field in ("server_url", "context_name"):
        with pytest.raises(ValueError, match="must not be blank"):
            MCPSettings.model_validate({field: "  \t"})
    for variable in ("KITARU_MCP_SERVER", "KITARU_MCP_CONTEXT"):
        with pytest.raises(ValueError, match="must not be blank"):
            MCPSettings.from_environment({variable: ""})
    with pytest.raises(ValueError, match="must not be blank"):
        MCPSettings.from_environment(
            {"KITARU_MCP_SERVER": "https://valid.example"}, server_url=""
        )
    with pytest.raises(ValueError, match="must not be blank"):
        MCPSettings.from_environment(
            {"KITARU_MCP_CONTEXT": "valid-context"}, context_name="  "
        )
    with pytest.raises(ValueError, match="finite"):
        MCPSettings(timeout=float("inf"))
    with pytest.raises(ValueError):
        MCPSettings(max_concurrency=0)


@pytest.mark.parametrize(
    ("error", "code", "retryable"),
    [
        (APIError(400, "bad"), "invalid_arguments", False),
        (APIError(401, "bad"), "authentication_failed", False),
        (APIError(403, "bad"), "permission_denied", False),
        (APIError(404, "bad"), "not_found", False),
        (
            APIError(
                409,
                "The idempotency key was already used for a different request.",
            ),
            "idempotency_mismatch",
            False,
        ),
        (APIError(409, "request in progress"), "request_in_progress", True),
        (APIError(425, "in progress"), "request_in_progress", True),
        (APIError(429, "slow down"), "rate_limited", True),
        (APIError(500, "failed"), "remote_failed", True),
        (httpx.ConnectError("offline"), "network_error", True),
        (httpx.ReadTimeout("slow"), "timeout", True),
        (RuntimeError("secret"), "internal_error", False),
    ],
)
def test_complete_expected_error_categories(
    error: BaseException, code: str, retryable: bool
) -> None:
    mapped = map_exception(error)
    assert mapped.code == code
    assert mapped.retryable is retryable


def test_input_and_output_validation_map_to_distinct_contracts() -> None:
    validation_error = _get_validation_error()
    caller_input = map_exception(validation_error)
    remote_output = map_exception(MCPOutputValidationError(validation_error))
    assert caller_input.code == "invalid_arguments"
    assert caller_input.message == "Tool arguments are invalid."
    assert remote_output.code == "internal_error"
    assert remote_output.message == "The Kitaru response failed MCP output validation."


def test_remote_error_body_is_not_returned_to_the_model() -> None:
    mapped = map_exception(APIError(500, "Bearer top-secret arbitrary response"))
    assert "top-secret" not in mapped.message
    assert mapped.message == "The Kitaru server failed the request."


def _get_validation_error() -> ValidationError:
    with pytest.raises(ValidationError) as error:
        MCPSettings.model_validate({"max_concurrency": 0})
    return error.value


def test_tool_error_keeps_bounded_recovery_contract() -> None:
    mapped = map_exception(
        MCPToolError(
            "partial_failure",
            "One step failed",
            details={"completed": 1},
            recovery="Inspect the returned receipt.",
        )
    )
    assert mapped.details == {"completed": 1}
    assert mapped.recovery == "Inspect the returned receipt."
