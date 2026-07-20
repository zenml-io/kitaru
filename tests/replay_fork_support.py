"""Shared deterministic bootstrap for replay-fork tests.

This module deliberately performs setup only.  Each caller keeps the
behavioral assertions for the scenario it exercises.
"""

import importlib
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic_ai import messages as pydantic_messages
from pydantic_ai.models.function import FunctionModel
from zenml.client import Client

from kitaru import KitaruClient
from tests.test_replay_fork_demo import DEMO_ROOT, _load_demo_module

FIXTURE = DEMO_ROOT / "trace_fixtures" / "imported-support-cases.jsonl"
ACCOUNT_TRACE_ID = "support-account-setting"
FIXED_CANDIDATE_VERSION = "permissions-fix-v1"


@dataclass
class ReplayForkBootstrap:
    """Assertion-free state shared by deterministic and live replay tests."""

    repository_root: Path
    demo: Any
    client: KitaruClient
    source_state: Any
    repeated_source_state: Any
    preview: Any
    imported: Any
    repeated_import: Any
    account_execution_id: str
    tool_boundary: Any
    model_boundary: Any
    model_resumed: Any
    recorded_model: FunctionModel


def bootstrap_account_setting_comparable_suite(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    *,
    preserve_openai_api_key: bool = False,
) -> ReplayForkBootstrap:
    """Create one imported account case and freeze its comparable suite."""
    repository_root = Path(Client.find_repository())
    _initialize_repository(repository_root)
    shutil.copytree(DEMO_ROOT / "evals", repository_root / "evals")
    shutil.copytree(DEMO_ROOT / "reference_agent", repository_root / "reference_agent")
    fixture_dir = repository_root / "trace_fixtures"
    fixture_dir.mkdir()
    shutil.copy2(FIXTURE, fixture_dir / FIXTURE.name)
    shutil.copy2(DEMO_ROOT / "demo.py", repository_root / "demo.py")

    _clear_copied_modules()
    request.addfinalizer(_clear_copied_modules)
    monkeypatch.syspath_prepend(str(repository_root))
    if not preserve_openai_api_key:
        monkeypatch.setenv("OPENAI_API_KEY", "deterministic-test-key")
    demo = _load_demo_module(repository_root)
    registration = cast(Any, importlib.import_module("evals.register"))
    source_agent = registration.baseline_agent
    source_agent.register(
        label=demo.SOURCE_VERSION,
        entrypoint="evals.register:baseline_agent",
    )
    source_state = source_agent._registered_state
    assert source_state is not None
    source_agent.register(
        label=demo.SOURCE_VERSION,
        entrypoint="evals.register:baseline_agent",
    )
    repeated_source_state = source_agent._registered_state
    assert repeated_source_state is not None

    fixture = repository_root / "trace_fixtures" / "imported-support-cases.jsonl"
    client = KitaruClient()
    preview = client.imports.langfuse(
        str(fixture),
        source_project_id="langfuse-replay-example",
        agent=demo.AGENT_NAME,
        version=demo.SOURCE_VERSION,
        trace_ids=[ACCOUNT_TRACE_ID],
        dry_run=True,
    )
    imported = client.imports.langfuse(
        str(fixture),
        source_project_id="langfuse-replay-example",
        agent=demo.AGENT_NAME,
        version=demo.SOURCE_VERSION,
        trace_ids=[ACCOUNT_TRACE_ID],
        dry_run=False,
        confirm_data_storage=True,
    )
    repeated_import = client.imports.langfuse(
        str(fixture),
        source_project_id="langfuse-replay-example",
        agent=demo.AGENT_NAME,
        version=demo.SOURCE_VERSION,
        trace_ids=[ACCOUNT_TRACE_ID],
        dry_run=False,
        confirm_data_storage=True,
    )
    account_execution_id = str(imported.outcomes[0].execution_id)
    tool_boundary = demo._message_history_boundary(
        account_execution_id,
        kind="tool-result",
        index=1,
    )
    model_boundary = demo._message_history_boundary(
        account_execution_id,
        kind="model-message",
        index=0,
    )
    recorded_model = recorded_path_model()
    default_candidate = registration.mini_tool_budget_2_agent
    try:
        model_resumed = demo._resume_case(
            account_execution_id,
            boundary_kind="model-message",
            boundary_index=0,
            name="account-setting-model-message",
            idempotency_key="account-setting-model-message-v1",
            candidate_variant="mini_tool_budget_2",
            candidate_version=FIXED_CANDIDATE_VERSION,
            model=recorded_model,
        )
    finally:
        registration.mini_tool_budget_2_agent = default_candidate
    return ReplayForkBootstrap(
        repository_root=repository_root,
        demo=demo,
        client=client,
        source_state=source_state,
        repeated_source_state=repeated_source_state,
        preview=preview,
        imported=imported,
        repeated_import=repeated_import,
        account_execution_id=account_execution_id,
        tool_boundary=tool_boundary,
        model_boundary=model_boundary,
        model_resumed=model_resumed,
        recorded_model=recorded_model,
    )


def _initialize_repository(repository_root: Path) -> None:
    (repository_root / ".gitignore").write_text(".kitaru/\n", encoding="utf-8")
    (repository_root / "acceptance-entrypoint.txt").write_text(
        "replay acceptance\n", encoding="utf-8"
    )
    subprocess.run(["git", "init", "-q", str(repository_root)], check=True)
    subprocess.run(
        ["git", "-C", str(repository_root), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repository_root), "config", "user.name", "Test"],
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(repository_root),
            "add",
            ".gitignore",
            "acceptance-entrypoint.txt",
        ],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repository_root), "commit", "-qm", "acceptance entrypoint"],
        check=True,
    )


def _clear_copied_modules() -> None:
    for module_name in tuple(sys.modules):
        if module_name == "evals" or module_name.startswith("evals."):
            sys.modules.pop(module_name)
        if module_name == "reference_agent" or module_name.startswith(
            "reference_agent."
        ):
            sys.modules.pop(module_name)
    importlib.invalidate_caches()


def _tool_returns(
    messages: list[pydantic_messages.ModelMessage],
) -> list[pydantic_messages.ToolReturnPart]:
    return [
        part
        for message in messages
        if isinstance(message, pydantic_messages.ModelRequest)
        for part in message.parts
        if isinstance(part, pydantic_messages.ToolReturnPart)
    ]


def _root_prompt(messages: list[pydantic_messages.ModelMessage]) -> str:
    for message in messages:
        if not isinstance(message, pydantic_messages.ModelRequest):
            continue
        for part in message.parts:
            if isinstance(part, pydantic_messages.UserPromptPart):
                return str(part.content)
    raise AssertionError("deterministic model received no root prompt")


def _final_result(arguments: dict[str, Any]) -> pydantic_messages.ModelResponse:
    return pydantic_messages.ModelResponse(
        parts=[
            pydantic_messages.TextPart(content=json.dumps(arguments, sort_keys=True))
        ]
    )


def recorded_path_model() -> FunctionModel:
    """Return the deterministic model that follows both checked-in cases."""

    def respond(
        messages: list[pydantic_messages.ModelMessage],
        _info: Any,
    ) -> pydantic_messages.ModelResponse:
        prompt = _root_prompt(messages)
        returned_names = [part.tool_name for part in _tool_returns(messages)]

        if "beta_exports_fast_path" in prompt:
            if "lookup_customer" not in returned_names:
                return pydantic_messages.ModelResponse(
                    parts=[
                        pydantic_messages.ToolCallPart(
                            tool_name="lookup_customer",
                            args={"email_or_id": "Acme"},
                            tool_call_id="candidate-lookup",
                        )
                    ]
                )
            if "search_kb" not in returned_names:
                return pydantic_messages.ModelResponse(
                    parts=[
                        pydantic_messages.ToolCallPart(
                            tool_name="search_kb",
                            args={
                                "query": (
                                    "beta_exports_fast_path enable account setting "
                                    "policy admin change SSO beta feature enablement "
                                    "policy"
                                )
                            },
                            tool_call_id="candidate-kb",
                        )
                    ]
                )
            if "escalate_to_human" not in returned_names:
                return pydantic_messages.ModelResponse(
                    parts=[
                        pydantic_messages.ToolCallPart(
                            tool_name="escalate_to_human",
                            args={
                                "customer_id": "cust_acme",
                                "policy_label": "permissions_policy",
                            },
                            tool_call_id="candidate-escalation",
                        )
                    ]
                )
            return _final_result(
                {
                    "policy_label": "permissions_policy",
                    "risk_status": "needs_review",
                    "required_action": "escalate_to_human",
                    "summary": "Restricted setting change sent for human approval.",
                    "evidence_ids": ["db:audit:escalation:cust_acme"],
                    "tool_names": ["escalate_to_human"],
                }
            )

        if "currently timing out" in prompt:
            if "get_service_status" not in returned_names:
                return pydantic_messages.ModelResponse(
                    parts=[
                        pydantic_messages.ToolCallPart(
                            tool_name="get_service_status",
                            args={"service": "export API"},
                            tool_call_id="candidate-status",
                        )
                    ]
                )
            if "search_kb" not in returned_names:
                return pydantic_messages.ModelResponse(
                    parts=[
                        pydantic_messages.ToolCallPart(
                            tool_name="search_kb",
                            args={"query": "export API timeout outage"},
                            tool_call_id="candidate-status-kb",
                        )
                    ]
                )
            return _final_result(
                {
                    "policy_label": "incident_policy",
                    "risk_status": "safe",
                    "required_action": "answer_directly",
                    "summary": (
                        "The recorded status and incident evidence were reproduced."
                    ),
                    "evidence_ids": ["api:status:export API"],
                    "tool_names": ["get_service_status", "search_kb"],
                }
            )

        raise AssertionError(f"unexpected deterministic prompt: {prompt}")

    return FunctionModel(respond)
