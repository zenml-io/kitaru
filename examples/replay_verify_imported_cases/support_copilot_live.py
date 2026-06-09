"""Live PydanticAI runner for imported-input Replay Verify cases.

Unlike ``support_copilot_demo`` (deterministic, credential-free), this module
makes real model calls through a PydanticAI agent. The trust rules stay the
same:

- The agent only sees imported ``root_input`` and the imported
  ``available_tools``; tools delegate to the same deterministic, side-effect
  safe ``tool_registry`` used by the deterministic runner.
- ``tool_names`` and ``retrieval_document_ids`` in the payload are derived from
  the tool calls the agent actually made, never from the model's self-report.
- Exceptions propagate so the orchestrator fails the case closed.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel
from pydantic_ai import Agent, Tool
from pydantic_ai.models import Model
from pydantic_ai.settings import ModelSettings
from pydantic_ai.usage import UsageLimits

from examples.replay_verify_imported_cases.live_prompt_config import (
    BASELINE_LIVE_CONFIG,
    CANDIDATE_LIVE_CONFIG,
    LIVE_RUNNER_ENTRYPOINT,
    LivePromptConfig,
)
from examples.replay_verify_imported_cases.tool_registry import run_imported_tool
from kitaru._replay_verify_imported_models import ImportedReplayCase
from kitaru._replay_verify_imported_runner import (
    ImportedRunnerInvocation,
    ImportedRunnerOutput,
)

__all__ = [
    "LIVE_RUNNER_ENTRYPOINT",
    "SupportCopilotLiveOutput",
    "execute_case_tool",
    "run_baseline_support_copilot_case_live",
    "run_candidate_support_copilot_case_live",
    "run_support_copilot_case_live",
]

# Exact vocabulary from the deterministic runner
# (support_copilot_demo.POLICY_LABEL_VOCABULARY / RISK_STATUS_VOCABULARY);
# tests assert these Literal args stay equal to those constants.
PolicyLabel = Literal[
    "support_policy",
    "billing_policy",
    "escalation_policy",
    "knowledge_base_policy",
]
RiskStatus = Literal["safe", "needs_review"]


class SupportCopilotLiveOutput(BaseModel):
    """Structured fields the live agent must emit.

    ``tool_names`` and ``retrieval_document_ids`` are intentionally absent:
    they are code-derived from captured tool calls, not model output.
    """

    policy_label: PolicyLabel
    risk_status: RiskStatus
    response: str


_TOOL_DESCRIPTIONS = {
    "lookup_subscription": (
        "Look up the customer's subscription status for this imported case."
    ),
    "lookup_invoice": "Look up the customer's invoice status for this imported case.",
    "search_knowledge_base": (
        "Search the support knowledge base and return imported document ids."
    ),
    "create_support_ticket": (
        "Create a mocked support ticket for this imported case (no live write)."
    ),
}


def run_baseline_support_copilot_case_live(
    case: ImportedReplayCase,
    invocation: ImportedRunnerInvocation,
) -> ImportedRunnerOutput:
    """Run the baseline live support-copilot agent for one imported case."""
    return _run_live_case(case, invocation, BASELINE_LIVE_CONFIG)


def run_candidate_support_copilot_case_live(
    case: ImportedReplayCase,
    invocation: ImportedRunnerInvocation,
) -> ImportedRunnerOutput:
    """Run the candidate live support-copilot agent for one imported case."""
    return _run_live_case(case, invocation, CANDIDATE_LIVE_CONFIG)


def run_support_copilot_case_live(
    case: ImportedReplayCase,
    invocation: ImportedRunnerInvocation,
) -> ImportedRunnerOutput:
    """Dispatch one imported case to the baseline or candidate live config."""
    config = (
        BASELINE_LIVE_CONFIG if invocation.role == "baseline" else CANDIDATE_LIVE_CONFIG
    )
    return _run_live_case(case, invocation, config)


def execute_case_tool(
    tool_name: str,
    case: ImportedReplayCase,
    invocation: ImportedRunnerInvocation,
    captured: list[tuple[str, dict[str, Any]]],
) -> dict[str, Any]:
    """Run one registry tool for the live agent and record the call.

    Delegates to ``tool_registry.run_imported_tool``, which raises for any tool
    not present in ``invocation.available_tools`` and always returns
    ``executed_live: False`` results.
    """
    result = run_imported_tool(tool_name, case, invocation)
    captured.append((tool_name, result))
    return result


def _run_live_case(
    case: ImportedReplayCase,
    invocation: ImportedRunnerInvocation,
    config: LivePromptConfig,
) -> ImportedRunnerOutput:
    model = invocation.config.get("model") or config.model
    captured: list[tuple[str, dict[str, Any]]] = []
    agent = Agent(
        model,
        output_type=SupportCopilotLiveOutput,
        instructions=config.prompt_text,
        tools=[
            _case_tool(tool_name, case, invocation, captured)
            for tool_name in invocation.available_tools
        ],
        model_settings=ModelSettings(
            temperature=config.temperature,
            max_tokens=config.max_output_tokens,
        ),
    )
    result = agent.run_sync(
        _user_prompt(case),
        usage_limits=UsageLimits(request_limit=config.usage_limit_requests),
    )
    output = result.output.model_dump()
    metadata = {
        "live_model_call": True,
        "model": _model_identifier(model),
        "prompt_version": config.prompt_version,
        "prompt_hash": config.prompt_hash,
        "case_id": case.case_id,
        "runner_role": invocation.role,
        "runner_id": invocation.runner_id,
        "execution_mode": invocation.execution_mode,
    }
    payload = {
        "policy_label": output["policy_label"],
        "risk_status": output["risk_status"],
        # Code-derived from captured calls, never the model's self-report.
        "tool_names": sorted({name for name, _ in captured}),
        "retrieval_document_ids": _captured_retrieval_document_ids(captured),
        "response": output["response"],
        "tool_results": [tool_result for _, tool_result in captured],
        "metadata": metadata,
    }
    return ImportedRunnerOutput(
        payload=payload,
        metadata=metadata,
        # Derived from the captured tool results, not a hardcoded zero, so a
        # registry regression that executes live is reported by the runner too.
        unsafe_live_execution_count=sum(
            1 for _, result in captured if result.get("executed_live") is True
        ),
    )


def _case_tool(
    tool_name: str,
    case: ImportedReplayCase,
    invocation: ImportedRunnerInvocation,
    captured: list[tuple[str, dict[str, Any]]],
) -> Tool:
    def _run() -> dict[str, Any]:
        return execute_case_tool(tool_name, case, invocation, captured)

    _run.__name__ = tool_name
    return Tool(
        _run,
        name=tool_name,
        takes_ctx=False,
        description=_TOOL_DESCRIPTIONS.get(
            tool_name, f"Run the {tool_name} support tool for this case."
        ),
    )


def _captured_retrieval_document_ids(
    captured: list[tuple[str, dict[str, Any]]],
) -> list[str]:
    for tool_name, result in captured:
        if tool_name == "search_knowledge_base":
            document_ids = result.get("document_ids")
            if isinstance(document_ids, list):
                return [str(document_id) for document_id in document_ids]
    return []


def _user_prompt(case: ImportedReplayCase) -> str:
    root = case.root_input if isinstance(case.root_input, Mapping) else {}
    message = str(root.get("user_message") or "Support request received.")
    context = ", ".join(
        f"{key}={root[key]}"
        for key in ("account_id", "plan", "invoice_id", "ticket_subject")
        if key in root
    )
    if context:
        return f"{message}\n\nImported case context: {context}."
    return message


def _model_identifier(model: object) -> str:
    if isinstance(model, str):
        return model
    if isinstance(model, Model):
        return model.model_name
    return str(model)
