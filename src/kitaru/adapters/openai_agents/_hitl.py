"""Human-in-the-loop request helpers for the OpenAI Agents SDK adapter."""

import kitaru
from kitaru.errors import KitaruUsageError

from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._types import (
    OpenAIApprovalDecision,
    OpenAIInterruptionSummary,
    OpenAIRunRequest,
    OpenAIRunResult,
)


def build_resume_request(
    result: OpenAIRunResult,
    *,
    approve: bool,
    interruption_index: int | None = None,
    rejection_message: str | None = None,
) -> OpenAIRunRequest:
    """Build a resume request for one pending OpenAI interruption."""
    if result.status != "interrupted":
        raise KitaruUsageError(
            "`build_resume_request()` requires an interrupted OpenAI run result."
        )
    if result.pending_state is None:
        raise KitaruUsageError(
            "Interrupted OpenAI run result is missing pending_state."
        )

    interruption = _select_interruption(result, interruption_index)

    decision = OpenAIApprovalDecision(
        interruption_index=interruption.index,
        approve=approve,
        rejection_message=rejection_message,
    )
    return OpenAIRunRequest.resume(result.pending_state, decision)


def wait_for_approval(
    result: OpenAIRunResult,
    *,
    name: str | None = None,
    interruption_index: int | None = None,
    denial_message: str = "Denied via Kitaru wait.",
    timeout: int | None = None,
) -> OpenAIRunRequest:
    """Wait for human approval and return an OpenAI resume request."""
    if not is_inside_flow():
        raise KitaruUsageError("`wait_for_approval()` must be called inside a flow.")
    if is_inside_checkpoint():
        raise KitaruUsageError(
            "`wait_for_approval()` must be called from the flow body, not "
            "inside a checkpoint."
        )
    if result.status != "interrupted":
        raise KitaruUsageError(
            "`wait_for_approval()` requires an interrupted OpenAI run result."
        )
    if result.pending_state is None:
        raise KitaruUsageError(
            "Interrupted OpenAI run result is missing pending_state."
        )

    interruption = _select_interruption(result, interruption_index)
    metadata = {
        "adapter": "openai_agents",
        "source": "approval_bridge",
        "interruption_index": interruption.index,
        "tool_name": interruption.tool_name,
        "call_id": interruption.call_id,
    }
    question = _approval_question(interruption)
    approved = bool(
        kitaru.wait(
            schema=bool,
            name=name or "OpenAI tool approval",
            question=question,
            timeout=timeout,
            metadata=metadata,
        )
    )
    return build_resume_request(
        result,
        approve=approved,
        interruption_index=interruption.index,
        rejection_message=None if approved else denial_message,
    )


def _select_interruption(
    result: OpenAIRunResult, index: int | None
) -> OpenAIInterruptionSummary:
    if not result.interruptions:
        raise KitaruUsageError(
            "Interrupted OpenAI run result has no interruption summaries."
        )
    if index is None:
        return result.interruptions[0]
    for interruption in result.interruptions:
        if interruption.index == index:
            return interruption
    available = ", ".join(str(item.index) for item in result.interruptions)
    raise KitaruUsageError(
        "OpenAI interrupted run result does not contain interruption index "
        f"{index}. Available indexes: {available}."
    )


def _approval_question(interruption: OpenAIInterruptionSummary | None) -> str:
    if interruption is not None and interruption.tool_name:
        if interruption.call_id:
            return (
                f"Approve OpenAI tool call `{interruption.tool_name}` "
                f"({interruption.call_id})?"
            )
        return f"Approve OpenAI tool call `{interruption.tool_name}`?"
    return "Approve the pending OpenAI Agents SDK interruption?"
