"""Human-in-the-loop request helpers for the LangGraph adapter."""

from typing import Any, Literal, cast

import kitaru
from kitaru.errors import KitaruUsageError

from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._types import (
    LangGraphInterruptSummary,
    LangGraphRunRequest,
    LangGraphRunResult,
)


def build_resume_request(
    result: LangGraphRunResult,
    resume: Any,
    *,
    interrupt_index: int | None = None,
    durability: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> LangGraphRunRequest:
    """Build a LangGraph ``Command(resume=...)`` request for an interrupt."""
    if result.status != "interrupted":
        raise KitaruUsageError(
            "`build_resume_request()` requires an interrupted LangGraph result."
        )
    if result.pending_state is None:
        raise KitaruUsageError("Interrupted LangGraph result is missing pending_state.")
    _select_interrupt(result, interrupt_index)

    from langgraph.types import Command

    resolved_durability = cast(Literal["sync", "async", "exit"] | None, durability)

    return LangGraphRunRequest.resume(
        Command(resume=resume),
        thread_id=result.pending_state.thread_id,
        # Do not force the latest checkpoint_id into the resume config. With
        # LangGraph 1.1.x, doing so can pin the call to the interrupted
        # checkpoint and produce the same interrupt again. The stable thread_id
        # is the durable identity LangGraph uses to find the pending thread.
        checkpoint_ns=result.pending_state.checkpoint_ns,
        durability=resolved_durability,
        metadata=metadata or {},
    )


def wait_for_interrupt(
    result: LangGraphRunResult,
    *,
    schema: Any,
    name: str | None = None,
    question: str | None = None,
    interrupt_index: int | None = None,
    timeout: int | None = None,
    durability: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> LangGraphRunRequest:
    """Wait at flow scope and return a LangGraph resume request."""
    if not is_inside_flow():
        raise KitaruUsageError("`wait_for_interrupt()` must be called inside a flow.")
    if is_inside_checkpoint():
        raise KitaruUsageError(
            "`wait_for_interrupt()` must be called from the flow body, not "
            "inside a checkpoint."
        )
    if result.status != "interrupted":
        raise KitaruUsageError(
            "`wait_for_interrupt()` requires an interrupted LangGraph result."
        )

    interrupt = _select_interrupt(result, interrupt_index)
    wait_metadata = {
        "adapter": "langgraph",
        "source": "interrupt_bridge",
        "interrupt_index": interrupt.index,
        "task_id": interrupt.task_id,
        "node_name": interrupt.node_name,
    }
    if metadata:
        wait_metadata.update(metadata)
    payload = kitaru.wait(
        schema=schema,
        name=name or "LangGraph interrupt",
        question=question or _interrupt_question(interrupt),
        timeout=timeout,
        metadata=wait_metadata,
    )
    return build_resume_request(
        result,
        payload,
        interrupt_index=interrupt.index,
        durability=durability,
    )


def _select_interrupt(
    result: LangGraphRunResult, index: int | None
) -> LangGraphInterruptSummary:
    if not result.interrupts:
        raise KitaruUsageError("Interrupted LangGraph result has no interrupts.")
    if index is None:
        return result.interrupts[0]
    for interrupt in result.interrupts:
        if interrupt.index == index:
            return interrupt
    available = ", ".join(str(item.index) for item in result.interrupts)
    raise KitaruUsageError(
        "LangGraph interrupted result does not contain interrupt index "
        f"{index}. Available indexes: {available}."
    )


def _interrupt_question(interrupt: LangGraphInterruptSummary) -> str:
    if interrupt.node_name:
        return f"Resume LangGraph node `{interrupt.node_name}`?"
    return "Resume the pending LangGraph interrupt?"
