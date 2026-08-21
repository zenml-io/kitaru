"""Convert a retained Codex JSONL trace into portable Kitaru JSONL."""

import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any

_HOME_PATH = re.compile(r"/(?:Users|home)/[^/\s]+")
_MACOS_TEMP_PATH = re.compile(r"/(?:private/)?var/folders/[^\s\"']+")
_SECRET = re.compile(r"\b(?:sk|rk|pk)_[A-Za-z0-9_-]{12,}\b")
_HOOK_TRUST_WARNING = "`--dangerously-bypass-hook-trust` is enabled."
_GENAI_PRICES_SOURCE = (
    "https://github.com/pydantic/genai-prices/blob/main/prices/new_data/v2/data.json"
)


def convert_trace(
    trace_path: Path,
    *,
    task: dict[str, Any],
    answer: str,
    score_receipt: dict[str, Any],
    run_metadata: dict[str, Any],
) -> dict[str, Any]:
    """Convert one retained native trace into one portable Kitaru session.

    The converter intentionally recognizes only the event shapes used by the
    V0 wrapper. Unsupported records stay visible as a fidelity gap in session
    metadata rather than being guessed at or discarded silently.

    Args:
        trace_path: Native JSONL emitted by ``codex exec --json``.
        task: Agent-visible task metadata.
        answer: Answer copied from the disposable workdir after the run.
        score_receipt: External scorer receipt.
        run_metadata: Wrapper provenance and validation results.

    Returns:
        One Kitaru JSONL-compatible session object.
    """
    records = _read_records(trace_path)
    activity_nodes = _nodes_from_records(records)
    session_id = _session_id(records)
    started_at = run_metadata.get("started_at") or _first_timestamp(records)
    ended_at = run_metadata.get("ended_at") or _last_timestamp(records)
    usage = _usage(records)
    cost, cost_metadata = _estimate_cost(usage, run_metadata.get("model"))
    root_node = _agent_run_node(
        task=task,
        answer=answer,
        score_receipt=score_receipt,
        run_metadata=run_metadata,
        started_at=started_at,
        ended_at=ended_at,
        usage=usage,
        cost=cost,
        cost_metadata=cost_metadata,
    )
    nodes = [root_node, *_attach_activity_nodes(activity_nodes)]
    redacted, redaction_applied = _redact(
        {
            "task": task,
            "answer": answer,
            "score_receipt": score_receipt,
            "nodes": nodes,
            "run_metadata": run_metadata,
        }
    )
    return {
        "status": "completed" if run_metadata.get("exit_code") == 0 else "failed",
        "name": f"DABstep coding-agent task {task['task_id']}",
        "inputs": redacted["task"],
        "outputs": {"answer": redacted["answer"], "score": redacted["score_receipt"]},
        "error": (
            None if run_metadata.get("exit_code") == 0 else "Codex exited non-zero"
        ),
        "started_at": started_at,
        "ended_at": ended_at,
        "external_id": f"codex-dabstep-{session_id or trace_path.stem}",
        "metadata": {
            "source": "codex-jsonl",
            "codex_jsonl": {
                "session_id": session_id,
                "record_types": sorted(
                    {record.get("type", "unknown") for record in records}
                ),
            },
            "fidelity_gaps": [
                "V0 preserves visible messages and tool records only.",
                "Native workspace state and hidden reasoning are not replayable.",
            ],
            "redaction": {"applied": redaction_applied, "policy": "v0-path-and-token"},
            "intervention": {
                "kind": "skill",
                "name": redacted["run_metadata"].get("skill_name"),
                "sha256": redacted["run_metadata"].get("skill_sha256"),
                "content": redacted["run_metadata"].get("skill_content"),
            },
            "invocation": {
                "prompt": redacted["run_metadata"].get("invocation_prompt"),
                "model": redacted["run_metadata"].get("model"),
                "model_provider": redacted["run_metadata"].get("model_provider"),
                "system_prompt": {
                    "available": False,
                    "reason": "Not exposed by codex exec --json.",
                },
            },
            "cost_estimate": cost_metadata,
            "run": redacted["run_metadata"],
        },
        "framework": "codex-cli",
        "nodes": redacted["nodes"],
    }


def _read_records(trace_path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line in trace_path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            records.append(value)
    return records


def _nodes_from_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    tool_calls: dict[str, dict[str, Any]] = {}
    nodes: list[dict[str, Any]] = []
    item_started_at = {
        str(item["id"]): _record_timestamp(record)
        for record in records
        if record.get("type") == "item.started"
        and isinstance((item := record.get("item")), dict)
        and item.get("id") is not None
    }
    for record in records:
        if _append_codex_exec_item(nodes, record, item_started_at):
            continue
        if record.get("type") in {"wrapper.output", "wrapper.error"}:
            message = str(record.get("message", ""))
            is_error = (
                record.get("type") == "wrapper.error" or "error" in message.lower()
            )
            nodes.append(
                _node(
                    nodes,
                    node_type="span",
                    name="Codex diagnostic",
                    status="failed" if is_error else "completed",
                    external_id=None,
                    inputs={},
                    outputs={"message": message},
                    started_at=_record_timestamp(record),
                    ended_at=_record_timestamp(record),
                )
            )
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        payload_type = payload.get("type")
        timestamp = _record_timestamp(record)
        if record.get("type") == "response_item" and payload_type in {
            "function_call",
            "custom_tool_call",
        }:
            call_id = str(payload.get("call_id", len(tool_calls)))
            tool_calls[call_id] = {
                "index": len(nodes),
                "parent_index": None,
                "secondary_parent_indexes": [],
                "external_id": call_id,
                "trace_id": None,
                "node_type": "tool_call",
                "name": str(payload.get("name", "tool call")),
                "status": "completed",
                "started_at": timestamp,
                "ended_at": None,
                "inputs": _tool_input(payload),
                "outputs": None,
                "tool_name": str(payload.get("name", "tool call")),
                "attributes": {"source_record_type": payload_type},
                "metadata": {},
            }
            nodes.append(tool_calls[call_id])
        elif record.get("type") == "response_item" and payload_type in {
            "function_call_output",
            "custom_tool_call_output",
        }:
            call_id = str(payload.get("call_id", ""))
            if node := tool_calls.get(call_id):
                node["outputs"] = payload.get("output")
                node["ended_at"] = timestamp
        elif payload_type in {"agent_message", "message"}:
            text = _message_text(payload)
            if text:
                nodes.append(
                    {
                        "index": len(nodes),
                        "parent_index": None,
                        "secondary_parent_indexes": [],
                        "external_id": None,
                        "trace_id": None,
                        "node_type": "llm_call",
                        "name": "Codex message",
                        "status": "completed",
                        "started_at": timestamp,
                        "ended_at": timestamp,
                        "inputs": {"role": payload.get("role", "assistant")},
                        "outputs": {"text": text},
                        "output_text_selector": "/text",
                        "attributes": {"source_record_type": payload_type},
                        "metadata": {},
                    }
                )
    return nodes


def _append_codex_exec_item(
    nodes: list[dict[str, Any]],
    record: dict[str, Any],
    item_started_at: dict[str, str | None],
) -> bool:
    """Append one completed current ``codex exec --json`` item when present."""
    item = record.get("item")
    if record.get("type") != "item.completed" or not isinstance(item, dict):
        return False
    item_type = item.get("type")
    timestamp = _record_timestamp(record)
    started_at = item_started_at.get(str(item.get("id")), timestamp)
    if item_type == "command_execution":
        nodes.append(
            _node(
                nodes,
                node_type="tool_call",
                name="Codex command",
                status="failed" if item.get("status") == "failed" else "completed",
                external_id=item.get("id"),
                inputs={"command": item.get("command")},
                outputs={
                    "output": item.get("aggregated_output"),
                    "exit_code": item.get("exit_code"),
                },
                tool_name="command_execution",
                started_at=started_at,
                ended_at=timestamp,
            )
        )
    elif item_type == "agent_message":
        nodes.append(
            _node(
                nodes,
                node_type="span",
                name="Codex message",
                status="completed",
                external_id=item.get("id"),
                inputs={"role": "assistant"},
                outputs={"text": item.get("text", "")},
                output_text_selector="/text",
                started_at=started_at,
                ended_at=timestamp,
            )
        )
    elif item_type == "file_change":
        nodes.append(
            _node(
                nodes,
                node_type="span",
                name="Codex file change",
                status="failed" if item.get("status") == "failed" else "completed",
                external_id=item.get("id"),
                inputs={},
                outputs={"changes": item.get("changes")},
                started_at=started_at,
                ended_at=timestamp,
            )
        )
    elif item_type == "error":
        message = str(item.get("message", "Codex error"))
        is_warning = message.startswith(_HOOK_TRUST_WARNING)
        nodes.append(
            _node(
                nodes,
                node_type="span",
                name="Codex warning" if is_warning else "Codex error",
                status="completed" if is_warning else "failed",
                external_id=item.get("id"),
                inputs={},
                outputs={"message": message},
                error=None if is_warning else message,
                started_at=started_at,
                ended_at=timestamp,
            )
        )
    return True


def _node(
    nodes: list[dict[str, Any]],
    *,
    node_type: str,
    name: str,
    status: str,
    external_id: Any,
    inputs: Any,
    outputs: Any,
    tool_name: str | None = None,
    output_text_selector: str | None = None,
    error: str | None = None,
    started_at: str | None = None,
    ended_at: str | None = None,
) -> dict[str, Any]:
    """Build the common portable node shape for a current Codex event."""
    return {
        "index": len(nodes),
        "parent_index": None,
        "secondary_parent_indexes": [],
        "external_id": str(external_id) if external_id is not None else None,
        "trace_id": None,
        "node_type": node_type,
        "name": name,
        "status": status,
        "error": error,
        "started_at": started_at,
        "ended_at": ended_at,
        "inputs": inputs,
        "outputs": outputs,
        "output_text_selector": output_text_selector,
        "tool_name": tool_name,
        "attributes": {"source_record_type": "codex-exec-item"},
        "metadata": {},
    }


def _tool_input(payload: dict[str, Any]) -> Any:
    value = payload.get("arguments", payload.get("input"))
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {"raw": value}
    return value


def _message_text(payload: dict[str, Any]) -> str | None:
    message = payload.get("message")
    if isinstance(message, str):
        return message
    content = payload.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return (
            "\n".join(
                item.get("text", "") for item in content if isinstance(item, dict)
            )
            or None
        )
    return None


def _session_id(records: list[dict[str, Any]]) -> str | None:
    for record in records:
        if record.get("type") == "thread.started" and record.get("thread_id"):
            return str(record["thread_id"])
        payload = record.get("payload")
        if record.get("type") == "session_meta" and isinstance(payload, dict):
            value = payload.get("session_id", payload.get("id"))
            if value:
                return str(value)
    return None


def _first_timestamp(records: list[dict[str, Any]]) -> str | None:
    timestamps = (_record_timestamp(record) for record in records)
    return next((timestamp for timestamp in timestamps if timestamp), None)


def _last_timestamp(records: list[dict[str, Any]]) -> str | None:
    return next(
        (
            _record_timestamp(record)
            for record in reversed(records)
            if _record_timestamp(record)
        ),
        None,
    )


def _record_timestamp(record: dict[str, Any]) -> str | None:
    value = record.get("timestamp") or record.get("_kitaru_observed_at")
    return str(value) if value else None


def _usage(records: list[dict[str, Any]]) -> dict[str, int] | None:
    totals = {
        "input_tokens": 0,
        "output_tokens": 0,
        "cached_input_tokens": 0,
        "reasoning_tokens": 0,
    }
    found = False
    for record in records:
        if record.get("type") != "turn.completed":
            continue
        usage = record.get("usage")
        if not isinstance(usage, dict):
            continue
        found = True
        totals["input_tokens"] += int(usage.get("input_tokens") or 0)
        totals["output_tokens"] += int(usage.get("output_tokens") or 0)
        totals["cached_input_tokens"] += int(usage.get("cached_input_tokens") or 0)
        totals["reasoning_tokens"] += int(usage.get("reasoning_output_tokens") or 0)
    return totals if found else None


def _estimate_cost(
    usage: dict[str, int] | None, model: Any
) -> tuple[str | None, dict[str, Any] | None]:
    """Estimate GPT-5.4 API-equivalent cost from a fixed price snapshot."""
    if usage is None or model != "gpt-5.4":
        return None, None
    input_tokens = usage["input_tokens"]
    cached_tokens = usage["cached_input_tokens"]
    if cached_tokens > input_tokens:
        return None, {"available": False, "reason": "Invalid cached token total."}
    input_rate = Decimal("2.5")
    cached_rate = Decimal("0.25")
    output_rate = Decimal("15")
    million = Decimal(1_000_000)
    uncached_tokens = input_tokens - cached_tokens
    total = (
        Decimal(uncached_tokens) * input_rate
        + Decimal(cached_tokens) * cached_rate
        + Decimal(usage["output_tokens"]) * output_rate
    ) / million
    return str(total), {
        "available": True,
        "kind": "estimated_api_equivalent",
        "currency": "USD",
        "model": model,
        "provider": "openai",
        "amount": str(total),
        "pricing_source": _GENAI_PRICES_SOURCE,
        "pricing_snapshot_date": "2026-08-21",
        "tiered_rate_applied": False,
        "warning": (
            "Indicative base-rate estimate, not an observed Codex charge. Codex "
            "reports aggregate turn usage, so long-context requests may cost more."
        ),
    }


def _agent_run_node(
    *,
    task: dict[str, Any],
    answer: str,
    score_receipt: dict[str, Any],
    run_metadata: dict[str, Any],
    started_at: str | None,
    ended_at: str | None,
    usage: dict[str, int] | None,
    cost: str | None,
    cost_metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build the root span representing the complete Codex agent invocation."""
    return {
        "index": 0,
        "parent_index": None,
        "secondary_parent_indexes": [],
        "external_id": None,
        "trace_id": None,
        "node_type": "span",
        "name": "Codex agent run",
        "status": "completed" if run_metadata.get("exit_code") == 0 else "failed",
        "error": None,
        "started_at": started_at,
        "ended_at": ended_at,
        "input_text_selector": "/prompt",
        "inputs": {
            "prompt": run_metadata.get("invocation_prompt"),
            "task": task,
            "skill": {
                "name": run_metadata.get("skill_name"),
                "sha256": run_metadata.get("skill_sha256"),
                "content": run_metadata.get("skill_content"),
            },
        },
        "outputs": {"answer": answer, "score": score_receipt},
        "requested_model": run_metadata.get("model"),
        "model": run_metadata.get("model"),
        "model_provider": run_metadata.get("model_provider"),
        "tokens": usage,
        "cost": cost,
        "attributes": {"source_record_type": "codex-agent-run"},
        "metadata": {"cost_estimate": cost_metadata},
    }


def _attach_activity_nodes(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Place native activity nodes beneath the synthetic agent-run root span."""
    for node in nodes:
        node["index"] += 1
        if node.get("parent_index") is None:
            node["parent_index"] = 0
        else:
            node["parent_index"] += 1
        node["secondary_parent_indexes"] = [
            index + 1 for index in node.get("secondary_parent_indexes", [])
        ]
    return nodes


def _redact(value: Any) -> tuple[Any, bool]:
    if isinstance(value, dict):
        redacted_items = {key: _redact(item) for key, item in value.items()}
        return (
            {key: item for key, (item, _) in redacted_items.items()},
            any(changed for _, changed in redacted_items.values()),
        )
    if isinstance(value, list):
        redacted_items = [_redact(item) for item in value]
        return (
            [item for item, _ in redacted_items],
            any(changed for _, changed in redacted_items),
        )
    if isinstance(value, str):
        result = _SECRET.sub("<redacted-token>", value)
        result = _HOME_PATH.sub("<redacted-home>", result)
        result = _MACOS_TEMP_PATH.sub("<redacted-temp>", result)
        return result, result != value
    return value, False
