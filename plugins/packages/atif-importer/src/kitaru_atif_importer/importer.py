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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Import archived ATIF documents without reading referenced files or URLs."""

import hashlib
import json
import math
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal
from typing import Any
from urllib.parse import quote

from pydantic_core import PydanticSerializationError

from kitaru.api_models.v1.session import SessionCreateRequest, SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import (
    ImportedNode,
    ImportedSession,
    ImportFailure,
    SessionImportError,
    flatten_nodes,
)

__all__ = ["parse"]

_MAX_DEPTH = 32
_TOKEN_FIELDS = {
    "prompt_tokens": "input_tokens",
    "completion_tokens": "output_tokens",
    "cached_tokens": "cached_input_tokens",
}


def _get_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _get_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be an array")
    return value


def _get_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonempty string")
    return value


def _validate_optional_fields(value: dict[str, Any]) -> None:
    for key in ("extra",):
        if value.get(key) is not None:
            _get_object(value[key], key)


def _validate_number(value: Any, label: str, *, integer: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{label} must be a number")
    if integer and not isinstance(value, int):
        raise ValueError(f"{label} must be an integer")
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{label} must be finite and nonnegative")


def _validate_metrics(value: Any, *, final: bool = False) -> None:
    metrics = _get_object(value, "metrics")
    _validate_optional_fields(metrics)
    extra = metrics.get("extra") or {}
    if extra.get("reasoning_output_tokens") is not None:
        _validate_number(
            extra["reasoning_output_tokens"], "reasoning_output_tokens", integer=True
        )
    prefix = "total_" if final else ""
    for key in (*_TOKEN_FIELDS, "steps" if final else "llm_call_count"):
        name = prefix + key
        if metrics.get(name) is not None:
            _validate_number(metrics[name], name, integer=True)
    if metrics.get(prefix + "cost_usd") is not None:
        _validate_number(metrics[prefix + "cost_usd"], prefix + "cost_usd")
    for key in ("prompt_token_ids", "completion_token_ids"):
        if metrics.get(key) is not None:
            for token in _get_list(metrics[key], key):
                _validate_number(token, key, integer=True)
    if metrics.get("logprobs") is not None:
        for number in _get_list(metrics["logprobs"], "logprobs"):
            if (
                isinstance(number, bool)
                or not isinstance(number, int | float)
                or not math.isfinite(number)
            ):
                raise ValueError("logprobs must contain finite numbers")
    prompt, cached = (
        metrics.get(prefix + "prompt_tokens"),
        metrics.get(prefix + "cached_tokens"),
    )
    if prompt is not None and cached is not None and cached > prompt:
        raise ValueError("cached_tokens cannot exceed prompt_tokens")


def _validate_content(value: Any, label: str, *, nullable: bool = False) -> None:
    if isinstance(value, str) or (nullable and value is None):
        return
    for raw_part in _get_list(value, label):
        part = _get_object(raw_part, "content part")
        kind = part.get("type")
        if kind == "text":
            if not isinstance(part.get("text"), str) or part.get("source") is not None:
                raise ValueError("text content requires text and no source")
        elif kind in {"image", "audio"}:
            source = _get_object(part.get("source"), "media source")
            _get_string(source.get("path"), "media source.path")
            media_type = _get_string(source.get("media_type"), "media_type")
            if not media_type.lower().strip().startswith(f"{kind}/"):
                raise ValueError("media_type must match the content type")
            if part.get("text") is not None:
                raise ValueError("media content cannot contain text")
            if source.get("duration_sec") is not None:
                _validate_number(source["duration_sec"], "duration_sec")
        else:
            raise ValueError("content type must be text, image, or audio")


def _get_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    raw = _get_string(value, "timestamp")
    try:
        timestamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("timestamp must be ISO 8601") from None
    # ATIF permits naive times. Preserve them in metadata, without inventing UTC.
    return timestamp if timestamp.tzinfo is not None else None


def _validate_trajectory(value: Any, depth: int = 0) -> dict[str, Any]:
    if depth > _MAX_DEPTH:
        raise ValueError("embedded trajectory nesting exceeds 32 levels")
    trajectory = _get_object(value, "trajectory")
    if trajectory.get("schema_version") not in {
        f"ATIF-v1.{minor}" for minor in range(9)
    }:
        raise ValueError("schema_version must be ATIF-v1.0 through ATIF-v1.8")
    _validate_optional_fields(trajectory)
    for key in ("session_id", "trajectory_id", "continued_trajectory_ref", "notes"):
        if trajectory.get(key) is not None and not isinstance(trajectory[key], str):
            raise ValueError(f"{key} must be a string")
    agent = _get_object(trajectory.get("agent"), "agent")
    for key in ("name", "version"):
        _get_string(agent.get(key), f"agent.{key}")
    if agent.get("model_name") is not None:
        _get_string(agent["model_name"], "agent.model_name")
    _validate_optional_fields(agent)
    if agent.get("tool_definitions") is not None:
        for definition in _get_list(agent["tool_definitions"], "tool_definitions"):
            _get_object(definition, "tool definition")
    if trajectory.get("final_metrics") is not None:
        _validate_metrics(trajectory["final_metrics"], final=True)
    steps = _get_list(trajectory.get("steps"), "steps")
    if not steps:
        raise ValueError("steps cannot be empty")
    for expected_id, raw_step in enumerate(steps, 1):
        step = _get_object(raw_step, "step")
        if type(step.get("step_id")) is not int or step["step_id"] != expected_id:
            raise ValueError("step_id values must be sequential integers starting at 1")
        source = step.get("source")
        if source not in {"system", "user", "agent"}:
            raise ValueError("step source must be system, user, or agent")
        _validate_content(step.get("message"), "message")
        _validate_optional_fields(step)
        _get_timestamp(step.get("timestamp"))
        for key in ("model_name", "reasoning_content"):
            if step.get(key) is not None and not isinstance(step[key], str):
                raise ValueError(f"{key} must be a string")
        effort = step.get("reasoning_effort")
        if (
            effort is not None
            and not isinstance(effort, str)
            and (
                isinstance(effort, bool)
                or not isinstance(effort, int | float)
                or not math.isfinite(effort)
            )
        ):
            raise ValueError("reasoning_effort must be a string or finite number")
        if step.get("is_copied_context") is not None and not isinstance(
            step["is_copied_context"], bool
        ):
            raise ValueError("is_copied_context must be a boolean")
        count = step.get("llm_call_count")
        if count is not None:
            _validate_number(count, "llm_call_count", integer=True)
        if source != "agent" and any(
            step.get(key) is not None
            for key in (
                "model_name",
                "reasoning_effort",
                "reasoning_content",
                "tool_calls",
                "metrics",
            )
        ):
            raise ValueError("LLM and tool fields require source agent")
        if (
            count == 0
            and source == "agent"
            and any(
                step.get(key) is not None for key in ("metrics", "reasoning_content")
            )
        ):
            raise ValueError(
                "llm_call_count 0 requires absent metrics and reasoning_content"
            )
        if step.get("metrics") is not None:
            _validate_metrics(step["metrics"])
        call_ids: set[str] = set()
        if step.get("tool_calls") is not None:
            for raw_call in _get_list(step["tool_calls"], "tool_calls"):
                call = _get_object(raw_call, "tool call")
                call_id = _get_string(call.get("tool_call_id"), "tool_call_id")
                if call_id in call_ids:
                    raise ValueError("tool_call_id values must be unique within a step")
                call_ids.add(call_id)
                _get_string(call.get("function_name"), "function_name")
                _get_object(call.get("arguments"), "tool arguments")
                _validate_optional_fields(call)
        if step.get("observation") is not None:
            observation = _get_object(step["observation"], "observation")
            _validate_optional_fields(observation)
            for raw_result in _get_list(
                observation.get("results"), "observation.results"
            ):
                result = _get_object(raw_result, "observation result")
                source_id = result.get("source_call_id")
                if source_id is not None and (
                    not isinstance(source_id, str) or source_id not in call_ids
                ):
                    raise ValueError(
                        "source_call_id must reference a tool call in its step"
                    )
                _validate_content(
                    result.get("content"), "observation content", nullable=True
                )
                _validate_optional_fields(result)
                result_extra = result.get("extra") or {}
                if result_extra.get(
                    "tool_result_is_error"
                ) is not None and not isinstance(
                    result_extra["tool_result_is_error"], bool
                ):
                    raise ValueError("tool_result_is_error must be a boolean")
                if result.get("subagent_trajectory_ref") is not None:
                    for raw_ref in _get_list(
                        result["subagent_trajectory_ref"], "subagent_trajectory_ref"
                    ):
                        ref = _get_object(raw_ref, "subagent reference")
                        _validate_optional_fields(ref)
                        for key in ("trajectory_id", "trajectory_path", "session_id"):
                            if ref.get(key) is not None:
                                _get_string(ref[key], f"subagent reference.{key}")
                        if not ref.get("trajectory_id") and not ref.get(
                            "trajectory_path"
                        ):
                            raise ValueError(
                                "subagent reference requires trajectory_id "
                                "or trajectory_path"
                            )
    embedded_ids: set[str] = set()
    if trajectory.get("subagent_trajectories") is not None:
        for embedded in _get_list(
            trajectory["subagent_trajectories"], "subagent_trajectories"
        ):
            child = _validate_trajectory(embedded, depth + 1)
            child_id = _get_string(child.get("trajectory_id"), "embedded trajectory_id")
            if child_id in embedded_ids:
                raise ValueError("embedded trajectory_id values must be unique")
            embedded_ids.add(child_id)
    return trajectory


def _get_text_selector(content: Any, prefix: str) -> str | None:
    if isinstance(content, str):
        return prefix
    if isinstance(content, list):
        for index, part in enumerate(content):
            if isinstance(part, dict) and part.get("type") == "text":
                return f"{prefix}/{index}/text"
    return None


def _get_document_metadata(trajectory: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in trajectory.items()
        if key not in {"steps", "subagent_trajectories"}
    }


def _get_argument_selector(arguments: dict[str, Any]) -> str | None:
    """Select a named text input, or the sole scalar text argument."""
    keys = ["command", "query", "input", "text", "prompt", "code"]
    if len(arguments) == 1:
        keys.extend(arguments)
    for key in keys:
        if isinstance(arguments.get(key), str):
            return "/" + key.replace("~", "~0").replace("/", "~1")
    return None


def _get_subagent_id(value: str | None) -> str | None:
    """Fit native subagent identity storage while retaining raw IDs in metadata."""
    if value is not None and len(value) > 255:
        return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()
    return value


def _apply_final_cost_fallback(root: ImportedNode, trajectory: dict[str, Any]) -> None:
    """Use a reported total only when it cannot duplicate known execution costs."""
    total_cost = (trajectory.get("final_metrics") or {}).get("total_cost_usd")
    if total_cost is None:
        return
    documents = [trajectory]
    while documents:
        document = documents.pop()
        if any(step.get("is_copied_context") is True for step in document["steps"]):
            return
        documents.extend(document.get("subagent_trajectories") or [])
    nodes = [root]
    while nodes:
        node = nodes.pop()
        if node.cost is not None:
            return
        nodes.extend(node.children)
    root.cost = Decimal(str(total_cost))
    root.metadata["normalization"] = {
        "cost": "final_metrics fallback, no per-step costs reported"
    }


def _build_trajectory_node(trajectory: dict[str, Any], key: str) -> ImportedNode:
    agent = trajectory["agent"]
    root = ImportedNode(
        node_type=NodeType.SPAN,
        name=agent["name"],
        external_id=key,
        status=NodeStatus.COMPLETED,
        inputs=None,
        outputs=None,
        attributes={},
        metadata={"atif": _get_document_metadata(trajectory)},
    )
    embedded = {
        child["trajectory_id"]: child
        for child in trajectory.get("subagent_trajectories") or []
    }
    attached: set[str] = set()
    previous: dict[str, Any] | None = None
    for step in trajectory["steps"]:
        step_key = f"{key}/step/{step['step_id']}"
        count = step.get("llm_call_count")
        copied = step.get("is_copied_context") is True
        is_model = step["source"] == "agent" and count in (None, 1) and not copied
        raw_metrics = step.get("metrics") or {}
        tokens = {
            target: raw_metrics[source]
            for source, target in _TOKEN_FIELDS.items()
            if raw_metrics.get(source) is not None
        }
        reasoning_tokens = (raw_metrics.get("extra") or {}).get(
            "reasoning_output_tokens"
        )
        if reasoning_tokens is not None:
            tokens["reasoning_tokens"] = reasoning_tokens
        metadata = {
            "atif": {
                field: value
                for field, value in step.items()
                if field not in {"message", "tool_calls", "observation"}
            }
        }
        if step["source"] == "agent" and count is None:
            metadata["normalization"] = {
                "llm_call_count": "unreported; displayed as one model step"
            }
        if copied:
            metadata["normalization"] = {
                "execution": "copied context; not counted as new execution"
            }
        inputs = (
            {
                "previous_step_id": previous["step_id"],
                "message": previous["message"],
                **(
                    {"observation": previous["observation"]}
                    if previous.get("observation") is not None
                    else {}
                ),
            }
            if previous is not None and step["source"] == "agent"
            else None
        )
        outputs: dict[str, Any] = {"message": step["message"]}
        if step.get("tool_calls") is not None:
            outputs["tool_calls"] = step["tool_calls"]
        observation = step.get("observation") or {}
        results = observation.get("results") or []
        grouped_results: dict[str | None, list[dict[str, Any]]] = {}
        for result in results:
            grouped_results.setdefault(result.get("source_call_id"), []).append(result)
        unbound = grouped_results.get(None, [])
        if unbound:
            outputs["observation"] = {"results": unbound}
        observation_metadata = {
            field: value for field, value in observation.items() if field != "results"
        }
        if observation_metadata:
            metadata["observation"] = observation_metadata
        node = ImportedNode(
            external_id=step_key,
            node_type=NodeType.LLM_CALL if is_model else NodeType.SPAN,
            name=f"{step['source']} step {step['step_id']}",
            status=NodeStatus.COMPLETED,
            started_at=_get_timestamp(step.get("timestamp")),
            inputs=inputs,
            input_text_selector=_get_text_selector(inputs["message"], "/message")
            if inputs
            else None,
            system_prompt_selector=(
                _get_text_selector(previous["message"], "/message")
                if inputs and previous and previous["source"] == "system"
                else None
            ),
            outputs=outputs,
            output_text_selector=_get_text_selector(step["message"], "/message"),
            reasoning=step.get("reasoning_content"),
            model=(step.get("model_name") or agent.get("model_name"))
            if step["source"] == "agent" and count != 0
            else None,
            tokens=TokenUsage(**tokens) if tokens and not copied else None,
            cost=Decimal(str(raw_metrics["cost_usd"]))
            if raw_metrics.get("cost_usd") is not None and not copied
            else None,
            attributes={},
            metadata=metadata,
        )
        tool_nodes: dict[str, ImportedNode] = {}
        for call in step.get("tool_calls") or []:
            matching = grouped_results.get(call["tool_call_id"], [])
            failed = any(
                (result.get("extra") or {}).get("tool_result_is_error") is True
                for result in matching
            )
            tool = ImportedNode(
                external_id=f"{step_key}/tool/{quote(call['tool_call_id'], safe='')}",
                node_type=NodeType.SPAN if copied else NodeType.TOOL_CALL,
                name=call["function_name"],
                tool_name=call["function_name"],
                status=NodeStatus.FAILED
                if failed
                else NodeStatus.COMPLETED
                if matching
                else NodeStatus.IN_PROGRESS,
                error="ATIF observation reports tool_result_is_error"
                if failed
                else None,
                inputs=call["arguments"],
                input_text_selector=_get_argument_selector(call["arguments"]),
                outputs={"results": matching},
                output_text_selector=next(
                    (
                        selector
                        for index, result in enumerate(matching)
                        if (
                            selector := _get_text_selector(
                                result.get("content"), f"/results/{index}/content"
                            )
                        )
                        is not None
                    ),
                    None,
                ),
                attributes={},
                metadata={
                    "atif": {
                        field: value
                        for field, value in call.items()
                        if field != "arguments"
                    },
                    "status_source": "tool_result_is_error"
                    if failed
                    else "observation_present"
                    if matching
                    else "missing_observation",
                    "is_copied_context": copied,
                },
            )
            node.children.append(tool)
            tool_nodes[call["tool_call_id"]] = tool
        for result_index, result in enumerate(results):
            parent = tool_nodes.get(result.get("source_call_id"), node)
            for ref_index, ref in enumerate(
                result.get("subagent_trajectory_ref") or []
            ):
                target_id = ref.get("trajectory_id")
                target = (
                    embedded.get(target_id) if not ref.get("trajectory_path") else None
                )
                subagent = ImportedNode(
                    external_id=f"{step_key}/observation/{result_index}/subagent/{ref_index}",
                    node_type=NodeType.SPAN if copied else NodeType.SUBAGENT_CALL,
                    name=target["agent"]["name"] if target else "Referenced subagent",
                    subagent_id=_get_subagent_id(target_id or ref.get("session_id")),
                    status=NodeStatus.COMPLETED,
                    inputs=None,
                    outputs=None,
                    attributes={},
                    metadata={"atif": ref, "resolution": "external_or_unresolved"},
                )
                if copied:
                    subagent.metadata["resolution"] = "copied_context"
                elif target is not None and target_id not in attached:
                    attached.add(target_id)
                    subagent.metadata["resolution"] = "embedded"
                    subagent.children.append(
                        _build_trajectory_node(
                            target, f"{key}/trajectory/{quote(target_id, safe='')}"
                        )
                    )
                elif target is not None and target_id in attached:
                    subagent.metadata["resolution"] = "already_imported"
                parent.children.append(subagent)
        root.children.append(node)
        previous = step
    unreferenced = [
        child for child_id, child in embedded.items() if child_id not in attached
    ]
    if unreferenced:
        root.metadata["unreferenced_subagent_trajectories"] = unreferenced
    _apply_final_cost_fallback(root, trajectory)
    return root


def _get_external_id(
    trajectory: dict[str, Any], source_id: str | None, namespace: str | None
) -> str:
    if source_id is not None:
        identity = source_id
    elif trajectory.get("trajectory_id"):
        identity = trajectory["trajectory_id"]
    else:
        canonical = json.dumps(
            trajectory,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        identity = f"{trajectory.get('session_id') or 'atif'}:sha256:{digest}"
    # Encode both components, so separators in a namespace cannot cause collisions.
    return (
        json.dumps([namespace, identity], ensure_ascii=True, separators=(",", ":"))
        if namespace is not None
        else identity
    )


def _build_session(record: Any, namespace: str | None) -> ImportedSession:
    value = _get_object(record, "record")
    source_id = None
    harbor = None
    if "trajectory" in value:
        unknown = value.keys() - {"trajectory", "source_id", "harbor_result"}
        if unknown:
            raise ValueError("trajectory envelope contains unsupported fields")
        if value.get("source_id") is not None:
            source_id = _get_string(value["source_id"], "source_id")
        if value.get("harbor_result") is not None:
            harbor = _get_object(value["harbor_result"], "harbor_result")
        value = value["trajectory"]
    trajectory = _validate_trajectory(value)
    external_id = _get_external_id(trajectory, source_id, namespace)
    root = _build_trajectory_node(trajectory, external_id)
    messages = [
        {
            "step_id": step["step_id"],
            "source": step["source"],
            "message": step["message"],
        }
        for step in trajectory["steps"]
        if step["source"] in {"user", "system"}
    ]
    input_selector = next(
        (
            selector
            for index, message in enumerate(messages)
            if message["source"] == "user"
            and (
                selector := _get_text_selector(
                    message["message"], f"/messages/{index}/message"
                )
            )
            is not None
        ),
        None,
    )
    last_agent = next(
        (step for step in reversed(trajectory["steps"]) if step["source"] == "agent"),
        None,
    )
    outputs = {"message": last_agent["message"]} if last_agent else None
    metadata: dict[str, Any] = {
        "atif": _get_document_metadata(trajectory),
        "normalization": {
            "status": (
                "archived trajectory treated as completed unless Harbor "
                "reports an exception"
            ),
            "model_inputs": (
                "immediately preceding source step only; complete provider "
                "prompts are not reconstructed"
            ),
            "metrics": (
                "per-step reported usage; final cost used only without descendant "
                "costs or copied context; all final_metrics retained as metadata"
            ),
            "timestamps": (
                "aware step timestamps are event starts; naive values retained "
                "in metadata; session times from Harbor agent_execution only"
            ),
            "media": "references preserved without opening files or URLs",
            "copied_context": (
                "historical steps remain spans with raw usage in metadata; "
                "no new model, tool, or subagent execution is counted"
            ),
        },
    }
    if "normalization" in root.metadata:
        metadata["normalization"].update(root.metadata["normalization"])
    if source_id is not None:
        metadata["source_id"] = source_id
    if namespace is not None:
        metadata["namespace"] = namespace
    error = None
    started_at = ended_at = None
    if harbor is not None:
        metadata["harbor_result"] = harbor
        if harbor.get("agent_execution") is not None:
            execution = _get_object(harbor["agent_execution"], "agent_execution")
            started_at = _get_timestamp(execution.get("started_at"))
            ended_at = _get_timestamp(execution.get("finished_at"))
            if (
                started_at is not None
                and ended_at is not None
                and ended_at < started_at
            ):
                raise ValueError("agent_execution finished_at precedes started_at")
        exception = harbor.get("exception_info")
        if exception is not None:
            exception = _get_object(exception, "harbor_result.exception_info")
            error = (
                exception.get("exception_message")
                or exception.get("exception_type")
                or "Harbor reported an exception"
            )
            if not isinstance(error, str):
                raise ValueError("Harbor exception message/type must be a string")
    root.status = NodeStatus.FAILED if error else NodeStatus.COMPLETED
    root.error = error
    root.started_at = started_at
    root.ended_at = ended_at
    return ImportedSession(
        external_id=external_id,
        name=trajectory["agent"]["name"],
        status=SessionStatus.FAILED if error else SessionStatus.COMPLETED,
        error=error,
        started_at=started_at,
        ended_at=ended_at,
        framework="harbor" if harbor is not None else None,
        inputs={"messages": messages},
        input_text_selector=input_selector,
        outputs=outputs,
        output_text_selector=_get_text_selector(last_agent["message"], "/message")
        if last_agent
        else None,
        metadata=metadata,
        nodes=[root],
    )


def parse(
    payload: bytes, params: dict[str, Any]
) -> Iterator[ImportedSession | ImportFailure]:
    """Parse one ATIF JSON document or a ``trajectories`` batch envelope.

    Args:
        payload: UTF-8 JSON bytes. Batch entries may be ATIF documents or objects
            with trajectory, optional harbor_result, and optional stable source_id.
        params: Optional namespace string used to scope source identities.

    Yields:
        One session per document, or an isolated failure for each invalid record.
    """
    try:
        _get_object(params, "params")
        if params.keys() - {"namespace"}:
            raise ValueError("only the namespace parameter is supported")
        namespace = params.get("namespace")
        if "namespace" in params:
            namespace = _get_string(namespace, "namespace")
        decoded = json.loads(payload.decode("utf-8"))
        top = _get_object(decoded, "payload")
        if "trajectories" in top:
            if top.keys() - {"trajectories"}:
                raise ValueError("batch envelope contains unsupported fields")
            records = _get_list(top["trajectories"], "trajectories")
            if not records:
                raise ValueError("trajectories cannot be empty")
        else:
            records = [top]
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        yield ImportFailure(line=1, error=f"Invalid ATIF payload: {exc}")
        return
    for line, record in enumerate(records, 1):
        external_id = None
        try:
            # Reject NaN/Infinity even in opaque extras and Harbor evidence.
            json.dumps(record, allow_nan=False, ensure_ascii=False).encode("utf-8")
            session = _build_session(record, namespace)
            external_id = session.external_id
            SessionCreateRequest.model_validate(
                session.model_dump(exclude={"nodes"})
                | {"origin": "imported", "imported_from": "atif"}
            ).model_dump_json()
            for request in flatten_nodes(session.nodes):
                request.model_dump_json()
            session.model_dump_json()
            yield session
        except (
            ValueError,
            TypeError,
            OverflowError,
            RecursionError,
            UnicodeError,
            SessionImportError,
            PydanticSerializationError,
        ) as exc:
            yield ImportFailure(
                line=line,
                external_id=external_id,
                error=f"Invalid ATIF trajectory: {exc}".encode(
                    "utf-8", errors="backslashreplace"
                ).decode("utf-8"),
            )
