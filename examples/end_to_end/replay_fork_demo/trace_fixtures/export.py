"""Export Langfuse traces and derive the replay-ready example fixture."""

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import click
from langfuse import get_client
from reference_agent.config import IMPORTED_SOURCE_VERSION

FIXTURE_CONTRACT_REVISION = "pydantic-ai-final-generation-v1"
OBSERVATION_FIELDS = "basic,time,io,metadata,model,usage,prompt,metrics,trace_context"
METADATA_FIELDS = (
    "fixture_generation_id,variant,agent_version,case_id,scenario_id,intent"
)


def _json_value(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _observation_payload(observation: Any) -> dict[str, Any]:
    payload = observation.model_dump(mode="json", by_alias=True)
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        payload["metadata"] = {
            key: value
            for key, value in metadata.items()
            if not key.lower().endswith("public_key")
        }
    return payload


def _fetch_trace(trace_id: str) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        response = get_client().api.observations.get_many(
            trace_id=trace_id,
            limit=100,
            cursor=cursor,
            fields=OBSERVATION_FIELDS,
            expand_metadata=METADATA_FIELDS,
        )
        observations.extend(_observation_payload(item) for item in response.data)
        cursor = response.meta.cursor
        if cursor is None:
            break
    if not observations:
        raise click.ClickException(f"No observations found for trace {trace_id}.")
    return sorted(observations, key=lambda item: (item["startTime"], item["id"]))


def _text_output(observation: dict[str, Any]) -> str:
    output = _json_value(observation.get("output"))
    if not isinstance(output, list) or len(output) != 1:
        raise click.ClickException(
            f"Generation {observation['id']} does not have one output message."
        )
    parts = output[0].get("parts")
    if not isinstance(parts, list) or len(parts) != 1:
        raise click.ClickException(
            f"Generation {observation['id']} does not have one output part."
        )
    text = parts[0].get("content")
    if parts[0].get("type") != "text" or not isinstance(text, str):
        raise click.ClickException(
            f"Generation {observation['id']} does not end with text output."
        )
    return text


def _convert_message(message: dict[str, Any]) -> list[dict[str, Any]]:
    role = message.get("role")
    if role == "system":
        return []

    parts = message.get("parts")
    if not isinstance(parts, list) or not parts:
        raise click.ClickException(f"Unsupported PydanticAI message: {message!r}")

    converted: list[dict[str, Any]] = []
    for part in parts:
        part_type = part.get("type")
        if part_type == "text":
            converted.append({"role": role, "content": part["content"]})
        elif part_type == "tool_call":
            arguments = _json_value(part["arguments"])
            converted.append(
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": part["id"],
                            "type": "function",
                            "function": {
                                "name": part["name"],
                                "arguments": arguments,
                            },
                        }
                    ],
                }
            )
        elif part_type == "tool_call_response":
            converted.append(
                {
                    "role": "tool",
                    "tool_call_id": part["id"],
                    "name": part["name"],
                    "content": part["result"],
                }
            )
        else:
            raise click.ClickException(f"Unsupported PydanticAI part: {part!r}")
    return converted


def _derive_replay_row(observations: list[dict[str, Any]]) -> dict[str, Any]:
    roots = [item for item in observations if item.get("name") == "support-agent"]
    if len(roots) != 1:
        raise click.ClickException("Expected one support-agent root span per trace.")
    root = roots[0]

    generations: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
    for item in observations:
        if item.get("type") != "GENERATION":
            continue
        generation_input = _json_value(item.get("input"))
        if not isinstance(generation_input, dict):
            continue
        messages = generation_input.get("messages")
        if isinstance(messages, list):
            generations.append((len(messages), item, generation_input))
    if not generations:
        raise click.ClickException("Trace has no model generation with messages.")
    _, final_generation, generation_input = max(
        generations, key=lambda item: (item[0], item[1]["startTime"])
    )

    messages: list[dict[str, Any]] = []
    for message in generation_input["messages"]:
        messages.extend(_convert_message(message))
    final_text = _text_output(final_generation)
    output = json.loads(final_text)
    messages.append(
        {
            "role": "assistant",
            "content": json.dumps(output, separators=(",", ":")),
        }
    )

    metadata = {
        key: value
        for key, value in root.get("metadata", {}).items()
        if key in METADATA_FIELDS.split(",")
    }
    metadata["fixture_contract_revision"] = FIXTURE_CONTRACT_REVISION
    return {
        "id": f"{final_generation['id']}-replay",
        "traceId": root["traceId"],
        "type": "AGENT",
        "name": "support-agent",
        "startTime": root["startTime"],
        "endTime": final_generation["endTime"],
        "traceVersion": IMPORTED_SOURCE_VERSION,
        "input": {
            "messages": messages,
            "tools": generation_input["tools"],
        },
        "output": output,
        "metadata": metadata,
    }


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.write_text(
        "".join(f"{json.dumps(row, separators=(',', ':'))}\n" for row in rows),
        encoding="utf-8",
    )


@click.command()
@click.option("--trace-id", "trace_ids", multiple=True, required=True)
@click.option(
    "--raw-output",
    type=click.Path(path_type=Path),
    default=Path("trace_fixtures/raw-imported-support-cases.jsonl"),
    show_default=True,
)
@click.option(
    "--replay-output",
    type=click.Path(path_type=Path),
    default=Path("trace_fixtures/imported-support-cases.jsonl"),
    show_default=True,
)
def cli(trace_ids: tuple[str, ...], raw_output: Path, replay_output: Path) -> None:
    """Export TRACE_ID observations and build the replay-ready fixture."""
    traces = [_fetch_trace(trace_id) for trace_id in trace_ids]
    _write_jsonl(raw_output, (row for trace in traces for row in trace))
    _write_jsonl(replay_output, map(_derive_replay_row, traces))
    click.echo(f"raw_observations={sum(map(len, traces))} path={raw_output}")
    click.echo(f"replay_traces={len(traces)} path={replay_output}")


if __name__ == "__main__":
    cli()
