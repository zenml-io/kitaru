"""Import production traces, investigate a case, and replay a candidate agent.

This file uses the case-first replay primitives that are being added to the
Kitaru SDK. Trace generation lives in ``trace_fixtures/`` and is not part of
the user journey shown here.
"""

import json
from typing import Any

import click
from evals.register import AGENT_NAME, AGENT_VERSION, kagent

from kitaru import KitaruClient

DEFAULT_FILTER = 'metadata.intent == "permissions"'
DEFAULT_AT = "support_agent_model_request"
DEFAULT_EXPERIMENT = "support-agent-permissions-v2"
SOURCE_VERSION = "v2.2"


def _json(value: Any) -> str:
    """Serialize an SDK result for the tutorial CLI."""
    if hasattr(value, "to_json"):
        value = value.to_json()
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _import_traces(source: str, *, trace_format: str | None, name: str | None) -> Any:
    """Import one production trace or a batch export as Kitaru executions."""
    kwargs: dict[str, Any] = {
        "agent": AGENT_NAME,
        "version": SOURCE_VERSION,
    }
    if trace_format is not None:
        kwargs["format"] = trace_format
    if name is not None:
        kwargs["name"] = name
    client = KitaruClient()
    return client.executions.import_traces(  # ty: ignore[unresolved-attribute]
        source, **kwargs
    )


def _find_cases(where: str) -> list[Any]:
    """Resolve a production filter to the executions under investigation."""
    client = KitaruClient()
    return client.executions.list(  # type: ignore[call-arg]
        agent=AGENT_NAME,  # ty: ignore[unknown-argument]
        where=where,  # ty: ignore[unknown-argument]
    )


def _replay_cases(
    execution_ids: list[str],
    *,
    name: str,
    at: str,
    idempotency_key: str,
) -> Any:
    """Replay native executions as one durable registered-agent experiment."""
    return kagent.replay(
        execution_ids,
        at=at,
        on_error="collect",
        uncovered_policy="fail",
        idempotency_key=idempotency_key,
        repeats=3,
        wait=False,
        name=name,
    )


@click.group()
def cli() -> None:
    """Case-first PydanticAI replay example."""


@cli.command("register")
def register_cmd() -> None:
    """Register the wrapped agent once, without executing it."""
    result = kagent.register(  # ty: ignore[unresolved-attribute]
        version=AGENT_VERSION
    )
    click.echo(_json(result))


@cli.command("import-traces")
@click.argument("source")
@click.option("--format", "trace_format")
@click.option("--name")
def import_traces_cmd(
    source: str,
    trace_format: str | None,
    name: str | None,
) -> None:
    """Import one reported trace or a production export."""
    click.echo(_json(_import_traces(source, trace_format=trace_format, name=name)))


@cli.command("find")
@click.option("--where", default=DEFAULT_FILTER, show_default=True)
def find_cmd(where: str) -> None:
    """Find imported executions that match the failure signal."""
    executions = _find_cases(where)
    click.echo(_json(executions))


@cli.command("replay")
@click.argument("exec_id")
@click.option("--at", default=DEFAULT_AT, show_default=True)
@click.option("--idempotency-key", required=True)
def replay_cmd(exec_id: str, at: str, idempotency_key: str) -> None:
    """Replay one native execution as a durable experiment."""
    click.echo(
        _json(
            _replay_cases(
                [exec_id],
                name=f"case-{exec_id}",
                at=at,
                idempotency_key=idempotency_key,
            )
        )
    )


@cli.command("experiment")
@click.argument("exec_ids", nargs=-1, required=True)
@click.option("--name", default=DEFAULT_EXPERIMENT, show_default=True)
@click.option("--at", default=DEFAULT_AT, show_default=True)
@click.option("--idempotency-key", required=True)
def experiment_cmd(
    exec_ids: tuple[str, ...],
    name: str,
    at: str,
    idempotency_key: str,
) -> None:
    """Replay explicit native executions as one named experiment."""
    click.echo(
        _json(
            _replay_cases(
                list(exec_ids),
                name=name,
                at=at,
                idempotency_key=idempotency_key,
            )
        )
    )


if __name__ == "__main__":
    cli()
