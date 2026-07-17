"""Import production traces, investigate a case, and replay a candidate agent.

This file uses the case-first replay primitives that are being added to the
Kitaru SDK. Trace generation lives in ``trace_fixtures/`` and is not part of
the user journey shown here.
"""

import json
from typing import Any

import click
from evals.register import AGENT_NAME, AGENT_VERSION, kagent
from evals.scorers import avoided_restricted_setting_write

from kitaru import KitaruClient

DEFAULT_FILTER = 'metadata.intent == "permissions"'
DEFAULT_EXPERIMENT = "support-agent-permissions-v2"
DEFAULT_SCORE_SWEEP = "permissions-safety-sweep"
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


def _replay_cases(executions: list[Any], *, name: str) -> Any:
    """Replay the selected recordings with the agent code in this checkout."""
    return kagent.replay(  # ty: ignore[unresolved-attribute]
        executions,
        repeats=3,
        tools={
            "*": "recorded",
            "update_customer_setting": "blocked",
        },
        scorers=[avoided_restricted_setting_write],
        name=name,
    )


def _score_cases(executions: list[Any], *, name: str) -> Any:
    """Score imported recordings without running the agent."""
    client = KitaruClient()
    return client.executions.evaluate(  # ty: ignore[unresolved-attribute]
        executions,
        scorers=[avoided_restricted_setting_write],
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


@cli.command("score")
@click.option("--where", default=DEFAULT_FILTER, show_default=True)
@click.option("--name", default=DEFAULT_SCORE_SWEEP, show_default=True)
def score_cmd(where: str, name: str) -> None:
    """Score imported recordings without re-executing the agent."""
    executions = _find_cases(where)
    if not executions:
        raise click.ClickException("The filter matched no imported executions.")
    click.echo(_json(_score_cases(executions, name=name)))


@cli.command("replay")
@click.argument("exec_id")
def replay_cmd(exec_id: str) -> None:
    """Open one investigation by replaying its imported execution."""
    execution = KitaruClient().executions.get(exec_id)
    click.echo(_json(_replay_cases([execution], name=f"case-{exec_id}")))


@cli.command("experiment")
@click.option("--where", default=DEFAULT_FILTER, show_default=True)
@click.option("--name", default=DEFAULT_EXPERIMENT, show_default=True)
def experiment_cmd(where: str, name: str) -> None:
    """Replay the full resolved cohort as one named experiment."""
    executions = _find_cases(where)
    if not executions:
        raise click.ClickException("The filter matched no imported executions.")
    click.echo(_json(_replay_cases(executions, name=name)))


if __name__ == "__main__":
    cli()
