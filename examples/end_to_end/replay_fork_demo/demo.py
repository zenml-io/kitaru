"""Import production traces and replay a candidate agent over native cases.

Trace generation lives in ``trace_fixtures/`` and is not part of the user
journey shown here.
"""

import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import click
from evals.register import AGENT_NAME, AGENT_VERSION, kagent

from kitaru import KitaruClient, RegressionLimits

DEFAULT_AT = "support_agent_model_request"
DEFAULT_EXPERIMENT = "support-agent-permissions-v2"


def _json(value: Any) -> str:
    """Serialize an SDK result for the tutorial CLI."""
    if hasattr(value, "to_json"):
        value = value.to_json()
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    if is_dataclass(value) and not isinstance(value, type):
        value = asdict(value)
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _import_traces(
    path: Path,
    *,
    source_project_id: str,
    trace_ids: list[str],
    dry_run: bool,
) -> Any:
    """Plan or execute a Langfuse JSONL import as Kitaru executions."""
    client = KitaruClient()
    return client.imports.langfuse(
        str(path),
        source_project_id=source_project_id,
        agent_name=AGENT_NAME,
        trace_ids=trace_ids or None,
        dry_run=dry_run,
        confirm_data_storage=not dry_run,
    )


def _replay_cases(
    execution_ids: list[str],
    *,
    name: str,
    at: str,
    idempotency_key: str,
) -> Any:
    """Replay native executions as one durable registered-agent experiment."""
    # Each CLI command runs in a fresh process, so bind this agent instance to
    # the durable registration before submitting the replay.
    kagent.register(label=AGENT_VERSION)
    return kagent.replay(
        execution_ids,
        at=at,
        on_error="collect",
        uncovered_policy="fail",
        idempotency_key=idempotency_key,
        repeats=3,
        wait=True,
        name=name,
        suite_key=name,
    )


def _rerun_suite(
    suite: str,
    *,
    idempotency_key: str,
    limits: RegressionLimits,
) -> Any:
    """Rerun a protected suite against the registered candidate and assert PASS."""
    kagent.register(label=AGENT_VERSION)
    result = kagent.replay(
        experiment=suite,
        idempotency_key=idempotency_key,
        repeats=1,
        limits=limits,
    )
    result.assert_pass()
    return result


@click.group()
def cli() -> None:
    """Case-first PydanticAI replay example."""


@cli.command("register")
def register_cmd() -> None:
    """Register the wrapped agent once, without executing it."""
    result = kagent.register(label=AGENT_VERSION)
    click.echo(_json(result))


@cli.command("import-traces")
@click.argument("path", type=click.Path(path_type=Path, dir_okay=False))
@click.option("--source-project-id", required=True)
@click.option("--trace-id", "trace_ids", multiple=True)
@click.option(
    "--commit",
    is_flag=True,
    help="Persist imported observations. The default is a read-only dry run.",
)
def import_traces_cmd(
    path: Path,
    source_project_id: str,
    trace_ids: tuple[str, ...],
    commit: bool,
) -> None:
    """Plan or import traces from a Langfuse observations JSONL export."""
    click.echo(
        _json(
            _import_traces(
                path,
                source_project_id=source_project_id,
                trace_ids=list(trace_ids),
                dry_run=not commit,
            )
        )
    )


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


@cli.command("rerun")
@click.argument("suite")
@click.option("--idempotency-key", required=True)
@click.option("--max-trials", type=int, default=3, show_default=True)
@click.option("--max-cost-usd", type=float, default=1.0, show_default=True)
@click.option("--max-incurred-tokens", type=int, default=100_000, show_default=True)
@click.option("--max-duration-seconds", type=float, default=300.0, show_default=True)
def rerun_cmd(
    suite: str,
    idempotency_key: str,
    max_trials: int,
    max_cost_usd: float,
    max_incurred_tokens: int,
    max_duration_seconds: float,
) -> None:
    """Rerun a named protected suite as a bounded regression gate."""
    click.echo(
        _json(
            _rerun_suite(
                suite,
                idempotency_key=idempotency_key,
                limits=RegressionLimits(
                    max_trials=max_trials,
                    max_cost_usd=max_cost_usd,
                    max_incurred_tokens=max_incurred_tokens,
                    max_duration_seconds=max_duration_seconds,
                ),
            )
        )
    )


if __name__ == "__main__":
    cli()
