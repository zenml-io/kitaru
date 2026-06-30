"""Replay & fork a recorded LangGraph run — pick a step, or run the whole arc.

    set -a && . ./.env && set +a

    uv run python demo.py run-all
    uv run python demo.py create-trace
    uv run python demo.py import-trace langfuse:<id>

Offline rich fixture:

    export TRACE_ID=trace-replay-fork-rich-baseline
    export TRACE_FILE=reference_agent/fixtures/langfuse_rich_observations.jsonl
    uv run python demo.py import-trace "$TRACE_FILE" --trace-id "$TRACE_ID"
    uv run python demo.py replay "$TRACE_FILE" --trace-id "$TRACE_ID"
    uv run python demo.py fork "$TRACE_FILE" --trace-id "$TRACE_ID"

`utils` is the only domain-specific part (build the graph, rehydrate typed
state, generate a trace). A JSON-native agent wouldn't need it.
"""

import json
from pathlib import Path

import click
import utils

from kitaru.adapters.langgraph.replay import (
    KitaruAdapter,
    import_langgraph_trace,
    trace_ids_in_rows,
)

FORK_EDITS = {"model": "gpt-5-nano", "prompt_profile": "trimmed_permissions"}


def _agent() -> KitaruAdapter:
    return KitaruAdapter(utils.graph(), cut=utils.CUT, rehydrate=utils.rehydrate)


def _read_jsonl(path: str) -> list[dict]:
    rows: list[dict] = []
    for line_number, line in enumerate(
        Path(path).read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise click.ClickException(
                f"Invalid JSON in {path} on line {line_number}: {error.msg}"
            ) from error
        if not isinstance(row, dict):
            raise click.ClickException(
                f"Invalid JSONL row in {path} on line {line_number}: "
                "expected an object."
            )
        rows.append(row)
    if not rows:
        raise click.ClickException(f"No JSONL rows found in {path}.")
    return rows


def _select_trace_id(rows: list[dict], trace_id: str | None, *, ref: str) -> str:
    available = trace_ids_in_rows(rows)
    if trace_id is not None:
        if trace_id not in available:
            choices = ", ".join(available) or "(none)"
            raise click.ClickException(
                f"Trace id {trace_id!r} was not found in {ref}. "
                f"Available trace ids: {choices}."
            )
        return trace_id
    if len(available) == 1:
        return available[0]
    if len(available) > 1:
        choices = "\n  ".join(available)
        raise click.ClickException(
            f"{ref} contains {len(available)} traces. Select one with --trace-id.\n"
            f"Available trace ids:\n  {choices}\n"
            f"Example: python demo.py import-trace {ref} --trace-id {available[0]}"
        )
    raise click.ClickException(
        f"JSONL rows in {ref} do not contain Langfuse observation trace IDs. "
        "This demo needs observation export rows, not top-level case summaries."
    )


def _load(ref: str, *, trace_id: str | None = None):
    if ref.startswith("langfuse:"):
        return import_langgraph_trace(ref, trace_id=trace_id)
    rows = _read_jsonl(ref)
    selected_trace_id = _select_trace_id(rows, trace_id, ref=ref)
    return import_langgraph_trace(rows=rows, trace_id=selected_trace_id)


def _load_demo_case(ref: str, *, trace_id: str | None = None):
    case = _load(ref, trace_id=trace_id)
    summary = utils.summarize_case(case)
    try:
        utils.validate_case_for_demo(summary)
    except ValueError as error:
        click.echo(utils.format_summary(summary), err=True)
        raise click.ClickException(str(error)) from error
    click.echo(utils.format_summary(summary))
    return case


@click.group()
def cli() -> None:
    """Replay & fork demo for the bundled LangGraph reference agent."""


@cli.command("create-trace")
@click.option("--scenario", default=utils.SCENARIO, show_default=True)
@click.option("--variant", default=utils.VARIANT, show_default=True)
def create_trace(scenario: str, variant: str) -> None:
    """Run the agent once and trace it to Langfuse; print the trace id."""
    trace_id = utils.generate_trace(scenario, variant)
    click.echo(f"trace_id={trace_id}")
    click.echo(f"next:  python demo.py import-trace langfuse:{trace_id}")


@cli.command("import-trace")
@click.argument("ref")
@click.option("--trace-id", default=None, help="Trace ID to select from a JSONL file.")
def import_trace_cmd(ref: str, trace_id: str | None) -> None:
    """Import a recorded run as a Case.  REF = langfuse:<id> or a rows .jsonl."""
    case = _load_demo_case(ref, trace_id=trace_id)
    d = case.observed_output.get("decision", {})
    click.echo(
        f"{case.case_id}: {len(case.recorded_calls)} recorded calls; "
        f"observed risk_status={d.get('risk_status')} "
        f"required_action={d.get('required_action')}"
    )


@cli.command()
@click.argument("ref")
@click.option("--trace-id", default=None, help="Trace ID to select from a JSONL file.")
def replay(ref: str, trace_id: str | None) -> None:
    """Compare original trace → unchanged replay from the cut."""
    report = _agent().replay(_load_demo_case(ref, trace_id=trace_id)).vs_trace()
    click.echo(
        f"original trace → unchanged replay drift: {report.has_reproduction_drift}"
    )


@cli.command()
@click.argument("ref")
@click.option("--trace-id", default=None, help="Trace ID to select from a JSONL file.")
@click.option("--model", default=FORK_EDITS["model"], show_default=True)
@click.option(
    "--prompt-profile", default=FORK_EDITS["prompt_profile"], show_default=True
)
@click.option(
    "--html",
    "html_path",
    default="replay_vs_fork.html",
    show_default=True,
    help="write the comparison HTML here",
)
def fork(
    ref: str, trace_id: str | None, model: str, prompt_profile: str, html_path: str
) -> None:
    """Compare original trace → unchanged replay → edited fork."""
    agent, case = _agent(), _load_demo_case(ref, trace_id=trace_id)
    replay_run = agent.replay(case)
    edits = {"model": model, "prompt_profile": prompt_profile}
    fork_run = agent.fork(case, **edits).run()
    report = fork_run.diff(replay_run)
    click.echo(
        f"original trace → unchanged replay drift: {report.has_reproduction_drift}"
    )
    click.echo(f"unchanged replay → edited fork drift: {report.has_fork_drift}")
    click.echo(str(report))
    path = utils.write_report(
        html_path,
        case=case,
        replay_run=replay_run,
        fork_run=fork_run,
        report=report,
        edits=edits,
    )
    click.echo(f"html: {path}")


@cli.command("run-all")
@click.argument("ref", required=False)
@click.option("--scenario", default=utils.SCENARIO, show_default=True)
@click.option("--variant", default=utils.VARIANT, show_default=True)
@click.option("--trace-id", default=None, help="Trace ID to select from a JSONL file.")
def run_all(ref: str | None, scenario: str, variant: str, trace_id: str | None) -> None:
    """Import/generate a trace, then replay → fork → compare."""
    if ref is None:
        generated_trace_id = utils.generate_trace(scenario, variant)
        click.echo(f"generated trace_id={generated_trace_id}")
        case = _load_demo_case(f"langfuse:{generated_trace_id}")
    else:
        case = _load_demo_case(ref, trace_id=trace_id)
    agent = _agent()
    replay_run = agent.replay(case)
    fork_run = agent.fork(case, **FORK_EDITS).run()
    report = fork_run.diff(replay_run)
    click.echo(
        f"original trace → unchanged replay drift: {report.has_reproduction_drift}"
    )
    click.echo(f"unchanged replay → edited fork drift: {report.has_fork_drift}")
    click.echo(str(report))
    path = utils.write_report(
        "replay_vs_fork.html",
        case=case,
        replay_run=replay_run,
        fork_run=fork_run,
        report=report,
        edits=FORK_EDITS,
    )
    click.echo(f"html: {path}")


if __name__ == "__main__":
    cli()
