"""Replay & fork a recorded LangGraph run — pick a step, or run the whole arc.

    set -a && . ./.env && set +a

    uv run python demo.py run-all                    # generate → import → replay → fork → compare
    uv run python demo.py create-trace               # run the agent → Langfuse, print a trace id
    uv run python demo.py import-trace langfuse:<id> # import a trace (or: import-trace obs.jsonl)
    uv run python demo.py replay obs.jsonl           # reproduce: cached head, live tail
    uv run python demo.py fork obs.jsonl             # fork it, compare to the replay, write HTML

`utils` is the only domain-specific part (build the graph, rehydrate typed
state, generate a trace). A JSON-native agent wouldn't need it.
"""
import json

import click

from kitaru.adapters.langgraph.replay import KitaruAdapter, import_langgraph_trace

import utils

FORK_EDITS = {"model": "gpt-5-nano", "prompt_profile": "trimmed_permissions"}


def _agent() -> KitaruAdapter:
    return KitaruAdapter(utils.graph(), cut=utils.CUT, rehydrate=utils.rehydrate)


def _load(ref: str):
    if ref.startswith("langfuse:"):
        return import_langgraph_trace(ref)
    return import_langgraph_trace(rows=[json.loads(l) for l in open(ref) if l.strip()])


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
def import_trace_cmd(ref: str) -> None:
    """Import a recorded run as a Case.  REF = langfuse:<id> or a rows .jsonl."""
    case = _load(ref)
    d = case.observed_output.get("decision", {})
    click.echo(f"{case.case_id}: {len(case.recorded_calls)} recorded calls; "
               f"observed risk_status={d.get('risk_status')} "
               f"required_action={d.get('required_action')}")


@cli.command()
@click.argument("ref")
def replay(ref: str) -> None:
    """Reproduce the run from the cut (cached head, live tail)."""
    report = _agent().replay(_load(ref)).vs_trace()
    click.echo(f"reproduction drift: {report.has_reproduction_drift}")


@cli.command()
@click.argument("ref")
@click.option("--model", default=FORK_EDITS["model"], show_default=True)
@click.option("--prompt-profile", default=FORK_EDITS["prompt_profile"], show_default=True)
@click.option("--html", "html_path", default="replay_vs_fork.html", show_default=True,
              help="write the comparison HTML here")
def fork(ref: str, model: str, prompt_profile: str, html_path: str) -> None:
    """Fork the run (model/prompt edit) and compare to the unchanged replay."""
    agent, case = _agent(), _load(ref)
    replay_run = agent.replay(case)
    edits = {"model": model, "prompt_profile": prompt_profile}
    fork_run = agent.fork(case, **edits).run()
    report = fork_run.diff(replay_run)
    click.echo(str(report))
    path = utils.write_report(html_path, case=case, replay_run=replay_run,
                              fork_run=fork_run, report=report, edits=edits)
    click.echo(f"html: {path}")


@cli.command("run-all")
@click.option("--scenario", default=utils.SCENARIO, show_default=True)
@click.option("--variant", default=utils.VARIANT, show_default=True)
def run_all(scenario: str, variant: str) -> None:
    """Generate a fresh trace, then import → replay → fork → compare."""
    # 1) A production run of your agent.
    click.secho("1) Your LangGraph agent ran in production and was traced to Langfuse.", bold=True)
    trace_id = utils.generate_trace(scenario, variant)
    click.echo(f"   → it produced trace {trace_id}  (scenario={scenario}, variant={variant})")

    # 2) Import that recorded run — no rewrite of your agent.
    click.secho("2) You import that recorded run as a forkable case.", bold=True)
    case = import_langgraph_trace(f"langfuse:{trace_id}")
    d = case.observed_output.get("decision", {})
    click.echo(f"   → {case.case_id}: {len(case.recorded_calls)} recorded calls; "
               f"it decided risk_status={d.get('risk_status')!r}, "
               f"required_action={d.get('required_action')!r}")

    agent = _agent()

    # 3) Replay it unchanged to verify we faithfully reproduce the run.
    click.secho("3) You replay it unchanged to verify the reproduction "
                "(cached head, live tail).", bold=True)
    replay_run = agent.replay(case)
    repro = replay_run.vs_trace()
    click.echo("   → reproduction drift: " + (
        "False — faithfully reproduced the recorded decision"
        if not repro.has_reproduction_drift else "True — the replay diverged from the trace"))

    # 4) Fork it to test a change before shipping.
    click.secho(f"4) You fork it to test a change before shipping: "
                f"{FORK_EDITS['model']} + {FORK_EDITS['prompt_profile']}, run forward from the cut.", bold=True)
    fork_run = agent.fork(case, **FORK_EDITS).run()

    # 5) Compare the fork against the unchanged replay.
    click.secho("5) You compare the fork against the unchanged replay — "
                "did the change move the decision?", bold=True)
    report = fork_run.diff(replay_run)
    click.echo(f"   → {report}")
    path = utils.write_report("replay_vs_fork.html", case=case, replay_run=replay_run,
                              fork_run=fork_run, report=report, edits=FORK_EDITS)
    click.echo(f"   → report written to {path}")


if __name__ == "__main__":
    cli()
