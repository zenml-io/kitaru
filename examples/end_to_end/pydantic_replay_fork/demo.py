"""PydanticAI support-copilot demo — run, rerun, replay, cohort (SDK).

    cd examples/end_to_end/pydantic_replay_fork
    uv run python demo.py run-all

Individual commands:

    uv run python demo.py run
    uv run python demo.py rerun <EXEC-ID>
    uv run python demo.py replay <EXEC-ID>
    uv run python demo.py cohort

This is the SDK story. The same rerun/replay are also available straight from
the Kitaru CLI (`kitaru executions replay --from decide …`) — see the README.
"""
from __future__ import annotations

import click

from agent import SupportDecision
from cohort import cohort
from comparison_html import write as write_html
from support_copilot import KitaruAdapterPA
from utils import CUT, Recipe, cost, diff_decisions, latency, quality_judge


SCENARIO = (
    "I need to grant all members of our engineering team admin access to the "
    "production SSO settings so they can self-service identity provider changes "
    "without going through IT. Can you enable that for our account?"
)
CUSTOMER = "acme-corp / alice@acme.example"

BASELINE_MODEL = "openai:gpt-5-mini"
FORK_MODEL = "openai:gpt-5-nano"
FORK_PROMPT_PROFILE = "trimmed_permissions"

_FLOW_NODES = ("gather_context", "decide", "finalize")


def _decision_summary(decision: dict) -> str:
    return (
        f"risk={decision.get('risk_status', '?')}  "
        f"action={decision.get('required_action', '?')}  "
        f"label={decision.get('policy_label', '?')}"
    )


def _outcome_rows(rerun_dec: dict, replay_dec: dict) -> list[tuple[str, object, object, bool]]:
    fields = list(SupportDecision.model_fields.keys())
    return [(f, rerun_dec.get(f), replay_dec.get(f), rerun_dec.get(f) == replay_dec.get(f)) for f in fields]


def _write_compare(path, exec_id, model, prompt_profile, rerun_dec, replay_dec, report) -> str:
    return write_html(
        path,
        exec_id=exec_id,
        scenario=SCENARIO[:80] + "..." if len(SCENARIO) > 80 else SCENARIO,
        cut=CUT,
        nodes=_FLOW_NODES,
        settings_changes=[("model", BASELINE_MODEL, model), ("prompt_profile", "baseline", prompt_profile)],
        outcomes=_outcome_rows(rerun_dec, replay_dec),
        has_drift=report.has_fork_drift,
        rerun_summary=_decision_summary(rerun_dec),
        replay_summary=_decision_summary(replay_dec),
    )


@click.group()
def cli() -> None:
    """PydanticAI support-copilot demo — run, rerun, replay, cohort (Kitaru-wrapped)."""


@cli.command("run")
@click.option("--prompt", default=SCENARIO, show_default=False, help="Support request prompt.")
@click.option("--customer", default=CUSTOMER, show_default=True, help="Customer identifier.")
def run_cmd(prompt: str, customer: str) -> None:
    """Run the three-step flow; print exec_id and decision."""
    adapter = KitaruAdapterPA(model=BASELINE_MODEL)
    exec_id = adapter.run(prompt, customer)
    click.echo(f"exec_id={exec_id}")
    click.echo(_decision_summary(adapter.decision_of(exec_id)))


@cli.command("rerun")
@click.argument("exec_id")
def rerun_cmd(exec_id: str) -> None:
    """Reproduce EXEC_ID from the decide checkpoint, no edits (cached head, live tail)."""
    adapter = KitaruAdapterPA(model=BASELINE_MODEL)
    original = adapter.decision_of(exec_id)
    rerun = adapter.rerun(exec_id)
    click.echo(f"rerun_exec_id={rerun.exec_id}")
    drift = diff_decisions(original, rerun.decision)
    note = "decision changed" if drift.has_fork_drift else "faithfully reproduced"
    click.echo(f"rerun drift: {drift.has_fork_drift}  ({note})")


@cli.command("replay")
@click.argument("exec_id")
@click.option("--model", default=FORK_MODEL, show_default=True)
@click.option("--prompt-profile", default=FORK_PROMPT_PROFILE, show_default=True)
@click.option("--html", "html_path", default="replay_vs_rerun.html", show_default=True)
def replay_cmd(exec_id: str, model: str, prompt_profile: str, html_path: str) -> None:
    """Replay EXEC_ID with a reconfigured decide step; write the HTML comparison."""
    adapter = KitaruAdapterPA(model=BASELINE_MODEL)
    rerun = adapter.rerun(exec_id)
    replay = adapter.replay(exec_id, model=model, prompt_profile=prompt_profile)
    click.echo(f"replay_exec_id={replay.exec_id}")

    report = replay.diff(rerun)
    click.echo(f"{report}")
    path = _write_compare(html_path, exec_id, model, prompt_profile, rerun.decision, replay.decision, report)
    click.echo(f"html: {path}")


@cli.command("cohort")
@click.option("--model", default=FORK_MODEL, show_default=True)
@click.option("--prompt-profile", default=FORK_PROMPT_PROFILE, show_default=True)
@click.option("--n", default=10, show_default=True, help="Number of recent production runs.")
def cohort_cmd(model: str, prompt_profile: str, n: int) -> None:
    """Apply the same reconfiguration across the last N production runs."""
    adapter = KitaruAdapterPA(model=BASELINE_MODEL)
    variant = Recipe(model=model, prompt_profile=prompt_profile, at=CUT)
    report = cohort(adapter.last_executions(n)).experiment(
        adapter, variant=variant, metrics=[cost, latency, quality_judge], repeats=1
    )
    click.echo(report.summary())
    regs = report.regressions()
    click.echo(f"regressions: {[getattr(r, 'name', r) for r in regs]}" if regs else "no regressions")


@cli.command("run-all")
@click.option("--prompt", default=SCENARIO, show_default=False)
@click.option("--customer", default=CUSTOMER, show_default=True)
def run_all(prompt: str, customer: str) -> None:
    """Narrated arc: run → rerun → replay → compare + HTML → cohort."""
    adapter = KitaruAdapterPA(model=BASELINE_MODEL)

    click.secho("1) Your PydanticAI agent ran in production — every step is a durable checkpoint.", bold=True)
    exec_id = adapter.run(prompt, customer)
    original = adapter.decision_of(exec_id)
    click.echo(f"   exec_id={exec_id}")
    click.echo(f"   {_decision_summary(original)}")

    click.secho("2) Reproduce it from the decide step — cached head, live tail, no edits.", bold=True)
    rerun = adapter.rerun(exec_id)
    rerun_drift = diff_decisions(original, rerun.decision).has_fork_drift
    click.echo(f"   rerun drift: {rerun_drift}  "
               + ("(decision changed)" if rerun_drift else "(faithfully reproduced)"))

    click.secho(
        f"3) Replay with edits: {FORK_MODEL} + {FORK_PROMPT_PROFILE} — did the decision flip?",
        bold=True,
    )
    replay = adapter.replay(exec_id, model=FORK_MODEL, prompt_profile=FORK_PROMPT_PROFILE)
    report = replay.diff(rerun)
    click.echo(f"   replay_exec_id={replay.exec_id}")
    click.echo(f"   {report}")

    click.secho("4) Writing HTML comparison — rerun vs replay.", bold=True)
    path = _write_compare("replay_vs_rerun.html", exec_id, FORK_MODEL, FORK_PROMPT_PROFILE,
                          rerun.decision, replay.decision, report)
    click.echo(f"   html: {path}")

    click.secho("5) Cohort — apply the same change across recent production runs.", bold=True)
    variant = Recipe(model=FORK_MODEL, prompt_profile=FORK_PROMPT_PROFILE, at=CUT)
    cohort_report = cohort(adapter.last_executions(10)).experiment(
        adapter, variant=variant, metrics=[cost, latency, quality_judge], repeats=1
    )
    click.echo(cohort_report.summary())
    if cohort_report.improvement:
        click.echo("   verdict: improvement — cheaper, faster, quality not worse.")
    else:
        regs = [getattr(r, "name", r) for r in cohort_report.regressions()]
        click.echo(f"   verdict: not an improvement. regressions: {regs}")


if __name__ == "__main__":
    cli()
