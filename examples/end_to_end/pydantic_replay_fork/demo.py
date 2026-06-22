"""PydanticAI support-copilot demo — rerun, replay, cohort.

    cd examples/end_to_end/pydantic_replay_fork
    uv run python demo.py run-all

Individual commands:

    uv run python demo.py run
    uv run python demo.py rerun <EXEC-ID>
    uv run python demo.py replay <EXEC-ID>
    uv run python demo.py cohort
"""
from __future__ import annotations

import json
import subprocess

import click

try:
    from .support_copilot import KitaruAdapterPA
    from .utils import CUT, cost, latency, quality_judge, Recipe
    from .cohort import cohort
    from .comparison_html import write as write_html
    from .agent import SupportDecision as _SupportDecision
except ImportError:
    from support_copilot import KitaruAdapterPA  # type: ignore[no-redef]
    from utils import CUT, cost, latency, quality_judge, Recipe  # type: ignore[no-redef]
    from cohort import cohort  # type: ignore[no-redef]
    from comparison_html import write as write_html  # type: ignore[no-redef]
    from agent import SupportDecision as _SupportDecision  # type: ignore[no-redef]


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
    fields = list(_SupportDecision.model_fields.keys())
    return [(f, rerun_dec.get(f), replay_dec.get(f), rerun_dec.get(f) == replay_dec.get(f)) for f in fields]


@click.group()
def cli() -> None:
    """PydanticAI support-copilot demo — rerun, replay, cohort (Kitaru-wrapped)."""


@cli.command("run")
@click.option("--prompt", default=SCENARIO, show_default=False, help="Support request prompt.")
@click.option("--customer", default=CUSTOMER, show_default=True, help="Customer identifier.")
def run_cmd(prompt: str, customer: str) -> None:
    """Run the three-step flow; print exec_id and decision."""
    adapter = KitaruAdapterPA(model=BASELINE_MODEL)
    exec_id = adapter.run(prompt, customer)
    decision = adapter.decision_of(exec_id)
    click.echo(f"exec_id={exec_id}")
    click.echo(_decision_summary(decision))


@cli.command("rerun")
@click.argument("exec_id")
def rerun_cmd(exec_id: str) -> None:
    """Rerun EXEC_ID from the decide checkpoint via the Kitaru CLI.

    Shells out to ``kitaru executions replay --from decide <EXEC_ID> --output json``
    to demonstrate the CLI-native path, then diffs original vs rerun.
    """
    cmd = ["kitaru", "executions", "replay", "--from", CUT, exec_id, "--output", "json"]
    click.echo("  $ " + " ".join(cmd))

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        click.echo(f"kitaru CLI error: {result.stderr.strip()}", err=True)
        raise SystemExit(result.returncode)

    rerun_id = json.loads(result.stdout)["item"]["exec_id"]
    click.echo(f"rerun_exec_id={rerun_id}")

    adapter = KitaruAdapterPA(model=BASELINE_MODEL)
    rerun_handle = adapter.rerun(exec_id)
    drift = rerun_handle.diff(rerun_handle)
    click.echo(f"rerun drift: {drift.has_fork_drift}")


@cli.command("replay")
@click.argument("exec_id")
@click.option("--model", default=FORK_MODEL, show_default=True)
@click.option("--prompt-profile", default=FORK_PROMPT_PROFILE, show_default=True)
@click.option("--html", "html_path", default="replay_vs_rerun.html", show_default=True)
def replay_cmd(exec_id: str, model: str, prompt_profile: str, html_path: str) -> None:
    """Rerun baseline and replay with a reconfigured agent; write HTML comparison."""
    adapter = KitaruAdapterPA(model=BASELINE_MODEL)

    rerun_handle = adapter.rerun(exec_id)
    click.echo(f"rerun_exec_id={rerun_handle.exec_id}")

    replay_handle = adapter.replay(exec_id, model=model, prompt_profile=prompt_profile)
    click.echo(f"replay_exec_id={replay_handle.exec_id}")

    report = replay_handle.diff(rerun_handle)
    click.echo(str(report))

    path = write_html(
        html_path,
        exec_id=exec_id,
        scenario=SCENARIO[:80] + "..." if len(SCENARIO) > 80 else SCENARIO,
        cut=CUT,
        nodes=_FLOW_NODES,
        settings_changes=[("model", BASELINE_MODEL, model), ("prompt_profile", "baseline", prompt_profile)],
        outcomes=_outcome_rows(rerun_handle.decision, replay_handle.decision),
        has_drift=report.has_fork_drift,
        rerun_summary=_decision_summary(rerun_handle.decision),
        replay_summary=_decision_summary(replay_handle.decision),
    )
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
    if regs:
        click.echo(f"regressions: {[getattr(r, 'name', r) for r in regs]}")
    else:
        click.echo("no regressions")


@cli.command("run-all")
@click.option("--prompt", default=SCENARIO, show_default=False)
@click.option("--customer", default=CUSTOMER, show_default=True)
def run_all(prompt: str, customer: str) -> None:
    """Narrated arc: run → rerun (via CLI) → replay → compare + HTML → cohort."""

    click.secho(
        "1) Your PydanticAI agent ran in production — every step is a durable checkpoint.",
        bold=True,
    )
    adapter = KitaruAdapterPA(model=BASELINE_MODEL)
    exec_id = adapter.run(prompt, customer)
    decision = adapter.decision_of(exec_id)
    click.echo(f"   exec_id={exec_id}")
    click.echo(f"   {_decision_summary(decision)}")

    click.secho(
        "2) Rerun from the decide checkpoint via the Kitaru CLI (cached head, live tail).",
        bold=True,
    )
    cmd = ["kitaru", "executions", "replay", "--from", CUT, exec_id, "--output", "json"]
    click.echo("   $ " + " ".join(cmd))
    cli_result = subprocess.run(cmd, capture_output=True, text=True)
    if cli_result.returncode != 0:
        click.echo(f"   kitaru CLI error: {cli_result.stderr.strip()}", err=True)
        raise SystemExit(cli_result.returncode)
    cli_rerun_id = json.loads(cli_result.stdout)["item"]["exec_id"]
    click.echo(f"   rerun_exec_id={cli_rerun_id}")

    rerun_handle = adapter.rerun(exec_id)
    rerun_drift = rerun_handle.diff(rerun_handle)
    click.echo(f"   rerun drift: {rerun_drift.has_fork_drift}")

    click.secho(
        f"3) Replay with edits: {FORK_MODEL} + {FORK_PROMPT_PROFILE} — did the change flip the decision?",
        bold=True,
    )
    replay_handle = adapter.replay(exec_id, model=FORK_MODEL, prompt_profile=FORK_PROMPT_PROFILE)
    click.echo(f"   replay_exec_id={replay_handle.exec_id}")
    diff_report = replay_handle.diff(rerun_handle)
    click.echo(f"   {diff_report}")

    click.secho("4) Writing HTML comparison — rerun vs replay.", bold=True)
    html_path = write_html(
        "replay_vs_rerun.html",
        exec_id=exec_id,
        scenario=SCENARIO[:80] + "..." if len(SCENARIO) > 80 else SCENARIO,
        cut=CUT,
        nodes=_FLOW_NODES,
        settings_changes=[("model", BASELINE_MODEL, FORK_MODEL), ("prompt_profile", "baseline", FORK_PROMPT_PROFILE)],
        outcomes=_outcome_rows(rerun_handle.decision, replay_handle.decision),
        has_drift=diff_report.has_fork_drift,
        rerun_summary=_decision_summary(rerun_handle.decision),
        replay_summary=_decision_summary(replay_handle.decision),
    )
    click.echo(f"   html: {html_path}")

    click.secho("5) Cohort — apply the same change across recent production runs.", bold=True)
    variant = Recipe(model=FORK_MODEL, prompt_profile=FORK_PROMPT_PROFILE, at=CUT)
    cohort_report = cohort(adapter.last_executions(10)).experiment(
        adapter, variant=variant, metrics=[cost, latency, quality_judge], repeats=1
    )
    click.echo(cohort_report.summary())
    regs = cohort_report.regressions()
    if cohort_report.improvement:
        click.echo("   verdict: improvement — cheaper, faster, quality not worse.")
    else:
        click.echo(f"   verdict: not an improvement. regressions: {[getattr(r, 'name', r) for r in regs]}")


if __name__ == "__main__":
    cli()
