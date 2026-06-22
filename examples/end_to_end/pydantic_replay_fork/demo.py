"""PydanticAI support-copilot replay & experiment demo.

Run the narrated full arc:

    cd examples/end_to_end/pydantic_replay_fork
    uv run python demo.py run-all

Individual commands:

    uv run python demo.py run
    uv run python demo.py reproduce <EXEC-ID>
    uv run python demo.py experiment <EXEC-ID>
    uv run python demo.py cohort

Set KITARU_API_KEY (and OPENAI_API_KEY for real-model runs) in the environment.
Defaults use real model strings; for CI or local smoke-testing use --help only.
"""
from __future__ import annotations

import json
import subprocess

import click

# Support both direct-script execution (python demo.py from the folder) and
# package-level import (from pydantic_replay_fork import demo).
try:
    from .pipeline import CUT, KitaruAdapterPA
    from .comparison_html import write as write_html
    from .agent import SupportDecision as _SupportDecision
except ImportError:
    from pipeline import CUT, KitaruAdapterPA  # type: ignore[no-redef]
    from comparison_html import write as write_html  # type: ignore[no-redef]
    from agent import SupportDecision as _SupportDecision  # type: ignore[no-redef]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCENARIO = (
    "I need to grant all members of our engineering team admin access to the "
    "production SSO settings so they can self-service identity provider changes "
    "without going through IT. Can you enable that for our account?"
)
CUSTOMER = "acme-corp / alice@acme.example"

BASELINE_MODEL = "openai:gpt-5-mini"
FORK_MODEL = "openai:gpt-5-nano"
FORK_PROMPT_PROFILE = "trimmed_permissions"

#: Checkpoint names in the three-step flow (for the HTML execution diagram).
_FLOW_NODES = ("gather_context", "decide", "finalize")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _decision_summary(decision: dict) -> str:
    """One-line decision summary from a SupportDecision dict."""
    return (
        f"risk={decision.get('risk_status', '?')}  "
        f"action={decision.get('required_action', '?')}  "
        f"label={decision.get('policy_label', '?')}"
    )


def _build_outcomes(repro_dec: dict, exp_dec: dict) -> list[tuple[str, object, object, bool]]:
    """Build outcome rows for comparison_html.write."""
    fields = list(_SupportDecision.model_fields.keys())
    rows = []
    for field in fields:
        rv = repro_dec.get(field)
        ev = exp_dec.get(field)
        rows.append((field, rv, ev, rv == ev))
    return rows


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------

@click.group()
def cli() -> None:
    """PydanticAI support-copilot replay & experiment demo (Kitaru-wrapped)."""


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

@cli.command("run")
@click.option("--prompt", default=SCENARIO, show_default=False,
              help="Support request prompt.")
@click.option("--customer", default=CUSTOMER, show_default=True,
              help="Customer identifier.")
def run_cmd(prompt: str, customer: str) -> None:
    """Run the three-step support-copilot flow; print exec id + decision."""
    adapter = KitaruAdapterPA(model=BASELINE_MODEL)
    exec_id = adapter.run(prompt, customer)
    decision = adapter.decision_of(exec_id)
    click.echo(f"exec_id={exec_id}")
    click.echo(_decision_summary(decision))


# ---------------------------------------------------------------------------
# reproduce
# ---------------------------------------------------------------------------

@cli.command("reproduce")
@click.argument("exec_id")
def reproduce_cmd(exec_id: str) -> None:
    """Reproduce EXEC_ID from the decide checkpoint via the Kitaru CLI.

    Shells out to ``kitaru executions replay --from decide <EXEC_ID>``
    (CLI-native path) and parses the JSON result to get the replay exec id.
    Then diffs the original vs replay to show reproduction fidelity.
    """
    cmd = [
        "kitaru", "executions", "replay",
        "--from", CUT,
        exec_id,
        "--output", "json",
    ]
    click.echo("  $ " + " ".join(cmd))

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        click.echo(f"kitaru CLI error: {result.stderr.strip()}", err=True)
        raise SystemExit(result.returncode)

    envelope = json.loads(result.stdout)
    replay_id = envelope["item"]["exec_id"]
    click.echo(f"replay_exec_id={replay_id}")

    # Diff original vs replay to report reproduction fidelity.
    adapter = KitaruAdapterPA(model=BASELINE_MODEL)
    report = adapter.diff(exec_id, replay_id)
    click.echo(f"reproduction drift: {report.has_fork_drift}")


# ---------------------------------------------------------------------------
# experiment
# ---------------------------------------------------------------------------

@cli.command("experiment")
@click.argument("exec_id")
@click.option("--model", default=FORK_MODEL, show_default=True,
              help="Model for the reconfigured decide step.")
@click.option("--prompt-profile", default=FORK_PROMPT_PROFILE, show_default=True,
              help="Prompt profile for the reconfigured decide step.")
@click.option("--html", "html_path", default="replay_vs_experiment.html",
              show_default=True, help="Write the comparison HTML here.")
def experiment_cmd(
    exec_id: str,
    model: str,
    prompt_profile: str,
    html_path: str,
) -> None:
    """Reproduce baseline, then experiment with a reconfigured agent; write HTML.

    1. Reproduce EXEC_ID (no edits) from the decide checkpoint.
    2. Experiment: re-run decide+finalize under MODEL + PROMPT_PROFILE.
    3. Diff reproduce vs experiment and write the HTML comparison report.
    """
    adapter = KitaruAdapterPA(model=BASELINE_MODEL)

    # Step 1: reproduce (baseline, no edit).
    repro_id = adapter.reproduce(exec_id)
    click.echo(f"reproduce_exec_id={repro_id}")

    # Step 2: experiment (reconfigured agent).
    exp_id = adapter.experiment(exec_id, model=model, prompt_profile=prompt_profile)
    click.echo(f"experiment_exec_id={exp_id}")

    # Step 3: diff + HTML.
    report = adapter.diff(repro_id, exp_id)
    click.echo(str(report))

    repro_dec = adapter.decision_of(repro_id)
    exp_dec = adapter.decision_of(exp_id)
    outcomes = _build_outcomes(repro_dec, exp_dec)
    settings_changes = [
        ("model", BASELINE_MODEL, model),
        ("prompt_profile", "baseline", prompt_profile),
    ]

    path = write_html(
        html_path,
        exec_id=exec_id,
        scenario=SCENARIO[:80] + "..." if len(SCENARIO) > 80 else SCENARIO,
        cut=CUT,
        nodes=_FLOW_NODES,
        settings_changes=settings_changes,
        outcomes=outcomes,
        has_drift=report.has_fork_drift,
        reproduce_summary=_decision_summary(repro_dec),
        experiment_summary=_decision_summary(exp_dec),
    )
    click.echo(f"html: {path}")


# ---------------------------------------------------------------------------
# cohort
# ---------------------------------------------------------------------------

@cli.command("cohort")
@click.option("--model", default=FORK_MODEL, show_default=True,
              help="Model for the reconfigured decide step.")
@click.option("--prompt-profile", default=FORK_PROMPT_PROFILE, show_default=True,
              help="Prompt profile for the reconfigured decide step.")
@click.option("--n", default=10, show_default=True,
              help="Number of recent production runs to include.")
def cohort_cmd(model: str, prompt_profile: str, n: int) -> None:
    """Apply the same reconfiguration across the last N production runs.

    Runs reproduce + experiment for each of the N most recent baseline
    executions, then prints per-run rows, aggregate metrics, and the
    overall 'is it an improvement?' verdict.
    """
    adapter = KitaruAdapterPA(model=BASELINE_MODEL)
    report = adapter.cohort(model=model, prompt_profile=prompt_profile, n=n)
    click.echo(str(report))
    if report.improvement:
        click.echo("verdict: improvement — cheaper, faster, quality not worse.")
    else:
        click.echo("verdict: not an improvement across all three criteria.")


# ---------------------------------------------------------------------------
# run-all  (narrated full arc)
# ---------------------------------------------------------------------------

@cli.command("run-all")
@click.option("--prompt", default=SCENARIO, show_default=False,
              help="Support request prompt.")
@click.option("--customer", default=CUSTOMER, show_default=True,
              help="Customer identifier.")
def run_all(prompt: str, customer: str) -> None:
    """Narrated full arc: run → reproduce → experiment → compare → cohort."""

    # ------------------------------------------------------------------
    # 1) Production run.
    # ------------------------------------------------------------------
    click.secho(
        "1) Your PydanticAI agent ran in production — wrapped with Kitaru, "
        "every step is a durable checkpoint.",
        bold=True,
    )
    adapter = KitaruAdapterPA(model=BASELINE_MODEL)
    exec_id = adapter.run(prompt, customer)
    decision = adapter.decision_of(exec_id)
    click.echo(f"   exec_id={exec_id}")
    click.echo(f"   {_decision_summary(decision)}")

    # ------------------------------------------------------------------
    # 2) Reproduce via the Kitaru CLI (CLI-native path).
    # ------------------------------------------------------------------
    click.secho(
        "2) Reproduce it from the decision step via the Kitaru CLI "
        "(cached head, live tail).",
        bold=True,
    )
    cmd = [
        "kitaru", "executions", "replay",
        "--from", CUT,
        exec_id,
        "--output", "json",
    ]
    click.echo("   $ " + " ".join(cmd))

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        click.echo(f"   kitaru CLI error: {result.stderr.strip()}", err=True)
        raise SystemExit(result.returncode)

    envelope = json.loads(result.stdout)
    replay_id = envelope["item"]["exec_id"]
    repro_report = adapter.diff(exec_id, replay_id)
    click.echo(f"   replay_exec_id={replay_id}")
    click.echo(f"   reproduction drift: {repro_report.has_fork_drift}")

    # ------------------------------------------------------------------
    # 3) Experiment: re-run decide under a reconfigured agent.
    # ------------------------------------------------------------------
    click.secho(
        f"3) Experiment before shipping: re-run the decision step under "
        f"{FORK_MODEL} + {FORK_PROMPT_PROFILE}.",
        bold=True,
    )
    repro_id = adapter.reproduce(exec_id)
    exp_id = adapter.experiment(exec_id, model=FORK_MODEL, prompt_profile=FORK_PROMPT_PROFILE)
    click.echo(f"   reproduce_exec_id={repro_id}")
    click.echo(f"   experiment_exec_id={exp_id}")

    # ------------------------------------------------------------------
    # 4) Compare — did the change move the decision?
    # ------------------------------------------------------------------
    click.secho("4) Compare — did the change move the decision?", bold=True)
    report = adapter.diff(repro_id, exp_id)
    click.echo(f"   {report}")

    repro_dec = adapter.decision_of(repro_id)
    exp_dec = adapter.decision_of(exp_id)
    outcomes = _build_outcomes(repro_dec, exp_dec)
    settings_changes = [
        ("model", BASELINE_MODEL, FORK_MODEL),
        ("prompt_profile", "baseline", FORK_PROMPT_PROFILE),
    ]
    html_path = write_html(
        "replay_vs_experiment.html",
        exec_id=exec_id,
        scenario=SCENARIO[:80] + "..." if len(SCENARIO) > 80 else SCENARIO,
        cut=CUT,
        nodes=_FLOW_NODES,
        settings_changes=settings_changes,
        outcomes=outcomes,
        has_drift=report.has_fork_drift,
        reproduce_summary=_decision_summary(repro_dec),
        experiment_summary=_decision_summary(exp_dec),
    )
    click.echo(f"   html: {html_path}")

    # ------------------------------------------------------------------
    # 5) Cohort — apply the same change across recent production runs.
    # ------------------------------------------------------------------
    click.secho(
        "5) Now apply that same change across your last N production runs.",
        bold=True,
    )
    cohort_report = adapter.cohort(
        model=FORK_MODEL,
        prompt_profile=FORK_PROMPT_PROFILE,
        n=10,
    )
    click.echo(str(cohort_report))
    if cohort_report.improvement:
        click.echo("   verdict: improvement — cheaper, faster, quality not worse.")
    else:
        click.echo("   verdict: not an improvement across all three criteria.")


if __name__ == "__main__":
    cli()
