"""PydanticAI support-copilot demo — run / replay / cohort with Kitaru SDK primitives.

Read this top to bottom: it's the whole replay story in plain code. There is no
wrapper class hiding the SDK — every durable operation below is a direct Kitaru
call you can copy into your own project:

    support_copilot_flow.run(prompt, customer, model, prompt_profile)
    support_copilot_flow.replay(exec_id, from_="decide", cache=False, ...)
    KitaruClient().executions.get(...) / .list(...)

Pick which step to run via the command line:

    cd examples/end_to_end/pydantic_replay_fork
    uv run python demo.py run-all          # run -> replay -> cohort, narrated
    uv run python demo.py run              # one production run; prints exec_id
    uv run python demo.py replay <EXEC-ID> # reproduce + edited replay + HTML
    uv run python demo.py cohort           # apply the edit across recent runs

The same replay is a first-class CLI command too — see the README:
    kitaru executions replay --from decide <EXEC-ID>
"""

import sys

from agent import SupportDecision
from cohort import run_cohort
from comparison_html import write as write_html
from support_copilot import CUT, recent_exec_ids, support_copilot_flow
from utils import cost, decision_of, diff_decisions, latency, quality_judge

from kitaru import KitaruClient

# --- The scenario and the config we run/replay under -----------------------

SCENARIO = (
    "I need to grant all members of our engineering team admin access to the "
    "production SSO settings so they can self-service identity provider changes "
    "without going through IT. Can you enable that for our account?"
)
CUSTOMER = "acme-corp / alice@acme.example"

BASELINE_MODEL = "openai:gpt-5-mini"  # what production runs under
FORK_MODEL = "openai:gpt-5-nano"  # the cheaper model we replay under
FORK_PROMPT_PROFILE = "trimmed_permissions"  # the looser prompt we replay under

HTML_PATH = "replay_vs_rerun.html"
_FLOW_NODES = ("gather_context", "decide", "finalize")


# --- Tiny presentation helpers (not Kitaru — just printing) ----------------


def section(text: str) -> None:
    """Print a bold step header."""
    print(f"\n\033[1m{text}\033[0m")


def decision_summary(decision: dict) -> str:
    """One-line view of a SupportDecision dict."""
    return (
        f"risk={decision.get('risk_status', '?')}  "
        f"action={decision.get('required_action', '?')}  "
        f"label={decision.get('policy_label', '?')}"
    )


def write_comparison_html(exec_id: str, reproduced: dict, edited: dict) -> str:
    """Render the side-by-side reproduce-vs-edited HTML report."""
    fields = list(SupportDecision.model_fields)
    outcomes = [
        (f, reproduced.get(f), edited.get(f), reproduced.get(f) == edited.get(f))
        for f in fields
    ]
    has_drift = diff_decisions(reproduced, edited).has_fork_drift
    return write_html(
        HTML_PATH,
        exec_id=exec_id,
        scenario=SCENARIO[:80] + "..." if len(SCENARIO) > 80 else SCENARIO,
        cut=CUT,
        nodes=_FLOW_NODES,
        settings_changes=[
            ("model", BASELINE_MODEL, FORK_MODEL),
            ("prompt_profile", "baseline", FORK_PROMPT_PROFILE),
        ],
        outcomes=outcomes,
        has_drift=has_drift,
        reproduced_summary=decision_summary(reproduced),
        edited_summary=decision_summary(edited),
    )


# --- The steps. Each is independent so you can run just one. ----------------


def run() -> str:
    """Run the three-step flow once (a 'production' run); return its exec_id."""
    section("Run the PydanticAI agent as a durable Kitaru flow")

    # The one SDK call that starts a durable execution. .wait() blocks until the
    # flow reaches a terminal state; .exec_id is its execution id.
    handle = support_copilot_flow.run(SCENARIO, CUSTOMER, BASELINE_MODEL, "baseline")
    handle.wait()

    client = KitaruClient()
    print(f"   exec_id={handle.exec_id}")
    print(f"   {decision_summary(decision_of(client, handle.exec_id))}")
    return handle.exec_id


def replay(exec_id: str) -> None:
    """Replay exec_id from `decide` twice — faithfully, then edited — and compare."""
    client = KitaruClient()

    # The decision the original run produced — the baseline we compare against.
    original = decision_of(client, exec_id)

    # (1) Replay from `decide` with NO edits — a faithful reproduction.
    #     gather_context (before the cut) is served from cache; decide + finalize
    #     re-run live. No overrides are passed, so the recorded config is reused.
    section("Replay #1 — reproduce from `decide`, no edits (cached head, live tail)")
    reproduced = support_copilot_flow.replay(exec_id, from_=CUT, cache=False)
    reproduced.wait()
    reproduced_decision = decision_of(client, reproduced.exec_id)
    print(f"   exec_id={reproduced.exec_id}")
    print(f"   {diff_decisions(original, reproduced_decision)}")

    # (2) The SAME SDK call, now WITH edits — a cheaper model + looser prompt.
    #     The extra kwargs override the flow inputs, so decide + finalize re-run
    #     under the new config while gather_context is still served from cache.
    section(f"Replay #2 — re-run `decide` under {FORK_MODEL} + {FORK_PROMPT_PROFILE}")
    edited = support_copilot_flow.replay(
        exec_id,
        from_=CUT,
        cache=False,
        model=FORK_MODEL,
        prompt_profile=FORK_PROMPT_PROFILE,
    )
    edited.wait()
    edited_decision = decision_of(client, edited.exec_id)
    print(f"   exec_id={edited.exec_id}")
    print(f"   {diff_decisions(reproduced_decision, edited_decision)}")

    path = write_comparison_html(exec_id, reproduced_decision, edited_decision)
    print(f"   html: {path}")


def cohort() -> None:
    """Apply the same edit across the last N production runs and measure the delta."""
    section("Cohort — apply the edit across recent production runs")

    client = KitaruClient()
    cases = recent_exec_ids(client, 10)
    report = run_cohort(
        cases,
        baseline_model=BASELINE_MODEL,
        variant_model=FORK_MODEL,
        variant_prompt_profile=FORK_PROMPT_PROFILE,
        metrics=[cost, latency, quality_judge],
        repeats=1,
    )
    report.summary()
    regs = report.regressions()
    print(
        "   regressions:",
        [getattr(r, "name", r) for r in regs] if regs else "none",
    )


def run_all() -> None:
    """The full narrated arc: run -> replay -> cohort."""
    exec_id = run()
    replay(exec_id)
    cohort()


# --- Command-line dispatch -------------------------------------------------


def main(argv: list[str]) -> None:
    command = argv[0] if argv else "run-all"

    if command == "run":
        run()
    elif command == "replay":
        if len(argv) < 2:
            sys.exit("usage: python demo.py replay <EXEC-ID>")
        replay(argv[1])
    elif command == "cohort":
        cohort()
    elif command == "run-all":
        run_all()
    else:
        sys.exit(
            f"unknown command {command!r}. "
            "try: run | replay <EXEC-ID> | cohort | run-all"
        )


if __name__ == "__main__":
    main(sys.argv[1:])
