"""PydanticAI support-copilot demo — run / replay / cohort with Kitaru.

This isthe replay story in plain code. The existing CLI
command is the fastest way to replay a recorded execution:

    kitaru executions replay <EXEC-ID> --from support_decide_model_request
    kitaru executions replay <EXEC-ID> --from support_decide_model_request \
      --args '{"model": "openai:gpt-5-nano", "prompt_profile": "trimmed_permissions"}'

(The decide step runs as a KitaruAgent "calls" checkpoint named
``support_decide_model_request`` — that is the replay anchor, the constant CUT.)

The script below narrates the same operations with SDK calls:

    support_copilot_flow.run(prompt, customer, model, prompt_profile)
    support_copilot_flow.replay(exec_id, from_=CUT, cache=False, ...)
    KitaruClient().executions.get(...) / .list(...)

Pick which step to run via the command line:

    cd examples/end_to_end/pydantic_replay_fork
    uv run python demo.py run-all          # run -> replay -> cohort, narrated
    uv run python demo.py run              # one production run; prints exec_id
    uv run python demo.py replay <EXEC-ID> # reproduce + edited replay + HTML
    uv run python demo.py cohort           # apply the edit across recent runs
"""

import sys

from cohort import run_cohort
from cohort_html import write as write_cohort_html
from comparison_html import write as write_html
from dotenv import load_dotenv
from support_agent import (
    CUT,
    SupportDecision,
    recent_exec_ids,
    support_copilot_flow,
    wait_for_completion,
)
from utils import (
    cost,
    diff_decisions,
    latency,
    load_support_decision_from_execution,
    quality_judge,
)

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

HTML_PATH = "replay_three_way.html"
COHORT_HTML_PATH = "cohort_report.html"
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


def write_comparison_html(
    exec_id: str,
    original: dict,
    reproduced: dict,
    edited: dict,
) -> str:
    """Render the three-way original/reproduction/edited HTML report."""
    fields = list(SupportDecision.model_fields)
    outcomes = [
        (
            f,
            original.get(f),
            reproduced.get(f),
            edited.get(f),
            original.get(f) == reproduced.get(f),
            reproduced.get(f) == edited.get(f),
        )
        for f in fields
    ]
    reproduction_drift = diff_decisions(original, reproduced).has_drift
    edited_drift = diff_decisions(reproduced, edited).has_drift
    return write_html(
        HTML_PATH,
        exec_id=exec_id,
        scenario=SCENARIO[:80] + "..." if len(SCENARIO) > 80 else SCENARIO,
        # The HTML diagram labels steps with friendly names (_FLOW_NODES); the
        # replay anchor CUT is the adapter's "<agent>_model_request" checkpoint.
        cut="decide",
        nodes=_FLOW_NODES,
        settings_changes=[
            ("model", BASELINE_MODEL, FORK_MODEL),
            ("prompt_profile", "baseline", FORK_PROMPT_PROFILE),
        ],
        outcomes=outcomes,
        has_reproduction_drift=reproduction_drift,
        has_edited_drift=edited_drift,
        original_summary=decision_summary(original),
        reproduced_summary=decision_summary(reproduced),
        edited_summary=decision_summary(edited),
    )


# --- The steps. Each is independent so you can run just one. ----------------


def run() -> str:
    """Run the three-step flow once (a 'production' run); return its exec_id."""

    section("Run the PydanticAI agent as a durable Kitaru flow")

    # The one SDK call that starts a durable execution; wait_for_completion
    # blocks until terminal (each agent step is its own "calls" checkpoint).
    handle = support_copilot_flow.run(SCENARIO, CUSTOMER, BASELINE_MODEL, "baseline")
    wait_for_completion(handle)

    client = KitaruClient()
    decision = load_support_decision_from_execution(client, handle.exec_id)
    print(f"   original exec_id={handle.exec_id}")
    print(f"   {decision_summary(decision)}")
    print("   CLI reproduction:")
    print(f"     kitaru executions replay {handle.exec_id} --from {CUT}")
    print("   CLI edited replay:")
    print(
        f"     kitaru executions replay {handle.exec_id} --from {CUT} "
        f'--args \'{{"model": "{FORK_MODEL}", '
        f'"prompt_profile": "{FORK_PROMPT_PROFILE}"}}\''
    )
    return handle.exec_id


def replay(exec_id: str) -> None:
    """Replay exec_id from `decide` twice — faithfully, then edited — and compare."""

    client = KitaruClient()

    # The decision the original run produced — the baseline we compare against.
    original_decision = load_support_decision_from_execution(client, exec_id)

    # (1) Replay from `decide` with NO edits — a faithful reproduction.
    section("Replay #1 — reproduce from `decide`, no edits (cached head, live tail)")

    reproduced = support_copilot_flow.replay(exec_id, from_=CUT, cache=False)
    wait_for_completion(reproduced)

    reproduced_decision = load_support_decision_from_execution(
        client, reproduced.exec_id
    )

    print(f"   unchanged replay exec_id={reproduced.exec_id}")
    print(
        "   original recorded run → unchanged replay: "
        f"{diff_decisions(original_decision, reproduced_decision)}"
    )

    # (2) The SAME SDK call, now WITH edits — a cheaper model + looser prompt.
    section(f"Replay #2 — re-run `decide` under {FORK_MODEL} + {FORK_PROMPT_PROFILE}")
    edited = support_copilot_flow.replay(
        exec_id,
        from_=CUT,
        cache=False,
        model=FORK_MODEL,
        prompt_profile=FORK_PROMPT_PROFILE,
    )
    wait_for_completion(edited)

    edited_decision = load_support_decision_from_execution(client, edited.exec_id)

    print(f"   edited replay exec_id={edited.exec_id}")
    print(
        "   unchanged replay → edited replay: "
        f"{diff_decisions(reproduced_decision, edited_decision)}"
    )

    path = write_comparison_html(
        exec_id, original_decision, reproduced_decision, edited_decision
    )
    print(f"   html: {path}")


def cohort() -> None:
    """Apply the same edit across the last N production runs and measure the delta."""
    section("Cohort — apply the edit across recent 10 production runs")

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
    path = write_cohort_html(COHORT_HTML_PATH, report)
    print(f"   html: {path}")


def run_all() -> None:
    """The full narrated arc: run -> replay -> cohort."""
    exec_id = run()
    replay(exec_id)
    cohort()


# --- Command-line dispatch -------------------------------------------------


def main(argv: list[str]) -> None:
    # Load OPENAI_API_KEY (etc.) from a local .env so the in-process quality
    # judge has credentials. Existing environment variables take precedence.
    load_dotenv()
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
