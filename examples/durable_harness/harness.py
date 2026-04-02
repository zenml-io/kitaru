"""Durable planner-builder-evaluator harness.

Inspired by Anthropic's "Harness design for long-running application
development" (March 2026), rebuilt with Kitaru primitives for crash
recovery, replay, and human-in-the-loop gates.

Usage::

    cd examples/durable_harness
    uv run python harness.py "A personal dashboard with weather, todo, and quotes"

Or with a specific model and more rounds::

    uv run python harness.py "..." --model anthropic/claude-sonnet-4-6 --max-rounds 3
"""

import argparse
import json
import os
import re
import sys
import threading
import time
from pathlib import Path

from models import EvaluationReport, HarnessResult, ReviewDecision
from prompts import BUILDER_SYSTEM, EVALUATOR_SYSTEM, PLANNER_SYSTEM, SUMMARIZER_SYSTEM
from pydantic import ValidationError

import kitaru
from kitaru import KitaruClient, checkpoint, flow

DEFAULT_MODEL = os.environ.get("DURABLE_HARNESS_MODEL", "harness")
DEFAULT_FAST_MODEL = os.environ.get("DURABLE_HARNESS_FAST_MODEL", DEFAULT_MODEL)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _strip_code_fences(text: str) -> str:
    """Remove markdown code fences from builder output.

    LLMs frequently wrap code in ```html ... ``` despite prompt instructions.
    """
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped
    # Remove opening fence (```html, ```HTML, ```, etc.)
    stripped = re.sub(r"^```\w*\n?", "", stripped)
    # Remove closing fence
    stripped = re.sub(r"\n?```\s*$", "", stripped)
    return stripped.strip()


def _parse_evaluation(text: str) -> EvaluationReport:
    """Parse evaluator LLM output into a structured report.

    Tries JSON parsing first, falls back to heuristic text analysis.
    """
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        json_lines = [line for line in lines if not line.strip().startswith("```")]
        cleaned = "\n".join(json_lines).strip()

    try:
        data = json.loads(cleaned)
        return EvaluationReport.model_validate(data)
    except (json.JSONDecodeError, ValidationError):
        pass

    # Fallback: heuristic text-based parsing
    text_upper = text.upper()
    passed = "PASS" in text_upper and "FAIL" not in text_upper
    return EvaluationReport(
        passed=passed,
        feedback=text,
        criteria_met=0,
        criteria_total=0,
    )


def _prime_zenml_runtime() -> None:
    """Force ZenML's lazy store initialization on the main thread.

    Avoids a race condition when the watcher thread also accesses the
    store concurrently.
    """
    from zenml.client import Client

    _ = Client().zen_store


def _save_round_output(
    code: str,
    exec_id: str,
    round_index: int,
    output_dir: Path,
) -> Path:
    """Save a build round's HTML output to the outputs directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = exec_id[:8] if len(exec_id) > 8 else exec_id
    filename = f"{prefix}_round_{round_index}.html"
    output_path = output_dir / filename
    output_path.write_text(code)
    return output_path


def _save_all_round_outputs(
    *,
    exec_id: str,
    final_code: str,
    rounds_completed: int,
    output_dir: Path,
) -> None:
    """Save all round outputs locally by fetching artifacts from the server.

    Uses KitaruClient to browse the execution's artifacts and find each
    round's code_round_N artifact. Falls back to saving only the final
    round (from the flow return value) if server browsing fails.
    """
    saved_paths: list[Path] = []
    try:
        client = KitaruClient()
        execution = client.executions.get(exec_id)
        for art in execution.artifacts:
            if art.name.startswith("code_round_"):
                round_str = art.name.removeprefix("code_round_")
                try:
                    round_idx = int(round_str)
                except ValueError:
                    continue
                code = art.load()
                if isinstance(code, str):
                    path = _save_round_output(code, exec_id, round_idx, output_dir)
                    saved_paths.append(path)
    except Exception as exc:
        print(f"  (Could not fetch rounds from server: {exc}; saving final round only)")

    if not saved_paths:
        path = _save_round_output(
            final_code,
            exec_id,
            rounds_completed - 1,
            output_dir,
        )
        saved_paths.append(path)

    for path in saved_paths:
        print(f"  Saved: {path}")
    print(f"\n{len(saved_paths)} output file(s) written to {output_dir}/")


# ---------------------------------------------------------------------------
# Checkpoints
# ---------------------------------------------------------------------------


@checkpoint(type="llm_call", retries=1)
def planner_agent(task: str, model: str) -> str:
    """Expand a short task description into a full product spec."""
    spec = kitaru.llm(
        f"Create a detailed product spec for: {task}",
        model=model,
        system=PLANNER_SYSTEM,
        name="planner_call",
    )
    kitaru.save("spec", spec, type="context")
    kitaru.log(agent_role="planner", spec_chars=len(spec))
    return spec


@checkpoint(type="llm_call", retries=1)
def builder_agent(
    spec: str,
    feedback: str | None,
    model: str,
    round_index: int,
) -> str:
    """Generate a complete single-file HTML/CSS/JS app from spec + feedback."""
    prompt = f"Build this application:\n\n{spec}"
    if feedback:
        prompt += f"\n\nPrevious QA feedback to address:\n{feedback}"

    raw_code = kitaru.llm(
        prompt,
        model=model,
        system=BUILDER_SYSTEM,
        name=f"builder_call_{round_index}",
        max_tokens=8000,
    )
    code = _strip_code_fences(raw_code)
    kitaru.save(f"code_round_{round_index}", code, type="output")
    kitaru.log(
        agent_role="builder",
        round_index=round_index,
        code_chars=len(code),
        has_feedback=feedback is not None,
    )
    return code


@checkpoint(type="llm_call", retries=1)
def evaluator_agent(
    spec: str,
    code: str,
    model: str,
    round_index: int,
) -> EvaluationReport:
    """Grade the generated code against the spec's acceptance criteria."""
    report_text = kitaru.llm(
        f"Evaluate this code against the spec.\n\nSPEC:\n{spec}\n\nCODE:\n{code}",
        model=model,
        system=EVALUATOR_SYSTEM,
        name=f"evaluator_call_{round_index}",
    )
    kitaru.save(f"qa_report_round_{round_index}", report_text, type="context")

    report = _parse_evaluation(report_text)
    kitaru.log(
        agent_role="evaluator",
        round_index=round_index,
        passed=report.passed,
        criteria_met=report.criteria_met,
        criteria_total=report.criteria_total,
    )
    return report


@checkpoint(type="llm_call", retries=1)
def summarize_feedback(
    evaluation_feedback: str,
    human_feedback: str | None,
    model: str,
    round_index: int,
) -> str:
    """Condense QA feedback + human notes into actionable builder instructions."""
    prompt = f"QA Evaluation:\n{evaluation_feedback}"
    if human_feedback:
        prompt += f"\n\nHuman reviewer notes:\n{human_feedback}"
    prompt += (
        "\n\nSummarize into a concise, actionable list of changes for the next build."
    )

    summary = kitaru.llm(
        prompt,
        model=model,
        system=SUMMARIZER_SYSTEM,
        name=f"summarize_call_{round_index}",
    )
    kitaru.save(f"feedback_summary_round_{round_index}", summary, type="context")
    kitaru.log(
        agent_role="summarizer", round_index=round_index, summary_chars=len(summary)
    )
    return summary


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow
def durable_harness(
    task: str,
    model: str = DEFAULT_MODEL,
    fast_model: str = DEFAULT_FAST_MODEL,
    max_rounds: int = 3,
) -> HarnessResult:
    """Durable planner -> builder -> evaluator harness.

    Each agent phase is a checkpointed boundary. Crash at any point
    and replay from the last successful checkpoint without reburning
    earlier LLM calls.

    After a QA failure, the flow pauses via kitaru.wait() for a human
    review decision (approve / revise / abort).
    """
    kitaru.log(task=task, model=model, fast_model=fast_model, max_rounds=max_rounds)

    # Phase 1: Planning — uses fast_model (lighter task, just spec expansion)
    spec = planner_agent(task, fast_model, id="planner").load()

    # Phase 2: Build / Evaluate loop
    feedback: str | None = None
    code: str = ""

    for round_index in range(max_rounds):
        kitaru.log(round=round_index + 1)

        # Build (checkpointed — crash here? replay reuses cached planner output)
        code = builder_agent(
            spec,
            feedback,
            model,
            round_index,
            id=f"builder_round_{round_index}",
        ).load()

        # Evaluate (checkpointed — crash here? replay reuses cached build output)
        report: EvaluationReport = evaluator_agent(
            spec,
            code,
            model,
            round_index,
            id=f"evaluator_round_{round_index}",
        ).load()

        if report.passed:
            kitaru.log(outcome="passed", final_round=round_index + 1)
            return HarnessResult(
                code=code,
                spec=spec,
                rounds_completed=round_index + 1,
                outcome="passed",
            )

        # Human gate: review the QA failure
        feedback_preview = report.feedback[:500] if report.feedback else ""
        decision: ReviewDecision = kitaru.wait(
            schema=ReviewDecision,
            name=f"review_round_{round_index}",
            question=(
                f"Round {round_index + 1} QA: "
                f"{report.criteria_met}/{report.criteria_total} criteria met.\n\n"
                f"Feedback:\n{feedback_preview}\n\n"
                "Respond with: approve / revise (with feedback) / abort"
            ),
            timeout=600,
            metadata={
                "round": round_index + 1,
                "criteria_met": report.criteria_met,
                "criteria_total": report.criteria_total,
            },
        )

        if decision.action == "approve":
            kitaru.log(outcome="approved_by_user", final_round=round_index + 1)
            return HarnessResult(
                code=code,
                spec=spec,
                rounds_completed=round_index + 1,
                outcome="approved_by_user",
            )

        if decision.action == "abort":
            kitaru.log(outcome="aborted_by_user", final_round=round_index + 1)
            return HarnessResult(
                code="",
                spec=spec,
                rounds_completed=round_index + 1,
                outcome="aborted_by_user",
            )

        # Revise: summarize feedback for next build round (uses fast_model)
        human_feedback = decision.feedback if decision.feedback else None
        feedback = summarize_feedback(
            report.feedback,
            human_feedback,
            fast_model,
            round_index,
            id=f"summarize_round_{round_index}",
        ).load()

    kitaru.log(outcome="max_rounds_exhausted", final_round=max_rounds)
    return HarnessResult(
        code=code,
        spec=spec,
        rounds_completed=max_rounds,
        outcome="max_rounds_exhausted",
    )


# ---------------------------------------------------------------------------
# Watcher thread (prints fallback CLI commands when flow pauses)
# ---------------------------------------------------------------------------


def _find_pending_wait(
    client: KitaruClient,
    exec_id: str,
) -> bool:
    """Check if the execution has a pending wait."""
    try:
        execution = client.executions.get(exec_id)
        return execution.pending_wait is not None
    except Exception:
        return False


def _watch_and_print_commands(
    client: KitaruClient,
    exec_id: str,
    stop_event: threading.Event,
) -> None:
    """Watch for pending waits and print fallback CLI commands."""
    while not stop_event.is_set():
        if _find_pending_wait(client, exec_id):
            print("\n--- Flow is waiting for input ---")
            print("If this terminal is prompting, answer inline.")
            print("Otherwise, run one of these in another terminal:\n")
            print(
                f"  kitaru executions input {exec_id} "
                f'--value \'{{"action": "approve", "feedback": ""}}\''
            )
            print(
                f"  kitaru executions input {exec_id} "
                f'--value \'{{"action": "revise", "feedback": "Your feedback"}}\''
            )
            print(
                f"  kitaru executions input {exec_id} "
                f'--value \'{{"action": "abort", "feedback": ""}}\''
            )
            print(
                f"  kitaru executions resume {exec_id}  # only if runner has exited\n"
            )
            return
        time.sleep(2.0)


# ---------------------------------------------------------------------------
# Post-run summary
# ---------------------------------------------------------------------------


def _print_execution_summary(exec_id: str) -> None:
    """Print a summary of the execution's checkpoints and artifacts."""
    try:
        client = KitaruClient()
        execution = client.executions.get(exec_id)
        print(f"\n{'=' * 60}")
        print(f"Execution: {exec_id}")
        print(f"Status:    {execution.status.value}")
        print(f"{'=' * 60}")

        if execution.checkpoints:
            print(f"\n{'Checkpoint':<30} {'Status':<12} {'Artifacts':<10}")
            print(f"{'-' * 30} {'-' * 12} {'-' * 10}")
            for cp in execution.checkpoints:
                artifact_count = len(cp.artifacts) if cp.artifacts else 0
                print(f"{cp.name:<30} {cp.status:<12} {artifact_count:<10}")

        print("\nInspect details:")
        print(f"  kitaru executions get {exec_id}")
        print(f"  kitaru executions logs {exec_id} --grouped -v")
    except Exception as exc:
        print(f"\n(Could not fetch execution summary: {exc})")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Durable planner-builder-evaluator harness with Kitaru.",
    )
    parser.add_argument("task", help="Task description for the harness.")
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Model for builder + evaluator (default: {DEFAULT_MODEL}).",
    )
    parser.add_argument(
        "--fast-model",
        default=DEFAULT_FAST_MODEL,
        help=f"Model for planner + summarizer (default: {DEFAULT_FAST_MODEL}).",
    )
    parser.add_argument(
        "--max-rounds",
        type=int,
        default=3,
        help="Maximum build/evaluate rounds (default: 3).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs"),
        help="Directory for generated HTML files (default: outputs/).",
    )
    return parser


def main() -> None:
    """Run the harness from the command line."""
    args = _build_parser().parse_args()

    print("Starting durable harness:")
    print(f"  Task:       {args.task!r}")
    print(f"  Model:      {args.model!r} (builder + evaluator)")
    print(f"  Fast model: {args.fast_model!r} (planner + summarizer)")
    print(f"  Max rounds: {args.max_rounds}")

    _prime_zenml_runtime()
    client = KitaruClient()

    handle = durable_harness.run(
        args.task,
        model=args.model,
        fast_model=args.fast_model,
        max_rounds=args.max_rounds,
    )
    print(f"  Exec ID:    {handle.exec_id}\n")

    # Start watcher thread for fallback CLI commands
    stop_event = threading.Event()
    watcher = threading.Thread(
        target=_watch_and_print_commands,
        kwargs={
            "client": client,
            "exec_id": handle.exec_id,
            "stop_event": stop_event,
        },
        name="kitaru-harness-watcher",
        daemon=True,
    )
    watcher.start()

    try:
        result: HarnessResult = handle.wait()
    except KeyboardInterrupt:
        print("\nInterrupted. Replay later with:")
        print(f"  kitaru executions replay {handle.exec_id} --from builder_round_0")
        sys.exit(130)
    finally:
        stop_event.set()
        watcher.join(timeout=2.0)

    # Save output files — fetch all rounds from server-side artifacts
    if result.code and result.outcome != "aborted_by_user":
        _save_all_round_outputs(
            exec_id=handle.exec_id,
            final_code=result.code,
            rounds_completed=result.rounds_completed,
            output_dir=args.output_dir,
        )
    else:
        print(f"\nOutcome: {result.outcome}")

    # Print execution summary
    _print_execution_summary(handle.exec_id)


if __name__ == "__main__":
    main()
