"""Readable CLI for hosted inference and provider-free demonstrations."""

import argparse
import asyncio
import os
from uuid import UUID

from kitaru.client.exceptions import APIError
from pydantic_ai.messages import TextPart, ToolCallPart, ToolReturnPart, UserPromptPart

from .models import RunResult, get_scenario
from .runner import run_scenario

_COMPLETE_STATUSES = {"completed", "boundary-completed"}


def print_result(result: RunResult, *, verbose: bool = False) -> None:
    """Print chronological dialogue and distinguish completion from agent checks."""
    print(
        f"\n{result.scenario.name} | {result.variant} | {result.mode} | {result.backend}"
    )
    for index, message in enumerate(result.messages):
        historical = index < result.seed_message_count
        label = "PREFIX" if historical else "NEW"
        for part in message.parts:
            if isinstance(part, UserPromptPart):
                print(f"{label} customer: {part.content}")
            elif isinstance(part, TextPart):
                print(f"{label} agent: {part.content}")
            elif isinstance(part, ToolReturnPart):
                tool_label = "HISTORICAL" if historical else "EXECUTED"
                print(f"{tool_label} tool: {part.tool_name}")
                if verbose:
                    print(f"  Result: {part.content}")
            elif verbose and isinstance(part, ToolCallPart):
                print(f"{label} tool call: {part.tool_name}; arguments={part.args}")
    print()
    if result.status == "invalid-simulation":
        print("Simulation incomplete: customer continuation failed.")
        print("The customer simulator could not produce a valid continuation.")
    elif result.status == "agent-error":
        print("Simulation incomplete: delivery agent failed.")
    elif result.status == "turn-limit":
        print("Simulation incomplete: agent-turn limit reached.")
    elif result.status == "boundary-completed":
        print("Next-reply test complete.")
    else:
        print("Simulation complete.")
    if result.status in _COMPLETE_STATUSES:
        print(f"Agent checks: {'PASS' if result.verdict.passed else 'FAIL'}")
    else:
        print("Agent checks on replies produced so far (partial conversation):")
    print(
        f"  {'✓' if result.verdict.no_unsupported_date else '✗'} "
        "No unsupported delivery date"
    )
    if result.scenario.shipping.estimated_delivery is not None:
        print(
            f"  {'✓' if result.verdict.uses_supported_date else '✗'} "
            "Uses the supported delivery date"
        )
    if verbose:
        print(
            f"Status: {result.status}; turns={result.agent_turns}; "
            f"model requests={result.model_requests}"
        )
        print(f"Scenario SHA256: {result.scenario_hash}")
        if result.error:
            print(f"Error: {result.error}")
        for index, attempt in enumerate(result.simulator_attempts, start=1):
            print(f"Customer simulator call {index}:")
            for message in attempt.messages:
                for part in message.parts:
                    print(f"  {part.part_kind}: {part}")
            if attempt.error:
                print(f"  Error: {attempt.error}")
    elif result.status not in _COMPLETE_STATUSES:
        print("Use --verbose for request details and simulator validation errors.")


async def main() -> int:
    """Execute selected demonstrations and print readable outcomes."""
    os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")
    parser = argparse.ArgumentParser(description="Bounded delivery conversation demo")
    parser.add_argument(
        "--backend", choices=["openai", "scripted", "ollama"], default="openai"
    )
    parser.add_argument("--model", default="gpt-5-nano")
    parser.add_argument(
        "--base-url", default="http://localhost:11434/v1", help="Ollama endpoint only"
    )
    parser.add_argument("--variant", choices=["control", "fix", "both"], default="both")
    parser.add_argument(
        "--scenario", choices=["missing-date", "known-date", "all"], default="all"
    )
    parser.add_argument(
        "--mode", choices=["full", "n-minus-one", "tool-boundary"], default="full"
    )
    parser.add_argument(
        "--continue", dest="continue_after_boundary", action="store_true"
    )
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--record", action="store_true")
    parser.add_argument(
        "--server-url", help="Override the server selected by kitaru login"
    )
    parser.add_argument("--from-session", type=UUID)
    args = parser.parse_args()
    if args.from_session and args.scenario != "all":
        parser.error("--from-session cannot be combined with --scenario")
    if args.backend == "scripted":
        print(
            "SCRIPTED DEMO: responses are programmed fixtures, not inference or evidence that a prompt improves a model."
        )
    if args.from_session:
        from .persistence import load_scenario

        scenarios = [
            await load_scenario(
                session_id=args.from_session,
                server_url=args.server_url,
                api_key=os.environ.get("KITARU_API_KEY"),
            )
        ]
    else:
        scenarios = [
            get_scenario(n)
            for n in (
                ["missing-date", "known-date"]
                if args.scenario == "all"
                else [args.scenario]
            )
        ]
    failed = False
    for scenario in scenarios:
        for variant in ["control", "fix"] if args.variant == "both" else [args.variant]:
            result = await run_scenario(
                scenario,
                variant=variant,
                mode=args.mode,
                backend=args.backend,
                model=args.model,
                base_url=args.base_url,
                continue_after_boundary=args.continue_after_boundary,
            )
            print_result(result, verbose=args.verbose)
            failed |= result.status in {
                "agent-error",
                "invalid-simulation",
                "turn-limit",
            } or (args.check and not result.verdict.passed)
            if args.record:
                from .persistence import record_result

                session_id = await record_result(
                    result,
                    server_url=args.server_url,
                    api_key=os.environ.get("KITARU_API_KEY"),
                )
                print(f"Recorded session: {session_id}")
    if not args.check:
        print(
            "\nAgent checks cover ISO and English month-name dates only. Use --check to enforce them; incomplete runs always exit unsuccessfully."
        )
    return int(failed)


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except (APIError, ValueError, OSError, RuntimeError) as exc:
        print(f"Error: {exc}")
        raise SystemExit(1) from exc
