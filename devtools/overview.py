"""Print a read-only dashboard of what is in a running Kitaru server."""

import argparse
import asyncio
import json
import os
import sys
from collections import Counter
from collections.abc import Awaitable, Callable

from stack import STATE_FILE

from kitaru.api_models.v1.agent import AgentListParams
from kitaru.api_models.v1.cohort import CohortListParams
from kitaru.api_models.v1.evaluation import EvaluationListParams
from kitaru.api_models.v1.experiment import ExperimentListParams
from kitaru.api_models.v1.job import JobListParams
from kitaru.api_models.v1.session import SessionListParams
from kitaru.client.api_client import KitaruAPIClient

DEFAULT_LIMIT = 15
SESSION_SCAN_LIMIT = 2000
COUNT_PAGE_SIZE = 200


def print_table(
    title: str, headers: list[str], rows: list[list[str]], max_rows: int
) -> None:
    """Print a left-justified table capped at max_rows rows."""
    print(f"\n=== {title} ===")
    if not rows:
        print("(none)")
        return
    shown = rows[:max_rows]
    widths = [
        max(len(header), *(len(row[i]) for row in shown))
        for i, header in enumerate(headers)
    ]
    print(
        "  ".join(
            header.ljust(width) for header, width in zip(headers, widths, strict=True)
        )
    )
    for row in shown:
        print(
            "  ".join(
                cell.ljust(width) for cell, width in zip(row, widths, strict=True)
            )
        )
    if len(rows) > max_rows:
        print(f"... {len(rows) - max_rows} more")


def short(value: object) -> str:
    """Shorten a value to 18 characters, keeping the tail so ids stay distinct."""
    # UUIDv7 rows created in the same millisecond share a long common prefix,
    # so a head-only truncation renders them identically.
    text = str(value)
    if len(text) <= 18:
        return text
    return f"{text[:8]}...{text[-7:]}"


def _cell(value: object) -> str:
    """Render a value as a table cell, blank for None."""
    return "" if value is None else str(value)


async def _print_agents(client: KitaruAPIClient, limit: int) -> None:
    """Print a table of agents."""
    page = await client.agents.list(AgentListParams(size=limit))
    rows = [
        [short(agent.id), agent.name, str(agent.latest_version)] for agent in page.items
    ]
    print_table("Agents", ["id", "name", "latest_version"], rows, limit)


async def _print_sessions(client: KitaruAPIClient, limit: int) -> None:
    """Print the session status summary and the most recent sessions."""
    counts: Counter[tuple[str, str]] = Counter()
    scanned = 0
    truncated = False
    async for session in client.sessions.iter(SessionListParams(size=COUNT_PAGE_SIZE)):
        counts[(session.origin.value, session.status.value)] += 1
        scanned += 1
        if scanned >= SESSION_SCAN_LIMIT:
            truncated = True
            break
    title = "Sessions by origin and status"
    if truncated:
        title += f" (truncated at {SESSION_SCAN_LIMIT})"
    summary_rows = [
        [origin, status, str(count)]
        for (origin, status), count in sorted(counts.items())
    ]
    print_table(title, ["origin", "status", "count"], summary_rows, len(summary_rows))

    page = await client.sessions.list(SessionListParams(size=limit))
    rows = [
        [
            short(session.id),
            _cell(session.name),
            session.origin.value,
            session.status.value,
            str(session.llm_call_count),
            str(session.tool_call_count),
            _cell(session.cost),
        ]
        for session in page.items
    ]
    print_table(
        "Recent sessions",
        ["id", "name", "origin", "status", "llm_calls", "tool_calls", "cost"],
        rows,
        limit,
    )


async def _print_jobs(client: KitaruAPIClient, limit: int) -> None:
    """Print the job status summary and the most recent jobs."""
    counts: Counter[str] = Counter()
    async for job in client.jobs.iter(JobListParams(size=COUNT_PAGE_SIZE)):
        counts[job.status.value] += 1
    summary_rows = [[status, str(count)] for status, count in sorted(counts.items())]
    print_table("Jobs by status", ["status", "count"], summary_rows, len(summary_rows))

    page = await client.jobs.list(JobListParams(size=limit))
    rows = [
        [short(job.id), job.kind.value, job.status.value, _cell(job.error)[:60]]
        for job in page.items
    ]
    print_table("Recent jobs", ["id", "kind", "status", "error"], rows, limit)


async def _print_evaluations(client: KitaruAPIClient, limit: int) -> None:
    """Print the evaluation summary and the most recent evaluations."""
    counts: Counter[tuple[str, str]] = Counter()
    async for evaluation in client.evaluations.iter(
        EvaluationListParams(size=COUNT_PAGE_SIZE)
    ):
        counts[(evaluation.name, evaluation.data_type.value)] += 1
    summary_rows = [
        [name, data_type, str(count)]
        for (name, data_type), count in sorted(counts.items())
    ]
    print_table(
        "Evaluations by name and data type",
        ["name", "data_type", "count"],
        summary_rows,
        len(summary_rows),
    )

    page = await client.evaluations.list(EvaluationListParams(size=limit))
    rows = [
        [
            evaluation.name,
            evaluation.data_type.value,
            _cell(evaluation.score),
            _cell(evaluation.value),
            _cell(evaluation.passed),
            short(evaluation.session_id),
        ]
        for evaluation in page.items
    ]
    print_table(
        "Recent evaluations",
        ["name", "data_type", "score", "value", "passed", "session_id"],
        rows,
        limit,
    )


async def _print_cohorts(client: KitaruAPIClient, limit: int) -> None:
    """Print a table of cohorts."""
    page = await client.cohorts.list(CohortListParams(size=limit))
    rows = [
        [short(cohort.id), cohort.name, str(cohort.latest_version)]
        for cohort in page.items
    ]
    print_table("Cohorts", ["id", "name", "latest_version"], rows, limit)


async def _print_experiments(client: KitaruAPIClient, limit: int) -> None:
    """Print a table of experiments."""
    page = await client.experiments.list(ExperimentListParams(size=limit))
    rows = [[short(experiment.id), experiment.name] for experiment in page.items]
    print_table("Experiments", ["id", "name"], rows, limit)


SECTIONS: dict[str, Callable[[KitaruAPIClient, int], Awaitable[None]]] = {
    "agents": _print_agents,
    "sessions": _print_sessions,
    "jobs": _print_jobs,
    "evaluations": _print_evaluations,
    "cohorts": _print_cohorts,
    "experiments": _print_experiments,
}


def _resolve_base_url() -> str:
    """Resolve the server base URL from the environment or the stack state file."""
    if base_url := os.environ.get("KITARU_API_URL"):
        return base_url
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        base_url = state["base_url"]
        os.environ["KITARU_API_URL"] = base_url
        return base_url
    print("KITARU_API_URL is not set and no server state file exists.", file=sys.stderr)
    sys.exit(1)


async def _run(sections: list[str], limit: int) -> None:
    """Print every requested section through one shared client."""
    async with KitaruAPIClient() as client:
        for section in sections:
            await SECTIONS[section](client, limit)


def _parse_args() -> argparse.Namespace:
    """Parse the overview CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "sections",
        nargs="*",
        default=list(SECTIONS),
        help=f"Sections to inspect. Valid sections: {', '.join(SECTIONS)}.",
    )
    parser.add_argument(
        "--limit", type=int, default=DEFAULT_LIMIT, help="Rows per table."
    )
    return parser.parse_args()


def main() -> int:
    """Run the overview CLI."""
    args = _parse_args()
    unknown = [section for section in args.sections if section not in SECTIONS]
    if unknown:
        print(
            f"Unknown section(s): {', '.join(unknown)}. "
            f"Valid sections: {', '.join(SECTIONS)}.",
            file=sys.stderr,
        )
        return 1
    _resolve_base_url()
    asyncio.run(_run(args.sections, args.limit))
    return 0


if __name__ == "__main__":
    sys.exit(main())
