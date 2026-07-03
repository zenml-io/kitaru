#!/usr/bin/env python3
"""Summarize Kitaru pytest speed probe JSONL output."""

from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any


def _read_events(report_dir: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in sorted(report_dir.glob("events-*.jsonl")):
        with path.open(encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    raise SystemExit(
                        f"Invalid JSON in {path}:{line_number}: {exc}"
                    ) from exc
    return events


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * percentile
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]
    fraction = index - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _fmt_seconds(value: float) -> str:
    return f"{value:.3f}"


def _print_fixture_totals(events: Iterable[dict[str, Any]]) -> None:
    durations: dict[str, list[float]] = defaultdict(list)
    for event in events:
        kind = event.get("kind")
        fixture = event.get("fixture")
        if kind in {"fixture_setup", "fixture_teardown"} and fixture:
            durations[f"{fixture} {kind.removeprefix('fixture_')}"].append(
                float(event.get("seconds", 0.0))
            )
        elif kind == "primed_zenml_setup":
            durations["primed_zenml setup"].append(float(event.get("seconds", 0.0)))

    print("## Fixture totals")
    print()
    print("| Fixture/phase | Count | Total seconds | Mean | p50 | p95 |")
    print("|---|---:|---:|---:|---:|---:|")
    for name in sorted(durations):
        values = durations[name]
        total = sum(values)
        mean = statistics.mean(values)
        p50 = statistics.median(values)
        p95 = _percentile(values, 0.95)
        print(
            f"| {name} | {len(values)} | {_fmt_seconds(total)} | "
            f"{_fmt_seconds(mean)} | {_fmt_seconds(p50)} | {_fmt_seconds(p95)} |"
        )
    print()


def _print_top_tests(events: Iterable[dict[str, Any]]) -> None:
    phase_seconds: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for event in events:
        if event.get("kind") != "test_phase":
            continue
        nodeid = str(event.get("nodeid", ""))
        phase = str(event.get("phase", ""))
        phase_seconds[nodeid][phase] += float(event.get("seconds", 0.0))

    print("## Top call durations")
    print()
    for nodeid, phases in sorted(
        phase_seconds.items(), key=lambda item: item[1].get("call", 0.0), reverse=True
    )[:20]:
        print(f"- {_fmt_seconds(phases.get('call', 0.0))}s call — {nodeid}")
    print()

    print("## Top setup + teardown durations")
    print()
    for nodeid, phases in sorted(
        phase_seconds.items(),
        key=lambda item: item[1].get("setup", 0.0) + item[1].get("teardown", 0.0),
        reverse=True,
    )[:20]:
        setup_teardown = phases.get("setup", 0.0) + phases.get("teardown", 0.0)
        print(f"- {_fmt_seconds(setup_teardown)}s setup+teardown — {nodeid}")
    print()


def _print_worker_tail(events: Iterable[dict[str, Any]]) -> None:
    worker_phase_totals: dict[str, float] = defaultdict(float)
    last_test_by_worker: dict[str, tuple[float, str]] = {}
    primed_count_by_worker: dict[str, int] = defaultdict(int)
    primed_seconds_by_worker: dict[str, float] = defaultdict(float)

    for event in events:
        worker = str(event.get("worker", "unknown"))
        if event.get("kind") == "test_phase":
            worker_phase_totals[worker] += float(event.get("seconds", 0.0))
            nodeid = str(event.get("nodeid", ""))
            timestamp = float(event.get("timestamp", 0.0))
            if nodeid and timestamp >= last_test_by_worker.get(worker, (0.0, ""))[0]:
                last_test_by_worker[worker] = (timestamp, nodeid)
        elif event.get("kind") == "primed_zenml_setup":
            primed_count_by_worker[worker] += 1
            primed_seconds_by_worker[worker] += float(event.get("seconds", 0.0))

    print("## Worker tail")
    print()
    print(
        "| Worker | Total reported test-phase seconds | Last test | "
        "Primed count | Primed seconds |"
    )
    print("|---|---:|---|---:|---:|")
    for worker in sorted(worker_phase_totals):
        _, last_test = last_test_by_worker.get(worker, (0.0, ""))
        print(
            f"| {worker} | {_fmt_seconds(worker_phase_totals[worker])} | "
            f"{last_test} | {primed_count_by_worker[worker]} | "
            f"{_fmt_seconds(primed_seconds_by_worker[worker])} |"
        )
    print()


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("Usage: summarize_pytest_speed.py REPORT_DIR", file=sys.stderr)
        return 2

    report_dir = Path(argv[1])
    events = _read_events(report_dir)
    if not events:
        print(f"No probe events found in {report_dir}", file=sys.stderr)
        return 1

    print(f"# Pytest speed probe summary: `{report_dir}`")
    print()
    print(f"Events: {len(events)}")
    print()
    _print_fixture_totals(events)
    _print_top_tests(events)
    _print_worker_tail(events)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
