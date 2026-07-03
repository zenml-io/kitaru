#!/usr/bin/env python3
"""Summarize Kitaru pytest speed probe JSONL output."""

from __future__ import annotations

import json
import math
import statistics
import sys
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

_SOURCE_KEY = "_source"


def _read_events(report_dir: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in sorted(report_dir.glob("events-*.jsonl")):
        with path.open(encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                line = line.strip()
                if not line:
                    continue
                source = f"{path}:{line_number}"
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise SystemExit(f"Invalid JSON in {source}: {exc}") from exc
                if not isinstance(event, dict):
                    type_name = type(event).__name__
                    raise SystemExit(
                        f"Invalid probe event in {source}: expected JSON object, "
                        f"got {type_name}"
                    )
                event[_SOURCE_KEY] = source
                events.append(event)
    return events


def _validate_single_session(events: list[dict[str, Any]]) -> None:
    sources_by_session: dict[str, str] = {}
    for event in events:
        session_id = event.get("session_id")
        source = str(event.get(_SOURCE_KEY, "<unknown>"))
        if not isinstance(session_id, str) or not session_id:
            raise SystemExit(
                f"Invalid probe event in {source}: expected non-empty string "
                "field 'session_id'. Remove stale event files or rerun the "
                "speed probe."
            )
        sources_by_session.setdefault(session_id, source)

    if len(sources_by_session) <= 1:
        return

    session_sources = "; ".join(
        f"{session_id} at {sources_by_session[session_id]}"
        for session_id in sorted(sources_by_session)
    )
    raise SystemExit(
        "Probe report contains events from multiple pytest speed-probe "
        f"sessions: {session_sources}. Summarize one session or remove "
        "stale event files."
    )


def _event_context(event: dict[str, Any]) -> str:
    context_parts: list[str] = []
    for field in ("kind", "nodeid", "fixture", "phase", "worker"):
        value = event.get(field)
        if value:
            context_parts.append(f"{field}={value}")
    return ", ".join(context_parts) or "unknown event"


def _invalid_numeric_message(
    event: dict[str, Any], field: str, problem: str, value: Any
) -> str:
    source = str(event.get(_SOURCE_KEY, "<unknown>"))
    return (
        f"Invalid numeric field {field!r} in {source} ({_event_context(event)}): "
        f"{problem}, got {value!r}"
    )


def _event_float(event: dict[str, Any], field: str) -> float:
    if field not in event:
        raise SystemExit(
            _invalid_numeric_message(
                event, field, "expected a finite number", "missing"
            )
        )

    value = event[field]
    if isinstance(value, bool):
        raise SystemExit(
            _invalid_numeric_message(event, field, "expected a finite number", value)
        )

    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise SystemExit(
            _invalid_numeric_message(event, field, "expected a finite number", value)
        ) from exc

    if not math.isfinite(parsed):
        raise SystemExit(
            _invalid_numeric_message(event, field, "expected a finite number", value)
        )

    return parsed


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
            phase_name = f"{fixture} {kind.removeprefix('fixture_')}"
            durations[phase_name].append(_event_float(event, "seconds"))
        elif kind == "primed_zenml_setup":
            durations["primed_zenml setup"].append(_event_float(event, "seconds"))

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
        phase_seconds[nodeid][phase] += _event_float(event, "seconds")

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
            worker_phase_totals[worker] += _event_float(event, "seconds")
            nodeid = str(event.get("nodeid", ""))
            timestamp = _event_float(event, "timestamp")
            if nodeid and timestamp >= last_test_by_worker.get(worker, (0.0, ""))[0]:
                last_test_by_worker[worker] = (timestamp, nodeid)
        elif event.get("kind") == "primed_zenml_setup":
            primed_count_by_worker[worker] += 1
            primed_seconds_by_worker[worker] += _event_float(event, "seconds")

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
    _validate_single_session(events)

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
