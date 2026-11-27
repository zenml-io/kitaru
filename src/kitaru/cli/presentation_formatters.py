#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Value formatters for curated CLI human views."""

from typing import Any


def format_count(value: Any) -> str:
    """Format a collection as its item count."""
    if value is None:
        return "0"
    try:
        return str(len(value))
    except TypeError:
        return str(value)


def format_progress(value: Any) -> str:
    """Format experiment-run progress as completed over total."""
    if not isinstance(value, dict):
        return str(value)
    completed = value.get("completed", 0)
    failed = value.get("failed", 0)
    canceled = value.get("canceled", 0)
    total = value.get("total", 0)
    settled = completed + failed + canceled
    return f"{settled}/{total}"


def format_runtime(value: Any) -> str:
    """Format a worker runtime as a short platform summary."""
    if not isinstance(value, dict):
        return str(value)
    parts = [value.get("platform"), value.get("python_version")]
    return " / ".join(str(part) for part in parts if part) or "-"


def format_scope(value: Any) -> str:
    """Format worker claims and their restrictions."""
    if not isinstance(value, dict):
        return str(value)
    claims = [
        f"{claim['kind']}={claim['agent_version_id']}"
        if claim.get("agent_version_id")
        else str(claim["kind"])
        for claim in value["claims"]
    ]
    parts = [", ".join(claims)]
    for selector in value.get("selectors") or []:
        presence = "required" if selector["required"] else "if present"
        parts.append(
            f"{selector['key']} in [{', '.join(selector['values'])}] ({presence})"
        )
    if value.get("job_id"):
        parts.append(f"job={value['job_id']}")
    return "; ".join(parts)


def format_workers(value: Any) -> str:
    """Format live worker identities and scopes."""
    if value is None:
        return "unavailable"
    return (
        "\n".join(
            f"{worker['name']} ({worker['id']}): {format_scope(worker['scope'])}"
            for worker in value
        )
        or "none"
    )


def format_verdict(value: Any) -> str:
    """Format a nullable evaluation verdict."""
    if value is True:
        return "pass"
    if value is False:
        return "fail"
    return "-"


def _format_evaluation_stats(stats: dict[str, Any]) -> str:
    """Format evaluation counts, mean scores, pass rates, and category totals."""
    parts = [f"count={stats['count']}"]
    if stats.get("mean") is not None:
        parts.append(f"mean score={stats['mean']:g}")
    if stats.get("pass_rate") is not None:
        parts.append(f"pass rate={100 * stats['pass_rate']:g}%")
    if counts := stats.get("value_counts"):
        categories = ", ".join(f"{name}={count}" for name, count in counts.items())
        parts.append(f"values: {categories}")
    return "; ".join(parts)


def format_evaluation_aggregates(value: Any) -> str:
    """Summarize aggregate groups without including per-replay values."""
    if not value:
        return "No evaluation aggregates found."
    summaries = []
    for aggregate in value:
        evaluator = aggregate.get("evaluator_name") or "-"
        if aggregate.get("evaluator_version") is not None:
            evaluator += f"@{aggregate['evaluator_version']}"
        lines = [
            f"{aggregate['name']} ({aggregate['data_type']})",
            f"Evaluator: {evaluator}",
        ]
        if aggregate.get("evaluator_version_id") is not None:
            lines.append(f"Evaluator version ID: {aggregate['evaluator_version_id']}")
        lines.extend(
            (
                f"Baseline: {_format_evaluation_stats(aggregate['baseline'])}",
                f"Result: {_format_evaluation_stats(aggregate['result'])}",
            )
        )
        summaries.append("\n".join(lines))
    return "\n\n".join(summaries)


def format_analyzer_tasks(value: Any) -> str:
    """Format settled analyzer task summaries as concise one-line entries."""
    if not isinstance(value, list) or not value:
        return "-"
    lines: list[str] = []
    for task in value:
        if not isinstance(task, dict):
            lines.append(str(task))
            continue
        label = task.get("analyzer") or task.get("id") or "-"
        status = task.get("status") or "-"
        detail: str | None = None
        if task.get("skip_reason"):
            detail = str(task["skip_reason"])
        elif task.get("error"):
            detail = str(task["error"])
        elif task.get("insight_count") is not None:
            count = task["insight_count"]
            noun = "insight" if count == 1 else "insights"
            detail = f"{count} {noun}"
            if task.get("reason"):
                detail += f": {task['reason']}"
        summary = f"{label}: {status}"
        if detail:
            summary += f" ({detail})"
        if task.get("analyzer") and task.get("id"):
            summary += f" [{task['id']}]"
        lines.append(summary)
    return "\n".join(lines)


def format_evaluator_summary(value: Any) -> str:
    """Format aggregate evaluator task counts for a settled import receipt."""
    if not isinstance(value, dict):
        return str(value)
    completed = value.get("completed_tasks", 0)
    total = value.get("total_tasks", 0)
    parts = [f"{completed}/{total} completed"]
    for key, label in (
        ("failed_tasks", "failed"),
        ("canceled_tasks", "canceled"),
        ("skipped_tasks", "skipped"),
    ):
        count = value.get(key, 0)
        if count:
            parts.append(f"{count} {label}")
    evaluations = value.get("evaluation_count", 0)
    noun = "evaluation" if evaluations == 1 else "evaluations"
    summary = f"{', '.join(parts)} ({evaluations} {noun})"
    if value.get("truncated"):
        summary += " [task list truncated]"
    return summary


def format_evaluator_tasks(value: Any) -> str:
    """Format settled evaluator task summaries as concise one-line entries."""
    if not isinstance(value, list) or not value:
        return "-"
    lines: list[str] = []
    for task in value:
        if not isinstance(task, dict):
            lines.append(str(task))
            continue
        label = task.get("evaluator") or task.get("id") or "-"
        status = task.get("status") or "-"
        detail: str | None = None
        if task.get("skip_reason"):
            detail = str(task["skip_reason"])
        elif task.get("error"):
            detail = str(task["error"])
        elif task.get("evaluation_count") is not None:
            count = task["evaluation_count"]
            noun = "evaluation" if count == 1 else "evaluations"
            detail = f"{count} {noun}"
        summary = f"{label}: {status}"
        if detail:
            summary += f" ({detail})"
        refs: list[str] = []
        if task.get("input_session_id"):
            refs.append(f"session {task['input_session_id']}")
        if task.get("evaluator") and task.get("id"):
            refs.append(f"task {task['id']}")
        if refs:
            summary += f" [{', '.join(refs)}]"
        lines.append(summary)
    return "\n".join(lines)
