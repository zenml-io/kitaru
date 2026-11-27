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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Deterministic investigation briefings for core profiling findings."""

from kitaru_post_import_insights.findings import (
    Histogram,
    calculate_percent,
    format_briefing,
    pluralize,
)


def _format_ratio(numerator: int, denominator: int) -> str:
    """Format a per-session average with at most one decimal place."""
    if not denominator:
        return "0"
    return f"{numerator / denominator:.1f}".removesuffix(".0")


def build_signal_briefing(
    *,
    family: str,
    subject: str,
    affected: int,
    analyzed: int,
    share: int | float,
    occurrences: int,
    chart_groups: int,
    leading_category_count: int,
    excluded_sessions: int,
) -> str:
    """Build the briefing for a trajectory, tool-health, or language signal."""
    odd = (
        f"{pluralize(occurrences, 'occurrence')} of {subject} across "
        f"{pluralize(affected, 'session')}, which is {share}% of the "
        f"{pluralize(analyzed, 'session')} analyzed for this marker."
    )
    if chart_groups > 1:
        odd += (
            f" The chart splits them into {chart_groups} groups; the leading group "
            f"alone accounts for {leading_category_count} occurrences."
        )
    else:
        odd += (
            " All occurrences fall in one group, so the chart only compares "
            "matching sessions with the other sessions."
        )
    if excluded_sessions:
        odd += (
            f" {pluralize(excluded_sessions, 'session was', 'sessions were')} left "
            "out because their user text was missing or not fully inspected."
        )
    marker_look_first = (
        "Open the sessions named in evidence_locators; each locator points at "
        "the node where the marker was recorded. Compare them with analyzed "
        "sessions that have no occurrence"
    )
    if family == "language":
        look_first = (
            "Open the sessions named in evidence_locators; when a locator's node_id "
            "is null the matching text is in the session's recorded inputs, "
            "otherwise it is in that node's inputs. First confirm that the matching "
            "text is customer input rather than agent policy or harness instructions. "
            "Compare them with analyzed sessions that have no occurrence, and "
            "read the agent turn immediately before each flagged user message."
        )
        cohort = (
            f"sessions with at least one user message matching this marker (the "
            f"{affected} listed), versus sessions with fully inspected user text and "
            "no match."
        )
        hypothesis = (
            "Check whether the flagged messages follow one recognizable agent turn, "
            "such as an unanswered question, a wrong tool result, or a repeated "
            "clarification request. If they do, one agent change aimed at that turn "
            "is the candidate to replay."
        )
        held = (
            "the marker recurs in fewer replayed sessions than the "
            f"{pluralize(affected, 'session')} it appears in now, and the "
            "failed-session rate for the cohort does not rise."
        )
    elif family == "tool_health":
        look_first = marker_look_first + (
            ", starting with the chart's leading group when it dominates the count."
            if chart_groups > 1
            else "."
        )
        cohort = (
            f"sessions with at least one recorded occurrence of {subject} (the "
            f"{affected} listed)"
        )
        cohort += (
            ", optionally narrowed to the chart's leading group."
            if chart_groups > 1
            else "."
        )
        hypothesis = (
            "Check whether the affected calls fail or come back empty for the same "
            "inputs every time. If they do, the tool contract or the way the agent "
            "builds its arguments is the single factor to change and replay."
        )
        held = (
            f"recorded occurrences per session fall below the current average of "
            f"{_format_ratio(occurrences, affected)}, the affected share drops below "
            f"{share}%, and the failed-session rate does not rise."
        )
    else:
        look_first = (
            marker_look_first
            + ", and check what the agent saw between one call and its repeat."
        )
        cohort = (
            f"sessions with at least one recorded occurrence of {subject} (the "
            f"{affected} listed), versus sessions with tool calls and no repeat."
        )
        hypothesis = (
            "Check whether the repeat follows a tool result the agent could not act "
            "on, such as an error with no guidance or an unchanged answer. If it "
            "does, the tool's error message or the agent's retry instruction is the "
            "single factor to change and replay."
        )
        held = (
            f"repeated calls per affected session fall below the current average of "
            f"{_format_ratio(occurrences, affected)}, tool calls per session drop, "
            "and the failed-session rate does not rise."
        )
    return format_briefing(
        odd=odd,
        look_first=look_first,
        cohort=cohort,
        hypothesis=hypothesis,
        held=held,
    )


def build_outcome_briefing(
    *,
    failed: int,
    completed: int,
    analyzed: int,
    failed_percent: int | float,
    statuses: int,
) -> str:
    """Build the briefing for the recorded session outcome finding."""
    odd = (
        f"{pluralize(failed, 'session')} out of {analyzed} analyzed "
        f"({failed_percent}%) {'carries' if failed == 1 else 'carry'} the recorded "
        "status failed."
    )
    if completed:
        odd += f" {completed} are recorded completed."
    if statuses > 2:
        odd += f" The chart shows {statuses} distinct recorded statuses in total."
    if completed:
        look_first = (
            "Open the failed sessions listed in contributing_session_ids and read "
            "their last recorded node. Compare each with a completed analyzed "
            "session that starts with a similar input."
        )
    else:
        look_first = (
            "Open the failed sessions listed in contributing_session_ids and read "
            "their last recorded node. None of the analyzed sessions completed, so "
            "pick the comparison set from other sessions of this agent or from "
            "another agent version."
        )
    return format_briefing(
        odd=odd,
        look_first=look_first,
        cohort=f"sessions whose recorded status is failed (the {failed} listed).",
        hypothesis=(
            "Check whether the failed sessions share the same final step, such as "
            "one tool name and one error, right before the status flips to failed. "
            "If they do, that step is the single factor to change and replay."
        ),
        held=(
            f"the failed share of the replayed cohort falls below {failed_percent}% "
            "without a rise in duration or cost per session."
        ),
    )


def build_distribution_briefing(
    *,
    family: str,
    values: Histogram,
    sessions_analyzed: int,
    unit: str,
) -> str:
    """Build the briefing for an activity or timing distribution finding."""
    quantity = "recorded duration" if family == "timing" else f"{unit} per session"
    reading = "recorded duration" if family == "timing" else f"{unit} count"
    observations = len(values)
    minimum = f"{values.minimum:g}"
    maximum = f"{values.maximum:g}"
    tail_index = values.get_highest_occupied_bin()
    assert tail_index is not None
    tail_count = values.bins[tail_index]
    tail_lower = f"{values.bounds[tail_index - 1]:g}" if tail_index else None
    if tail_index == len(values.bounds):
        bin_name, bin_adjective = "top bin", "top-bin"
        bin_label = f"{tail_lower} or more {unit}"
    else:
        bin_name, bin_adjective = "highest occupied bin", "highest-bin"
        bin_label = (
            f"at least {tail_lower} and " if tail_lower is not None else ""
        ) + f"less than {values.bounds[tail_index]:g} {unit}"
    rest = observations - tail_count
    odd = (
        f"{observations} sessions range from {minimum} to {maximum} {unit}. "
        f"{pluralize(tail_count, 'session sits', 'sessions sit')} in the "
        f"{bin_name} ({bin_label})"
        + (
            f"; the other {rest} sit below it."
            if rest
            else "; all observations share this bin despite their different values."
        )
    )
    if observations < sessions_analyzed:
        odd += (
            f" Of the {sessions_analyzed} sessions analyzed, "
            f"{sessions_analyzed - observations} had no {reading}."
        )
    look_first = (
        "Open evidence_locators in their listed order: they point to sessions with "
        f"the highest recorded values in the {bin_name}, ranked by value descending "
        "and session UUID ascending for ties. Check evidence_scope for available "
        "versus retained counts and truncation. Other contributing_session_ids "
        "describe the wider distribution; they are not guaranteed to belong to "
        "this bin or form a comparison group. "
        + (
            "Compare them with sessions from lower occupied bins; read each "
            f"comparison session's {reading} to place it."
            if rest
            else "All observations share one bin; compare their actual recorded values."
        )
    )
    interval = f"at least {tail_lower}" if tail_lower is not None else ""
    if tail_index < len(values.bounds):
        interval += (" and " if interval else "") + (
            f"less than {values.bounds[tail_index]:g}"
        )
    if family == "timing":
        cohort = (
            f"sessions whose recorded duration is {interval} seconds (the {bin_name})."
        )
        hypothesis = (
            "Check whether the long sessions spend their time in a few slow nodes, "
            "such as one tool wait or one model call, rather than in more steps. "
            "If they do, that node is the single factor to change and replay."
        )
        held = (
            f"the {bin_adjective} cohort's recorded duration falls below {tail_lower} "
            "seconds for most sessions while the failed-session rate and cost per "
            "session do not rise."
        )
    else:
        cohort = f"sessions with {interval} {unit} (the {bin_name})."
        hypothesis = (
            f"Check whether the {bin_adjective} sessions reach the same outcome as the "
            f"lower-value sessions or whether the extra {unit} are repeats and "
            "retries. If they are repeats, the step that triggers them is the "
            "single factor to change and replay."
        )
        held = (
            f"the {bin_adjective} cohort's {unit} per session fall below {tail_lower} "
            "for most sessions while the failed-session rate does not rise."
        )
    if not rest:
        hypothesis = (
            f"Check whether sessions with higher {quantity} contain avoidable waits "
            "or repeated steps. If confirmed, change that one factor and replay "
            "the cohort against its recorded baseline."
        )
        held = (
            f"the cohort's {quantity} decreases against its recorded baseline while "
            "the failed-session rate does not rise."
        )
    return format_briefing(
        odd=odd,
        look_first=look_first,
        cohort=cohort,
        hypothesis=hypothesis,
        held=held,
    )


def build_model_mix_briefing(
    *,
    models: int,
    model_calls: int,
    leading_model_calls: int,
    affected: int,
    analyzed: int,
) -> str:
    """Build the briefing for the recorded model mix finding."""
    leading_share = calculate_percent(leading_model_calls, model_calls)
    odd = (
        f"{pluralize(models, 'distinct recorded model label')} across "
        f"{pluralize(model_calls, 'model call')} in {affected} of {analyzed} "
        f"sessions. The most frequent label accounts for "
        f"{pluralize(leading_model_calls, 'call')} ({leading_share}%)."
    )
    look_first = (
        "Open sessions from contributing_session_ids and group them by the model "
        "label on their model-call nodes. Compare sessions with at least one "
        "model-call node recording the chart's leading label against sessions "
        "with none."
    )
    cohort = (
        "sessions with at least one model-call node recording the chart's "
        f"leading label ({leading_model_calls} of {model_calls} calls)."
    )
    return format_briefing(
        odd=odd,
        look_first=look_first,
        cohort=cohort,
        hypothesis=(
            "Check whether swapping the leading model for one candidate changes "
            "outcome or cost. Run one replay with a single model override and no "
            "other change."
        ),
        held=(
            "cost per session or the failed-session rate falls on the replayed "
            "cohort while the other one does not rise."
        ),
    )
