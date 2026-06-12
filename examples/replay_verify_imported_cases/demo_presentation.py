"""Plain-language presentation layer for the imported-input Replay Verify demo.

This module is presentation only. It reads the verification summary that the
core Replay Verify logic already produces and renders a legible, audience-ready
terminal panel. It does not decide verdicts, run cases, or change any
verify/replay behavior — every number it shows comes straight from the summary.

The audience is partly non-technical, so machine stop-reason codes are
translated into short plain-English sentences. Unknown codes degrade gracefully
to a humanized version of the code instead of leaking raw debug text.
"""

from __future__ import annotations

from typing import Any

# Exact machine stop-reason code -> plain-English explanation.
_REASON_TEXT: dict[str, str] = {
    "missing_observed_output_or_evaluator_signal": (
        "No recorded result to compare against — we can't tell what the agent "
        "actually did, so there's nothing to safely check."
    ),
    "missing_available_tools": (
        "The case never recorded which tools the agent was allowed to use, so "
        "we can't confirm the new version would stay inside the same limits."
    ),
    "missing_root_input": (
        "The original customer request is missing, so the case can't be re-run."
    ),
    "missing_trace_id": (
        "The case has no trace reference, so it can't be tied back to a real run."
    ),
    "missing_local_runner": (
        "No runnable agent is wired up for this case, so it can't be executed."
    ),
    "unsafe_or_unknown_write_like_tool_blocked": (
        "The agent tried to take a real-world action (one that changes data) "
        "that we couldn't verify as safe — so we stopped before re-running it."
    ),
    "ambiguous_side_effect_status_write_like_tool": (
        "It's unclear whether an action would change real data, so we held the "
        "case rather than risk an unverified change."
    ),
    "baseline_unsafe_live_execution_detected": (
        "Re-running the case would have triggered a real live action, so we "
        "stopped instead of letting it execute."
    ),
    "stale_corpus_index_version": (
        "The answer relied on a knowledge base that has since changed, so the "
        "comparison would no longer be apples-to-apples."
    ),
    "baseline_runner_failed": (
        "The current version of the agent errored on this case, so there's no "
        "trustworthy baseline to compare against."
    ),
    "candidate_runner_failed": (
        "The new version of the agent errored on this case, so it can't be "
        "judged safe to switch to."
    ),
    "permission_scope_mismatch": (
        "The case's access permissions don't line up, so re-running it could "
        "cross a boundary it shouldn't."
    ),
    "permission_mismatch_cross_tenant_document": (
        "The case referenced data from a different account, so we held it rather "
        "than risk crossing accounts."
    ),
}

# Prefixed codes of the form "<prefix>:<detail>" -> template using the detail.
_REASON_PREFIX_TEXT: dict[str, str] = {
    "unknown_tool": (
        "The agent used a tool we don't recognize ({detail}), so we couldn't "
        "confirm it was safe."
    ),
    "missing_rag_metadata": (
        "The answer cited knowledge-base sources but the supporting evidence was "
        "incomplete ({detail} was missing), so we couldn't trust the citation."
    ),
    "missing_recorded_tool_call": (
        "A tool the agent was expected to use ({detail}) had no recorded result, "
        "so the case couldn't be reproduced."
    ),
    "missing_recorded_write_like_tool_call": (
        "An action that changes data ({detail}) had no recorded result, so we "
        "couldn't verify what it did."
    ),
    "observed_tool_not_available": (
        "The agent used a tool ({detail}) that isn't in the allowed set, so we "
        "couldn't confirm it stays within bounds."
    ),
    "observed_write_like_tool_not_available": (
        "The agent took a real-world action ({detail}) that isn't in the allowed "
        "set, so we stopped before re-running it."
    ),
}


def plain_english_reason(code: str) -> str:
    """Translate one machine stop-reason code into a plain-English sentence."""
    if code in _REASON_TEXT:
        return _REASON_TEXT[code]
    if ":" in code:
        prefix, _, detail = code.partition(":")
        template = _REASON_PREFIX_TEXT.get(prefix)
        detail_text = detail.replace("_", " ").strip() or "unspecified"
        if template is not None:
            return template.format(detail=detail_text)
    # Graceful fallback: never show a raw snake_case code to the audience.
    return code.replace("_", " ").replace(":", ": ").strip().capitalize() + "."


# Reasons that signal an unverified real-world action — the strongest business
# message, so they win over generic "unknown tool" wording when both are present.
_WRITE_ACTION_REASONS = {
    "unsafe_or_unknown_write_like_tool_blocked",
    "baseline_unsafe_live_execution_detected",
    "ambiguous_side_effect_status_write_like_tool",
}
_WRITE_ACTION_PREFIXES = (
    "observed_write_like_tool_not_available",
    "missing_recorded_write_like_tool_call",
)


def _action_name(reasons: list[str]) -> str | None:
    """Recover the tool/action name from whichever reason carries it."""
    for code in reasons:
        prefix, _, detail = code.partition(":")
        if detail and prefix in {
            "unknown_tool",
            "observed_write_like_tool_not_available",
            "observed_tool_not_available",
            "missing_recorded_write_like_tool_call",
        }:
            return detail.replace("_", " ").strip()
    return None


def _held_reason_sentence(reasons: list[str]) -> str:
    """Pick the single clearest reason to show for a held case.

    An unverified real-world action (e.g. issuing a refund) is the most
    important thing for a business audience to hear, so it is surfaced ahead of
    lower-level codes even when it is not the first reason in the list.
    """
    if not reasons:
        return "Held because the evidence couldn't be trusted."
    codes = [str(r) for r in reasons]
    is_write_action = any(c in _WRITE_ACTION_REASONS for c in codes) or any(
        c.partition(":")[0] in _WRITE_ACTION_PREFIXES for c in codes
    )
    if is_write_action:
        action = _action_name(codes)
        detail = f" — {action} — " if action else " "
        return (
            f"The agent tried to take a real-world action{detail}it couldn't "
            "verify as safe, so we stopped before re-running it."
        )
    return plain_english_reason(codes[0])


def render_demo_summary(summary: dict[str, Any]) -> str:
    """Render the audience-facing ship/hold panel from a verification summary.

    Args:
        summary: The verification report summary produced by the core
            Replay Verify runner. All displayed numbers are read from it.

    Returns:
        A multi-line string ready to print to a terminal.
    """
    verdicts = summary.get("verdict_counts", {}) or {}
    ship = int(verdicts.get("ship", 0))
    hold = int(verdicts.get("hold", 0))
    caution = int(verdicts.get("caution", 0))
    imported = int(summary.get("imported_count", ship + hold + caution))
    unsafe_live = int(summary.get("unsafe_live_execution_count", 0))
    stopped_for_candidate = int(
        summary.get("candidate_executions_for_stopped_cases", 0)
    )
    stopped_reasons: dict[str, Any] = summary.get("stopped_case_reasons", {}) or {}

    bar = "=" * 64
    out: list[str] = [
        "",
        bar,
        "  REPLAY VERIFY  —  is the cheaper model safe to switch to?",
        bar,
        "",
        f"  Checked {imported} imported support cases.",
        "",
        f"  SHIP   {ship:>2}   safe to switch to the new model",
        f"  HOLD   {hold:>2}   held back — evidence couldn't be trusted",
    ]
    if caution:
        out.append(f"  CAUTION {caution:>1}   needs a closer look before switching")

    if stopped_reasons:
        out.extend(["", "  Held cases — and why, in plain English:", ""])

        def _hold_sort_key(case_id: str) -> tuple[int, str]:
            raw = stopped_reasons[case_id]
            codes = [str(r) for r in (raw if isinstance(raw, list) else [])]
            is_action = any(c in _WRITE_ACTION_REASONS for c in codes) or any(
                c.partition(":")[0] in _WRITE_ACTION_PREFIXES for c in codes
            )
            # Unverified real-world actions lead; the rest stay alphabetical.
            return (0 if is_action else 1, case_id)

        for case_id in sorted(stopped_reasons, key=_hold_sort_key):
            raw = stopped_reasons[case_id]
            reasons = raw if isinstance(raw, list) else []
            out.append(f"  HOLD  {case_id}")
            out.append(f"        {_held_reason_sentence(reasons)}")
            out.append("")

    # The trust guarantee: the held cases never ran the candidate, and nothing
    # unsafe slipped through. Both numbers come from the real run.
    out.extend(
        [
            "  Safety guarantee (from this run):",
            f"    - Held cases that still ran the new model:  {stopped_for_candidate}",
            f"    - Unsafe actions allowed to execute:        {unsafe_live}",
            bar,
            "",
        ]
    )
    return "\n".join(out)


def render_business_summary(summary: dict[str, Any]) -> str:
    """Render a one-paragraph plain-language summary for a business audience.

    Every number is read from the actual run summary — nothing is hardcoded.
    """
    verdicts = summary.get("verdict_counts", {}) or {}
    ship = int(verdicts.get("ship", 0))
    hold = int(verdicts.get("hold", 0))
    imported = int(summary.get("imported_count", ship + hold))
    unsafe_live = int(summary.get("unsafe_live_execution_count", 0))
    stopped_reasons: dict[str, Any] = summary.get("stopped_case_reasons", {}) or {}

    # Find held cases that tried an unverified real-world action, to name one.
    action_name: str | None = None
    action_count = 0
    for raw in stopped_reasons.values():
        codes = [str(r) for r in (raw if isinstance(raw, list) else [])]
        is_action = any(c in _WRITE_ACTION_REASONS for c in codes) or any(
            c.partition(":")[0] in _WRITE_ACTION_PREFIXES for c in codes
        )
        if is_action:
            action_count += 1
            if action_name is None:
                action_name = _action_name(codes)

    sentences = [
        f"Checked {imported} imported support cases against a cheaper model.",
        f"{ship} safe to switch.",
    ]
    held = f"{hold} held back"
    if action_count and action_name:
        held += (
            f" — including {action_count} where the agent tried to take an "
            f"action it couldn't verify ({action_name})"
        )
    elif action_count:
        held += f" — including {action_count} that attempted an unverified action"
    sentences.append(held + ".")
    sentences.append(
        "Nothing unsafe was passed through."
        if unsafe_live == 0
        else f"WARNING: {unsafe_live} unsafe action(s) executed."
    )
    return "  IN PLAIN ENGLISH:  " + " ".join(sentences)
