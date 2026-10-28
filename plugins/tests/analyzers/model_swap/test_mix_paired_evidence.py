"""Mix decisions require comparable evidence from the reference model."""

from dataclasses import replace

import pytest

from kitaru_model_swap.calls import RecordedCall
from kitaru_model_swap.html import render_section
from kitaru_model_swap.mix import is_learning_session
from kitaru_model_swap.normalize import ToolCall
from kitaru_model_swap.runner import CallResult

from .test_mix_weighting import call, reply, report


def failed_reply(recorded: RecordedCall, name: str) -> CallResult:
    result = reply(recorded, name)
    assert result.reply is not None
    return replace(
        result,
        agreement=None,
        reply=replace(result.reply, tool_calls=(), failure="http_error"),
    )


@pytest.mark.parametrize("missing", ["max_spend", "http_error"])
def test_missing_reference_holdout_keeps_today_and_preserves_other_steps(
    missing: str,
) -> None:
    common = [call(1000 + i, "common") for i in range(80)]
    rare = [call(i, "rare") for i in range(40)]
    calls = common + rare
    results = []
    for recorded in calls:
        if recorded.node_name == "common" or is_learning_session(recorded):
            results.append(reply(recorded, "reference", "1"))
        elif missing == "max_spend":
            results.append(CallResult(recorded, "reference", not_run="max_spend"))
        else:
            results.append(failed_reply(recorded, "reference"))
        results.append(reply(recorded, "cheap"))

    built = report(calls, results)
    mix = built["best_mix"]
    common_group, rare_group = mix["groups"]

    assert common_group["candidate"] == "cheap"
    assert rare_group["candidate"] is None
    assert rare_group["held_out"] == "too_few"
    assert rare_group["held_out_rate"] is None
    assert rare_group["held_out_calls"] == 0
    assert rare_group["cost_per_call_usd"] == "1.00000000"
    assert "today&#x27;s model (too few comparable held-out answers to confirm)" in (
        render_section(built, dashboard_url=None)
    )


@pytest.mark.parametrize("disagreement_half", ["learning", "held_out", "both"])
def test_mix_compares_shared_answers_in_each_split(disagreement_half: str) -> None:
    pool = [call(i) for i in range(500)]
    learning = [recorded for recorded in pool if is_learning_session(recorded)][:100]
    held_out = [recorded for recorded in pool if not is_learning_session(recorded)][
        :100
    ]
    calls = learning + held_out
    results = []
    for half_name, half in (("learning", learning), ("held_out", held_out)):
        for index, recorded in enumerate(half):
            results.append(
                reply(recorded, "reference", "1")
                if index < 85
                else failed_reply(recorded, "reference")
            )
            candidate = (
                failed_reply(recorded, "cheap")
                if index < 15
                else reply(recorded, "cheap")
            )
            if 15 <= index < 23 and disagreement_half in {half_name, "both"}:
                assert candidate.reply is not None
                candidate = replace(
                    candidate,
                    agreement="different_tool",
                    reply=replace(candidate.reply, tool_calls=(ToolCall("wrong", {}),)),
                )
            results.append(candidate)

    built = report(calls, results)
    mix = built["best_mix"]

    assert mix is None or all(group["candidate"] is None for group in mix["groups"])
    if disagreement_half == "held_out":
        assert mix["groups"][0]["held_out"] == "failed"
    if disagreement_half == "both":
        assert built["candidates"][1]["paired"]["rate"] == 0.8857
