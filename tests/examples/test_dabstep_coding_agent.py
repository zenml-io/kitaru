"""Tests for the experimental DABstep coding-agent example."""

import json
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from examples.python.dabstep_coding_agent.convert import convert_trace
from examples.python.dabstep_coding_agent.evaluator import evaluate, matches_answer
from examples.python.dabstep_coding_agent.prepare import prepare_fixture
from examples.python.dabstep_coding_agent.runner import (
    NETWORK_PROBE,
    CodexExecution,
    _capture_codex_output,
    _codex_command,
    _fixture_sha256,
    _load_gold,
    _probe_failed,
    _raise_for_codex_failure,
    _resolve_task_fixture,
    _validate_task_inputs,
)
from examples.python.dabstep_coding_agent.score import score_answer

from kitaru.task.importer import ImportedSession


def test_scores_numeric_answers_with_dabstep_tolerance() -> None:
    receipt = score_answer("7.30001", "7.3")

    assert receipt["passed"] is True
    assert receipt["evaluator"] == "dabstep-v1-compatible"


def test_scores_comma_separated_numeric_value_before_list_matching() -> None:
    receipt = score_answer("1,000", "1000")

    assert receipt["passed"] is True


@pytest.mark.parametrize("answer", ["Applicable", "Not"])
def test_rejects_partial_not_applicable_answers(answer: str) -> None:
    assert matches_answer(answer.lower(), "not applicable") is False


def test_preserves_numeric_sign_when_scoring() -> None:
    assert matches_answer("-7.3", "7.3") is False


def test_evaluator_scores_answer_by_task_id() -> None:
    view = SimpleNamespace(
        session=SimpleNamespace(
            inputs={"task_id": "1273"}, outputs={"answer": "0.120132"}
        )
    )

    result = evaluate(cast(Any, view), expected_by_task={"1273": "0.120132"})

    assert result.name == "dabstep_answer_correct"
    assert result.score is True
    assert result.passed is True


def test_evaluator_rejects_wrong_answer() -> None:
    view = SimpleNamespace(
        session=SimpleNamespace(
            inputs={"task_id": "1273"}, outputs={"answer": "0.105361"}
        )
    )

    result = evaluate(cast(Any, view), expected_by_task={"1273": "0.120132"})

    assert result.score is False
    assert result.passed is False


def test_evaluator_requires_the_complete_list_answer() -> None:
    view = SimpleNamespace(
        session=SimpleNamespace(inputs={"task_id": "1464"}, outputs={"answer": "1, 2"})
    )

    result = evaluate(cast(Any, view), expected_by_task={"1464": "1, 2, 5, 6"})

    assert result.score is False
    assert result.passed is False


def test_refuses_to_overwrite_existing_fixture(tmp_path: Path) -> None:
    destination = tmp_path / "fixture"
    destination.mkdir()

    with pytest.raises(ValueError, match="Refusing to overwrite"):
        prepare_fixture(destination, "1273")


def test_codex_command_does_not_mix_sandbox_with_approve_for_me(tmp_path: Path) -> None:
    command = _codex_command(tmp_path, "inspect the fixture", "gpt-5.4")

    assert "--sandbox" in command
    assert "--approve-for-me" not in command
    assert "--skip-git-repo-check" in command
    assert "sandbox_workspace_write.network_access=false" in command


def test_captured_codex_records_receive_wrapper_timestamps() -> None:
    process = SimpleNamespace(
        stdout=StringIO('{"type":"turn.started"}\nplain diagnostic\n')
    )
    trace = StringIO()

    _capture_codex_output(cast(Any, process), trace)

    records = [json.loads(line) for line in trace.getvalue().splitlines()]
    assert records[0]["type"] == "turn.started"
    assert records[0]["_kitaru_observed_at"].endswith("Z")
    assert records[1]["type"] == "wrapper.output"
    assert records[1]["message"] == "plain diagnostic"


def test_network_probe_requires_failed_command_evidence(tmp_path: Path) -> None:
    trace = tmp_path / "probe.jsonl"
    trace.write_text(
        json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "type": "agent_message",
                    "text": "Network is disabled, so the probe failed.",
                },
            }
        ),
        encoding="utf-8",
    )
    assert _probe_failed(trace) is False

    trace.write_text(
        json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "type": "command_execution",
                    "command": f"/bin/zsh -lc {NETWORK_PROBE}",
                    "aggregated_output": (
                        "urllib.error.URLError: network is unreachable"
                    ),
                    "exit_code": 1,
                    "status": "failed",
                },
            }
        ),
        encoding="utf-8",
    )
    assert _probe_failed(trace) is True


def test_refuses_to_score_failed_codex_execution() -> None:
    execution = CodexExecution(exit_code=124, started_at="start", ended_at="end")

    with pytest.raises(RuntimeError, match="status 124"):
        _raise_for_codex_failure(execution)


def test_accepts_empty_gold_answer_and_checks_task_id(tmp_path: Path) -> None:
    gold = tmp_path / "gold.json"
    gold.write_text(json.dumps({"task_id": "1273", "answer": ""}), encoding="utf-8")

    assert _load_gold(gold, task_id="1273") == ""
    with pytest.raises(ValueError, match="task ID"):
        _load_gold(gold, task_id="1305")


def test_task_mode_refuses_inputs_from_a_different_fixture(tmp_path: Path) -> None:
    fixture = tmp_path / "fixture"
    fixture.mkdir()
    (fixture / "task.json").write_text(
        json.dumps({"task_id": "1273", "question": "Which run wins?"}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="do not match"):
        _validate_task_inputs(
            {"task_id": "1273", "question": "A different question"}, fixture
        )


def test_task_mode_resolves_fixture_from_recorded_task_id(tmp_path: Path) -> None:
    task_root = tmp_path / "fixtures"
    public = task_root / "task-1305" / "public"
    private = task_root / "task-1305" / "private"
    public.mkdir(parents=True)
    private.mkdir()
    (public / "task.json").write_text(
        json.dumps({"task_id": "1305", "question": "Which fee applies?"}),
        encoding="utf-8",
    )
    (private / "gold.json").write_text(
        json.dumps({"task_id": "1305", "answer": "42"}),
        encoding="utf-8",
    )

    fixture, gold = _resolve_task_fixture(
        {"task_id": "1305", "question": "Which fee applies?"}, task_root
    )

    assert fixture == public
    assert gold == private / "gold.json"


def test_task_mode_rejects_unsafe_task_id(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Invalid DABstep task ID"):
        _resolve_task_fixture(
            {"task_id": "../private", "question": "Find the answer"}, tmp_path
        )


def test_task_mode_rejects_changed_fixture_manifest(tmp_path: Path) -> None:
    fixture = tmp_path / "fixture"
    fixture.mkdir()
    (fixture / "task.json").write_text(
        json.dumps({"task_id": "1273", "question": "Which run wins?"}),
        encoding="utf-8",
    )
    original_sha256 = _fixture_sha256(fixture)
    (fixture / "context.csv").write_text("changed", encoding="utf-8")

    with pytest.raises(ValueError, match="fixed DABstep fixture"):
        _validate_task_inputs(
            {
                "task_id": "1273",
                "question": "Which run wins?",
                "fixture_sha256": original_sha256,
            },
            fixture,
        )


def test_converts_tool_trace_and_redacts_local_path(tmp_path: Path) -> None:
    trace_path = tmp_path / "codex.jsonl"
    trace_path.write_text(
        "\n".join(
            (
                json.dumps(
                    {
                        "type": "session_meta",
                        "timestamp": "2026-08-21T10:00:00Z",
                        "payload": {"id": "run-1", "cli_version": "0.1"},
                    }
                ),
                json.dumps(
                    {
                        "type": "response_item",
                        "timestamp": "2026-08-21T10:00:01Z",
                        "payload": {
                            "type": "function_call",
                            "call_id": "call-1",
                            "name": "shell",
                            "arguments": '{"command": "pwd"}',
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "response_item",
                        "timestamp": "2026-08-21T10:00:02Z",
                        "payload": {
                            "type": "function_call_output",
                            "call_id": "call-1",
                            "output": (
                                "/Users/alex/private-workdir "
                                "/private/var/folders/qf/demo/T/workdir/answer.txt"
                            ),
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "timestamp": "2026-08-21T10:00:03Z",
                        "payload": {
                            "type": "agent_message",
                            "message": "The answer is 7.3.",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "wrapper.output",
                        "message": "ERROR model catalog cache unavailable",
                        "_kitaru_observed_at": "2026-08-21T10:00:04Z",
                    }
                ),
            )
        ),
        encoding="utf-8",
    )

    session = convert_trace(
        trace_path,
        task={"task_id": "1273", "question": "Which run wins?"},
        answer="7.3",
        score_receipt={"passed": True},
        run_metadata={"network_probe_passed": True},
    )

    assert session["outputs"]["answer"] == "7.3"
    assert session["metadata"]["codex_jsonl"]["session_id"] == "run-1"
    assert session["metadata"]["redaction"]["applied"] is True
    assert session["nodes"][0]["name"] == "Codex agent run"
    assert session["nodes"][1]["node_type"] == "tool_call"
    assert session["nodes"][-1]["name"] == "Codex diagnostic"
    assert session["nodes"][-1]["status"] == "failed"
    assert "<redacted-home>" in json.dumps(session)
    assert "<redacted-temp>" in json.dumps(session)
    ImportedSession.model_validate(session)


def test_converts_current_codex_exec_jsonl_items(tmp_path: Path) -> None:
    trace_path = tmp_path / "codex-exec.jsonl"
    trace_path.write_text(
        "\n".join(
            (
                json.dumps(
                    {
                        "type": "thread.started",
                        "thread_id": "thread-1",
                        "_kitaru_observed_at": "2026-08-21T10:00:00Z",
                    }
                ),
                json.dumps(
                    {
                        "type": "item.completed",
                        "_kitaru_observed_at": "2026-08-21T10:00:01Z",
                        "item": {
                            "type": "error",
                            "id": "warning-1",
                            "message": (
                                "`--dangerously-bypass-hook-trust` is enabled. "
                                "Enabled hooks may run without review for this "
                                "invocation."
                            ),
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "item.started",
                        "_kitaru_observed_at": "2026-08-21T10:00:02Z",
                        "item": {
                            "type": "command_execution",
                            "id": "command-1",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "item.completed",
                        "_kitaru_observed_at": "2026-08-21T10:00:03Z",
                        "item": {
                            "type": "command_execution",
                            "id": "command-1",
                            "command": "python analysis.py",
                            "aggregated_output": "0.117667",
                            "exit_code": 0,
                            "status": "completed",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "item.completed",
                        "_kitaru_observed_at": "2026-08-21T10:00:04Z",
                        "item": {
                            "type": "agent_message",
                            "id": "message-1",
                            "text": "The answer is 0.117667.",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "turn.completed",
                        "_kitaru_observed_at": "2026-08-21T10:00:05Z",
                        "usage": {
                            "input_tokens": 162619,
                            "cached_input_tokens": 144512,
                            "output_tokens": 2099,
                            "reasoning_output_tokens": 382,
                        },
                    }
                ),
            )
        ),
        encoding="utf-8",
    )

    session = convert_trace(
        trace_path,
        task={"task_id": "1273", "question": "Which run wins?"},
        answer="0.117667",
        score_receipt={"passed": False},
        run_metadata={
            "exit_code": 0,
            "invocation_prompt": "Solve the task from the local files.",
            "skill_name": "analysis-b.md",
            "skill_sha256": "abc123",
            "skill_content": "Always apply null wildcard rules.",
            "model": "gpt-5.4",
            "model_provider": "openai",
        },
    )

    assert session["metadata"]["codex_jsonl"]["session_id"] == "thread-1"
    assert [node["node_type"] for node in session["nodes"]] == [
        "span",
        "span",
        "tool_call",
        "span",
    ]
    root = session["nodes"][0]
    assert root["inputs"]["skill"]["content"] == ("Always apply null wildcard rules.")
    assert root["tokens"] == {
        "input_tokens": 162619,
        "output_tokens": 2099,
        "cached_input_tokens": 144512,
        "reasoning_tokens": 382,
    }
    assert root["cost"] == "0.1128805"
    assert session["nodes"][1]["name"] == "Codex warning"
    assert session["nodes"][1]["status"] == "completed"
    assert session["nodes"][2]["inputs"] == {"command": "python analysis.py"}
    assert session["nodes"][2]["outputs"] == {
        "output": "0.117667",
        "exit_code": 0,
    }
    assert session["nodes"][2]["started_at"] == "2026-08-21T10:00:02Z"
    assert session["nodes"][2]["ended_at"] == "2026-08-21T10:00:03Z"
    assert session["metadata"]["intervention"]["content"] == (
        "Always apply null wildcard rules."
    )
    assert session["metadata"]["invocation"]["system_prompt"]["available"] is False
    ImportedSession.model_validate(session)
