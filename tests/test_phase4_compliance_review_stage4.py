"""Guarded tests for the compliance review Stage 4 wait/resume flow."""

from __future__ import annotations

import importlib
import threading
import time
from contextlib import suppress
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from kitaru import KitaruClient
from kitaru.errors import KitaruFeatureNotAvailableError, KitaruStateError
from kitaru.wait import _resolve_zenml_wait
from tests.compliance_review_fakes import (
    clear_compliance_review_modules,
    fake_claude_response,
    install_fake_claude_agent_sdk,
)

_WAIT_DISCOVERY_TIMEOUT_SECONDS = 900.0


@pytest.fixture
def stage4_module(monkeypatch):
    """Import Stage 4 with a fake Claude SDK module."""
    install_fake_claude_agent_sdk(monkeypatch)
    clear_compliance_review_modules(
        "examples.compliance_review.stage_4_conversational",
    )
    return importlib.import_module("examples.compliance_review.stage_4_conversational")


def _find_pending_wait(
    *,
    client: KitaruClient,
    conversation_label: str,
    turn_number: int,
) -> str | None:
    """Return the exec_id for the Stage 4 run waiting at a given turn."""
    executions = client.executions.list(
        flow="conversational_compliance_review",
        limit=30,
    )
    for execution in executions:
        detailed = client.executions.get(execution.exec_id)
        if detailed.pending_wait is None:
            continue
        metadata = detailed.pending_wait.metadata
        if metadata.get("conversation_label") != conversation_label:
            continue
        if metadata.get("turn_number") != turn_number:
            continue
        return detailed.exec_id
    return None


def _wait_for_pending_wait(
    *,
    client: KitaruClient,
    conversation_label: str,
    turn_number: int,
    state: dict[str, object],
) -> str:
    """Poll until the Stage 4 flow reaches the expected pending wait."""
    deadline = time.time() + _WAIT_DISCOVERY_TIMEOUT_SECONDS
    while time.time() < deadline:
        if state["error"] is not None:
            raise RuntimeError(
                "Flow run failed before reaching a wait condition."
            ) from state["error"]  # type: ignore[arg-type]
        try:
            found = _find_pending_wait(
                client=client,
                conversation_label=conversation_label,
                turn_number=turn_number,
            )
        except ValueError:
            found = None
        if found is not None:
            return found
        time.sleep(0.5)

    raise TimeoutError(
        f"Timed out after {_WAIT_DISCOVERY_TIMEOUT_SECONDS:.0f}s waiting for "
        f"Stage 4 turn {turn_number} to reach a pending wait."
    )


def test_stage4_checkpoint_resumes_existing_claude_session(
    monkeypatch,
    stage4_module,
) -> None:
    """The checkpoint wrapper should pass the prior session ID as `resume`."""
    calls: list[dict[str, Any]] = []
    log_calls: list[dict[str, Any]] = []
    prior = stage4_module.ClaudeAgentResult(
        session_id="existing-claude-session",
        cwd=str(stage4_module.EXAMPLE_DIR),
        transcript_path="/tmp/existing-claude-session.jsonl",
        result="Previous turn",
        num_turns=1,
    )

    async def fake_run_agent_turn(
        prompt: str,
        *,
        allowed_tools: list[str],
        resume: str | None,
        cwd: Path,
    ) -> dict[str, Any]:
        calls.append(
            {
                "prompt": prompt,
                "allowed_tools": allowed_tools,
                "resume": resume,
                "cwd": cwd,
            }
        )
        return fake_claude_response(
            prompt=prompt,
            cwd=cwd,
            session_id=resume or "new-claude-session",
            result="Follow-up answer",
        )

    monkeypatch.setattr(stage4_module, "run_agent_turn", fake_run_agent_turn)
    monkeypatch.setattr(
        stage4_module.kitaru,
        "log",
        lambda **kwargs: log_calls.append(kwargs),
    )

    result = stage4_module.run_claude_agent.__wrapped__(
        "Explain the HR remediation plan.",
        prior,
    )

    assert isinstance(result, stage4_module.ClaudeAgentResult)
    assert result.session_id == "existing-claude-session"
    assert result.result == "Follow-up answer"
    assert calls == [
        {
            "prompt": "Explain the HR remediation plan.",
            "allowed_tools": stage4_module.DEFAULT_ALLOWED_TOOLS,
            "resume": "existing-claude-session",
            "cwd": stage4_module.EXAMPLE_DIR,
        }
    ]
    assert log_calls == [
        {
            "stage": "stage_4_conversational",
            "checkpoint_boundary": "one_claude_turn",
            "resumed": True,
            "resume_session_id": "existing-claude-session",
            "session_id": "existing-claude-session",
        }
    ]


def test_stage4_flow_waits_resumes_and_reuses_session(
    monkeypatch,
    primed_zenml,
    stage4_module,
) -> None:
    """The real flow should pause, accept input, and resume the same session."""
    del primed_zenml
    try:
        _resolve_zenml_wait()
    except KitaruFeatureNotAvailableError:
        pytest.skip("Installed ZenML build does not expose wait support yet.")

    calls: list[dict[str, Any]] = []
    session_id = "stage-4-test-session"

    async def fake_run_agent_turn(
        prompt: str,
        *,
        allowed_tools: list[str],
        resume: str | None,
        cwd: Path,
    ) -> dict[str, Any]:
        turn_number = len(calls) + 1
        calls.append(
            {
                "prompt": prompt,
                "allowed_tools": allowed_tools,
                "resume": resume,
                "cwd": cwd,
            }
        )
        return fake_claude_response(
            prompt=prompt,
            cwd=cwd,
            session_id=resume or session_id,
            result=f"Stage 4 turn {turn_number} result.",
        )

    monkeypatch.setattr(stage4_module, "run_agent_turn", fake_run_agent_turn)

    initial_prompt = "Start Stage 4 test review."
    follow_up = "Please explain the remediation priority."
    conversation_label = f"stage4-test-{uuid4()}"
    client = KitaruClient()
    state: dict[str, object] = {"handle": None, "error": None}

    def _runner() -> None:
        try:
            state["handle"] = stage4_module.conversational_compliance_review.run(
                initial_prompt,
                conversation_label,
                None,
                cache=False,
            )
        except Exception as exc:
            state["error"] = exc

    starter = threading.Thread(target=_runner, name="test-stage4-starter", daemon=True)
    starter.start()

    try:
        # --- Wait 1: after the initial Claude turn ---
        exec_id = _wait_for_pending_wait(
            client=client,
            conversation_label=conversation_label,
            turn_number=1,
            state=state,
        )
        pending = client.executions.pending_waits(exec_id)
        assert len(pending) == 1
        assert pending[0].name == "compliance_follow_up_1"
        assert pending[0].schema is not None
        assert pending[0].schema.get("type") == "string"
        assert pending[0].metadata["session_id"] == session_id

        client.executions.input(
            exec_id,
            wait=pending[0].wait_id,
            value=follow_up,
        )
        with suppress(KitaruStateError):
            client.executions.resume(exec_id)

        # --- Wait 2: after the follow-up Claude turn in the same session ---
        exec_id = _wait_for_pending_wait(
            client=client,
            conversation_label=conversation_label,
            turn_number=2,
            state=state,
        )
        pending = client.executions.pending_waits(exec_id)
        assert len(pending) == 1
        assert pending[0].name == "compliance_follow_up_2"
        assert pending[0].metadata["session_id"] == session_id

        client.executions.input(
            exec_id,
            wait=pending[0].wait_id,
            value="/done",
        )
        with suppress(KitaruStateError):
            client.executions.resume(exec_id)
    finally:
        starter.join(timeout=60.0)

    assert not starter.is_alive(), "Background flow-start thread did not finish."
    assert state["error"] is None, f"Background flow start failed: {state['error']}"

    handle = state["handle"]
    assert handle is not None, "Flow handle was not captured from background run."

    result = handle.wait()
    assert isinstance(result, stage4_module.ClaudeAgentResult)
    assert result.session_id == session_id
    assert result.result == "Stage 4 turn 2 result."
    assert calls == [
        {
            "prompt": initial_prompt,
            "allowed_tools": stage4_module.DEFAULT_ALLOWED_TOOLS,
            "resume": None,
            "cwd": stage4_module.EXAMPLE_DIR,
        },
        {
            "prompt": follow_up,
            "allowed_tools": stage4_module.DEFAULT_ALLOWED_TOOLS,
            "resume": session_id,
            "cwd": stage4_module.EXAMPLE_DIR,
        },
    ]

