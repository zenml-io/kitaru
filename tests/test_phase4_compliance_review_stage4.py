"""Guarded tests for the compliance review Stage 4 wait/resume flow."""

from __future__ import annotations

import importlib
import shutil
import sys
import threading
import time
from contextlib import suppress
from pathlib import Path
from typing import Any
from unittest.mock import Mock
from uuid import uuid4

import pytest
from zenml.utils.source_utils import get_source_root

from kitaru import KitaruClient
from kitaru.errors import KitaruFeatureNotAvailableError, KitaruStateError
from kitaru.wait import _resolve_zenml_wait
from tests.compliance_review_fakes import (
    clear_compliance_review_modules,
    configure_fake_claude_home,
    fake_claude_response,
    fake_claude_transcript_path,
    install_fake_claude_agent_sdk,
)

_WAIT_DISCOVERY_TIMEOUT_SECONDS = 900.0


@pytest.fixture
def stage4_module(monkeypatch, tmp_path):
    """Import Stage 4 with a fake Claude SDK module."""
    configure_fake_claude_home(monkeypatch, tmp_path)
    install_fake_claude_agent_sdk(monkeypatch)
    clear_compliance_review_modules(
        "examples.end_to_end.compliance_review.stage_4_conversational",
    )
    return importlib.import_module(
        "examples.end_to_end.compliance_review.stage_4_conversational"
    )


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
            ) from state["error"]  # ty: ignore[invalid-raise]
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


def test_stage4_run_workflow_can_opt_into_runtime_secret_environment(
    monkeypatch,
    stage4_module,
) -> None:
    """run_workflow() should forward stack and secret-env image overrides."""
    expected = stage4_module.ClaudeAgentResult(
        session_id="stage-4-test-session",
        cwd=str(stage4_module.EXAMPLE_DIR),
        transcript_path="/tmp/stage-4-test-session.jsonl",
        result="Stubbed flow result",
        num_turns=1,
    )
    fake_handle = Mock()
    fake_handle.wait = Mock(return_value=expected)
    fake_flow = Mock()
    fake_flow.run = Mock(return_value=fake_handle)
    monkeypatch.setattr(stage4_module, "conversational_compliance_review", fake_flow)

    result = stage4_module.run_workflow(
        initial_prompt="Start the audit.",
        conversation_label="my-conversation",
        max_turns=2,
        stack="prod-k8s",
        use_secret_environment=True,
    )

    assert result == expected
    fake_flow.run.assert_called_once_with(
        initial_prompt="Start the audit.",
        conversation_label="my-conversation",
        max_turns=2,
        stack="prod-k8s",
        cache=False,
        image={
            "requirements": [
                stage4_module.CLAUDE_AGENT_SDK_REQUIREMENT,
                stage4_module.KITARU_REQUIREMENT,
            ],
            "secret_environment_from": [stage4_module.ANTHROPIC_SECRET_NAME],
        },
    )
    fake_handle.wait.assert_called_once_with()


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


@pytest.mark.parametrize(
    "repository_marker",
    ["without-kitaru", "too-deep-kitaru"],
)
def test_stage4_flow_loads_claude_result_from_checkout_with_unstable_repository_root(
    request,
    monkeypatch,
    tmp_path,
    primed_zenml,
    repository_marker: str,
) -> None:
    """Stage 4 should load Claude results when source root discovery is unstable."""
    del primed_zenml
    copied_checkout = tmp_path / "copied_checkout"
    copied_examples_dir = copied_checkout / "examples"
    copied_category_dir = copied_examples_dir / "end_to_end"
    copied_example_dir = copied_category_dir / "compliance_review"
    source_examples_dir = Path(__file__).resolve().parent.parent / "examples"
    source_category_dir = source_examples_dir / "end_to_end"
    source_example_dir = source_category_dir / "compliance_review"
    copied_category_dir.mkdir(parents=True)
    shutil.copy2(
        source_examples_dir / "__init__.py",
        copied_examples_dir / "__init__.py",
    )
    shutil.copy2(
        source_category_dir / "__init__.py",
        copied_category_dir / "__init__.py",
    )
    shutil.copytree(
        source_example_dir,
        copied_example_dir,
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    if repository_marker == "too-deep-kitaru":
        too_deep_kitaru_dir = copied_example_dir / ".kitaru"
        too_deep_kitaru_dir.mkdir()
        (too_deep_kitaru_dir / "config.yaml").write_text("{}\n")
    assert not (copied_checkout / ".kitaru").exists()

    configure_fake_claude_home(monkeypatch, tmp_path)
    install_fake_claude_agent_sdk(monkeypatch)
    clear_compliance_review_modules()
    sys.modules.pop("examples", None)
    monkeypatch.syspath_prepend(str(copied_checkout))

    # Unbind the tmp-path-imported package on teardown so subsequent tests
    # re-import compliance_review from the real repo path.
    def _restore_compliance_modules() -> None:
        clear_compliance_review_modules()
        sys.modules.pop("examples", None)

    request.addfinalizer(_restore_compliance_modules)

    stage4_module = importlib.import_module(
        "examples.end_to_end.compliance_review.stage_4_conversational"
    )
    assert stage4_module.__file__ is not None
    stage4_path = Path(stage4_module.__file__).resolve()
    assert copied_checkout.resolve() in stage4_path.parents
    assert get_source_root() == str(copied_checkout.resolve())

    calls: list[dict[str, Any]] = []
    session_id = f"{repository_marker}-checkout-session"
    result_text = f"Loaded from copied checkout with {repository_marker}."

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
            session_id=session_id,
            result=result_text,
        )

    monkeypatch.setattr(stage4_module, "run_agent_turn", fake_run_agent_turn)

    result = stage4_module.conversational_compliance_review.run(
        f"Start {repository_marker} checkout review.",
        f"{repository_marker}-checkout-review",
        1,
        cache=False,
    ).wait()

    assert isinstance(result, stage4_module.ClaudeAgentResult)
    assert result.session_id == session_id
    assert result.result == result_text
    assert calls == [
        {
            "prompt": f"Start {repository_marker} checkout review.",
            "allowed_tools": stage4_module.DEFAULT_ALLOWED_TOOLS,
            "resume": None,
            "cwd": stage4_module.EXAMPLE_DIR,
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
    restored_transcript_checks: list[bool] = []

    async def fake_run_agent_turn(
        prompt: str,
        *,
        allowed_tools: list[str],
        resume: str | None,
        cwd: Path,
    ) -> dict[str, Any]:
        turn_number = len(calls) + 1
        if resume is not None:
            transcript_path = Path(fake_claude_transcript_path(resume, cwd=cwd))
            restored_transcript_checks.append(transcript_path.exists())
            assert transcript_path.exists()
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
        first_turn_transcript = Path(
            fake_claude_transcript_path(session_id, cwd=stage4_module.EXAMPLE_DIR)
        )
        assert first_turn_transcript.exists()
        first_turn_transcript.unlink()
        assert not first_turn_transcript.exists()

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
    assert restored_transcript_checks == [True]
