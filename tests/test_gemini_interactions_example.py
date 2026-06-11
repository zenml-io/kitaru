"""No-network checks for the Gemini Interactions example."""

import argparse

import pytest
from examples.integrations.gemini_interactions_agent import gemini_interactions_adapter
from pydantic import BaseModel


class ForeignGeminiResult(BaseModel):
    status: str
    output_text: str


def test_gemini_interactions_example_help(capsys: pytest.CaptureFixture[str]) -> None:
    """The example help path should not require credentials or network."""
    with pytest.raises(SystemExit) as exc_info:
        gemini_interactions_adapter.main(["--help"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "Gemini Interactions API" in output
    assert "--dry-run" in output
    assert "--show-text-deltas" in output
    assert "--hide-text-deltas" in output
    assert "sandbox-function" in output


def test_gemini_interactions_example_dry_run_without_credentials(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The dry-run path should print a realistic summary without API keys."""
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    gemini_interactions_adapter.main(["--dry-run", "--mode", "antigravity"])

    output = capsys.readouterr().out
    assert "Dry run only: no Google request was made" in output
    assert "Status: completed" in output
    assert "Agent: antigravity-preview-05-2026" in output
    assert "Input: (disabled)" in output
    assert "Raw interaction: (disabled)" in output
    assert "Steps: (disabled)" in output


def test_gemini_interactions_example_dry_run_accepts_show_text_deltas(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    gemini_interactions_adapter.main(["--dry-run", "--stream", "--show-text-deltas"])

    output = capsys.readouterr().out
    assert "Dry run only: no Google request was made" in output
    assert "Stream metadata" in output


def test_gemini_interactions_example_sandbox_function_dry_run(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    gemini_interactions_adapter.main(["--dry-run", "--mode", "sandbox-function"])

    output = capsys.readouterr().out
    assert "Sandbox function dry run" in output
    assert "Fake Gemini requires_action result" in output
    assert "Status: requires_action" in output
    assert "sandbox_python_version" in output
    assert "Fake Kitaru sandbox command result" in output
    assert "python --version" in output
    assert "payload_output_max_chars" in output
    assert "stdout_payload_truncated" in output
    assert '"cleanup": {' in output
    assert '"error"' not in output
    assert "run the sandbox command from a @checkpoint" in output
    assert "Fake Gemini function_result request" in output
    assert "dry-run-call-id" in output


def test_gemini_interactions_example_main_shows_text_deltas_by_default(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[dict[str, object]] = []

    class FakeHandle:
        exec_id = "exec-123"

        def wait(self) -> gemini_interactions_adapter.GeminiInteractionResult:
            return gemini_interactions_adapter._fake_result(
                "model",
                "gemini-test",
                stream=True,
            )

    def fake_run(
        request: gemini_interactions_adapter.GeminiInteractionRequest,
        *,
        stream: bool = False,
        show_text_deltas: bool = False,
    ) -> FakeHandle:
        calls.append(
            {
                "model": request.model,
                "stream": stream,
                "show_text_deltas": show_text_deltas,
            }
        )
        return FakeHandle()

    monkeypatch.setattr(
        gemini_interactions_adapter, "_prepare_google_credentials", lambda: None
    )
    monkeypatch.setattr(
        gemini_interactions_adapter, "_guard_vertex_mode", lambda mode: None
    )
    monkeypatch.setattr(
        gemini_interactions_adapter,
        "_watch_gemini_stream",
        lambda exec_id, stop_watching: None,
    )
    monkeypatch.setattr(
        gemini_interactions_adapter.run_gemini_interaction, "run", fake_run
    )

    gemini_interactions_adapter.main(["--mode", "model", "--stream"])

    capsys.readouterr()
    assert calls == [
        {
            "model": "gemini-3.5-flash",
            "stream": True,
            "show_text_deltas": True,
        }
    ]


def test_gemini_interactions_example_hide_text_deltas_overrides_stream_default() -> (
    None
):
    args = gemini_interactions_adapter._parse_args(
        ["--mode", "model", "--stream", "--hide-text-deltas"]
    )

    assert args.stream is True
    assert args.show_text_deltas is False


def test_gemini_interactions_example_google_api_key_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GOOGLE_GENAI_USE_VERTEXAI", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv("GOOGLE_API_KEY", "alias-key")

    gemini_interactions_adapter._prepare_google_credentials()

    assert gemini_interactions_adapter.os.environ["GEMINI_API_KEY"] == "alias-key"


def test_gemini_interactions_example_vertex_adc_needs_no_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Vertex AI mode authenticates via ADC, so the gate must not demand a key."""
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.setenv("GOOGLE_GENAI_USE_VERTEXAI", "true")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "demo-project")
    monkeypatch.setenv("GOOGLE_CLOUD_LOCATION", "europe-north1")

    assert gemini_interactions_adapter._vertex_mode_enabled() is True
    # The gate must accept ADC/Vertex mode without raising for a missing key.
    gemini_interactions_adapter._prepare_google_credentials()


def test_gemini_interactions_example_antigravity_request_uses_background() -> None:
    args = argparse.Namespace(
        mode="antigravity",
        prompt="inspect safely",
        timeout=123.0,
        foreground_antigravity=False,
    )

    request = gemini_interactions_adapter._build_request(args)

    assert request.agent == "antigravity-preview-05-2026"
    assert request.background is True
    assert request.store is True
    assert request.timeout_s == 123.0


def test_gemini_interactions_example_antigravity_foreground_override() -> None:
    args = argparse.Namespace(
        mode="antigravity",
        prompt="inspect safely",
        timeout=123.0,
        foreground_antigravity=True,
    )

    request = gemini_interactions_adapter._build_request(args)

    assert request.agent == "antigravity-preview-05-2026"
    assert request.background is False
    assert request.store is True


def test_gemini_interactions_example_sandbox_function_request_uses_model_tool() -> None:
    args = argparse.Namespace(
        mode="sandbox-function",
        prompt="call the sandbox function",
        model="gemini-test",
        timeout=123.0,
        foreground_antigravity=False,
    )

    request = gemini_interactions_adapter._build_request(args)

    assert request.model == "gemini-test"
    assert request.agent is None
    assert request.tools == [gemini_interactions_adapter.SANDBOX_FUNCTION_TOOL]
    assert request.metadata["mode"] == "sandbox-function"


def test_gemini_interactions_example_sandbox_showcase_requires_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = gemini_interactions_adapter.GeminiInteractionRequest.start(
        "hello",
        model="gemini-test",
    )

    class FakeRunner:
        def __init__(self) -> None:
            self.calls = 0

        def run_sync(
            self,
            request: gemini_interactions_adapter.GeminiInteractionRequest,
        ) -> gemini_interactions_adapter.GeminiInteractionResult:
            self.calls += 1
            return gemini_interactions_adapter.GeminiInteractionResult(
                status="completed",
                interaction_id="interaction-1",
                model="gemini-test",
            )

    fake_runner = FakeRunner()
    monkeypatch.setattr(gemini_interactions_adapter, "RUNNER", fake_runner)

    with pytest.raises(RuntimeError, match="expected Gemini to request"):
        gemini_interactions_adapter.run_gemini_sandbox_function_showcase._func(
            request,
        )

    assert fake_runner.calls == 1


def test_gemini_interactions_example_show_text_deltas_builds_opt_in_runner() -> None:
    default_runner = gemini_interactions_adapter._build_runner()
    opt_in_runner = gemini_interactions_adapter._build_runner(
        include_stream_text_deltas=True
    )

    assert default_runner._capture.include_stream_text_deltas is False
    assert opt_in_runner._capture.include_stream_text_deltas is True


def test_gemini_interactions_example_text_delta_display_is_not_duplicated() -> None:
    lines = gemini_interactions_adapter._stream_event_display_lines(
        "gemini_interactions.stream.event",
        {
            "data": {
                "category": "text_delta",
                "display": "hello streamed chunk",
                "text_delta": "hello streamed chunk",
            }
        },
    )

    assert lines == [
        "- [text_delta] Gemini text delta",
        "  text_delta: hello streamed chunk",
    ]


def test_gemini_interactions_example_non_text_delta_display_is_preserved() -> None:
    lines = gemini_interactions_adapter._stream_event_display_lines(
        "gemini_interactions.stream.event",
        {"data": {"category": "content_start", "display": "Gemini content started"}},
    )

    assert lines == ["- [content_start] Gemini content started"]


def test_gemini_interactions_example_vertex_requires_project_and_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Vertex mode without project/location should fail with a clear message."""
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    monkeypatch.delenv("GOOGLE_CLOUD_LOCATION", raising=False)
    monkeypatch.setenv("GOOGLE_GENAI_USE_VERTEXAI", "true")

    with pytest.raises(SystemExit) as exc_info:
        gemini_interactions_adapter._prepare_google_credentials()

    assert "GOOGLE_CLOUD_PROJECT" in str(exc_info.value)
    assert "GOOGLE_CLOUD_LOCATION" in str(exc_info.value)


def test_gemini_interactions_example_vertex_rejects_sandbox_function_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GOOGLE_GENAI_USE_VERTEXAI", "true")

    with pytest.raises(SystemExit) as exc_info:
        gemini_interactions_adapter._guard_vertex_mode("sandbox-function")

    message = str(exc_info.value)
    assert "sandbox-function mode" in message
    assert "--mode antigravity" in message


def test_gemini_interactions_example_coerces_foreign_model_result() -> None:
    result = gemini_interactions_adapter._coerce_result(
        ForeignGeminiResult(status="completed", output_text="hello")
    )

    assert result.status == "completed"
    assert result.output_text == "hello"


def test_gemini_interactions_example_real_run_requires_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real run should fail early and clearly when no Google key is visible."""
    monkeypatch.delenv("GOOGLE_GENAI_USE_VERTEXAI", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    with pytest.raises(SystemExit) as exc_info:
        gemini_interactions_adapter.main(["--mode", "model"])

    assert "Missing Google/Gemini credentials" in str(exc_info.value)
