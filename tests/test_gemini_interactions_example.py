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
