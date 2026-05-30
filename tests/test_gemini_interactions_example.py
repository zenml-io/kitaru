"""No-network checks for the Gemini Interactions example."""

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
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    with pytest.raises(SystemExit) as exc_info:
        gemini_interactions_adapter.main(["--mode", "model"])

    assert "Missing Google/Gemini credentials" in str(exc_info.value)
