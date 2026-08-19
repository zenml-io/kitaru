"""Regression tests for the public example coverage audit."""

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture(scope="module")
def audit_module() -> ModuleType:
    """Load the standalone audit script as an importable module."""
    script_path = (
        Path(__file__).resolve().parents[1] / "scripts" / "audit-example-coverage.py"
    )
    spec = importlib.util.spec_from_file_location("audit_example_coverage", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_example_url_extracts_full_nested_path(audit_module: ModuleType) -> None:
    """Capture the complete nested example path from a GitHub URL."""
    text = (
        "https://github.com/zenml-io/kitaru/tree/develop/"
        "examples/typescript/mastra_support_triage"
    )

    assert audit_module.EXAMPLE_PATH_RE.findall(text) == [
        "examples/typescript/mastra_support_triage"
    ]


def test_public_doc_must_link_declared_example(
    audit_module: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject an existing public document with no backlink to its example."""
    docs_path = tmp_path / "adapter.md"
    docs_path.write_text("# Adapter\n\nNo runnable example here.\n", encoding="utf-8")
    monkeypatch.setattr(audit_module, "ROOT", tmp_path)

    errors = audit_module._audit_public_docs(
        {
            "path": "examples/typescript/mastra_support_triage",
            "public_docs": ["adapter.md"],
        },
        "entry 'mastra-support-triage'",
    )

    assert errors == [
        "entry 'mastra-support-triage': public docs path adapter.md does not link "
        "to examples/typescript/mastra_support_triage"
    ]


def test_public_doc_accepts_related_example_url(
    audit_module: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Accept a GitHub link to the declared example directory."""
    docs_path = tmp_path / "adapter.md"
    docs_path.write_text(
        "https://github.com/zenml-io/kitaru/tree/develop/"
        "examples/typescript/mastra_support_triage\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(audit_module, "ROOT", tmp_path)

    assert (
        audit_module._audit_public_docs(
            {
                "path": "examples/typescript/mastra_support_triage",
                "public_docs": ["adapter.md"],
            },
            "entry 'mastra-support-triage'",
        )
        == []
    )


def test_manual_command_rejects_shell_placeholder(audit_module: ModuleType) -> None:
    """Reject placeholder syntax that a shell interprets as redirection."""
    errors = audit_module._audit_command(
        {
            "status": "manual_only",
            "command": "OPENAI_API_KEY=<key> uv run python -m example.demo",
        },
        "entry 'demo'",
        "coverage.live_provider",
    )

    assert any("NAME=<value>" in error for error in errors)
