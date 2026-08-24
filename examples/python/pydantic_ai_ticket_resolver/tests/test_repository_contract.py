"""Repository-level contracts for Kitaru's canonical example."""

import tomllib
from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_canonical_example_contract_is_directory_local() -> None:
    """Keep the example runtime rooted in its own directory."""
    assert (ROOT / "returns_agent").is_dir()
    assert (ROOT / "traces" / "langfuse-traces.jsonl").is_file()
    assert (ROOT / "scripts" / "run_ci_e2e.py").is_file()
    assert not (ROOT / "tests" / "canonical_returns_agent.py").exists()
    assert ROOT.name == "pydantic_ai_ticket_resolver"


def test_readme_owns_setup_without_copying_the_tutorial() -> None:
    """Keep setup executable here and the teaching walkthrough in Kitaru."""
    readme = (ROOT / "README.md").read_text()
    assert "git clone https://github.com/zenml-io/kitaru.git" in readme
    assert "cd kitaru/examples/python/pydantic_ai_ticket_resolver" in readme
    assert "traces/langfuse-traces.jsonl" in readme
    assert "kitaru-guided-tour" in readme
    assert "kitaru-investigation" in readme
    assert "getting-started/quickstart" in readme
    assert "Show me the full run plan" in readme
    assert "If the investigation points to agent behavior" in readme
    assert "inspect and resume that state before it creates anything" in readme
    assert "docs/book/tutorials/returns-agent" in readme
    assert "kitaru-template" not in readme
    assert "If the selected server is healthy, keep using it" in readme
    assert "If no usable server is selected" in readme
    assert readme.index("kitaru status") < readme.index("kitaru login --local")
    assert "paid model calls" not in readme
    worker_command = (
        "uv run kitaru worker start --name canonical-example-worker --concurrency 10"
    )
    assert worker_command in readme
    assert readme.index(worker_command) < readme.index("kitaru session import")
    assert readme.index("kitaru-guided-tour") < readme.index("kitaru-investigation")
    assert len(readme.splitlines()) < 180


def test_development_checks_are_locked_with_the_template() -> None:
    """Keep standalone validation available from the frozen environment."""
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    development = project["dependency-groups"]["dev"]
    assert any(item.startswith("pytest") for item in development)
    assert any(item.startswith("ruff") for item in development)


def test_local_kitaru_state_is_ignored() -> None:
    """Keep generated investigation state out of the public template."""
    ignores = (ROOT.parents[2] / ".gitignore").read_text().splitlines()
    assert ".kitaru/" in ignores
    assert ".zen/" in ignores
