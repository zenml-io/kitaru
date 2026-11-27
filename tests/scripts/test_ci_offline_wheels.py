"""Regression checks for offline uv wheel configuration in CI and scripts."""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_workflows_and_scripts_do_not_set_nonexistent_uv_no_index() -> None:
    """Reject the nonexistent UV_NO_INDEX variable in workflows and shell scripts."""
    targets = [
        *sorted((REPO_ROOT / ".github" / "workflows").glob("*.yml")),
        *sorted((REPO_ROOT / ".github" / "workflows").glob("*.yaml")),
        *sorted((REPO_ROOT / "scripts").glob("*.sh")),
    ]
    for path in targets:
        assert "UV_NO_INDEX" not in path.read_text(encoding="utf-8"), (
            f"{path.relative_to(REPO_ROOT)} sets UV_NO_INDEX, which uv ignores"
        )


def test_quickstart_ci_job_enforces_offline_no_index_wheel_resolution() -> None:
    """Require the quickstart E2E job to resolve candidate wheels with no-index."""
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    )
    steps = workflow["jobs"]["quickstart-example"]["steps"]

    prepare_step = next(
        step
        for step in steps
        if step.get("name") == "Prepare offline wheelhouse for quickstart plugins"
    )
    prepare_run = prepare_step["run"]
    assert "no-index = true" in prepare_run
    assert "find-links = [" in prepare_run
    assert "kitaru-*.whl" in prepare_run
    assert "rm -f" in prepare_run

    e2e_step = next(
        step
        for step in steps
        if step.get("name") == "Run quickstart example end to end"
    )
    env = e2e_step["env"]
    assert env["UV_CONFIG_FILE"] == "${{ runner.temp }}/uv-offline.toml"
    assert env["UV_OFFLINE"] == "1"
    assert env["UV_NO_SYNC"] == "1"
    assert "UV_NO_INDEX" not in env
