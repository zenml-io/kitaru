from __future__ import annotations

import re
import tomllib
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _read_dockerfile() -> str:
    return (_repo_root() / "docker" / "Dockerfile").read_text()


def _read_pyproject() -> str:
    return (_repo_root() / "pyproject.toml").read_text()


def _dockerfile_zenml_git_ref() -> str:
    dockerfile = _read_dockerfile()
    match = re.search(r"^ARG ZENML_GIT_REF=([0-9a-f]{40})$", dockerfile, re.MULTILINE)
    assert match, "Dockerfile should declare an explicit ZENML_GIT_REF build arg."
    return match.group(1)


def _pyproject_zenml_git_refs() -> set[str]:
    pyproject = tomllib.loads(_read_pyproject())
    pattern = re.compile(r"git\+https://github\.com/zenml-io/zenml\.git@([0-9a-f]{40})")
    refs: set[str] = set()

    def _collect_refs(values: object) -> None:
        if isinstance(values, list):
            for value in values:
                _collect_refs(value)
        elif isinstance(values, dict):
            for value in values.values():
                _collect_refs(value)
        elif isinstance(values, str):
            refs.update(pattern.findall(values))

    _collect_refs(pyproject)
    return refs


def test_dockerfile_zenml_git_ref_matches_pyproject_dependency_pin() -> None:
    """The server image should use the same temporary ZenML pin as the SDK."""
    refs = _pyproject_zenml_git_refs()
    assert refs == {_dockerfile_zenml_git_ref()}, (
        "Dockerfile and pyproject ZenML git refs should stay aligned."
    )


def test_dockerfile_bundles_dashboard_before_final_local_zenml_install() -> None:
    """The Dockerfile should reinstall ZenML only after bundling dashboard assets."""
    dockerfile = _read_dockerfile()
    ordered_steps = [
        "COPY . /tmp/kitaru",
        "RUN cd /tmp/kitaru && uv pip install . && rm -rf /tmp/kitaru",
        'RUN git clone "$ZENML_REPO_URL" /build/zenml',
        'TAG="$ZENML_DASHBOARD_TAG" bash scripts/install-dashboard.sh',
        'uv pip install --reinstall ".[${ZENML_SERVER_EXTRAS}]" "alembic==1.15.2"',
    ]

    positions = [dockerfile.find(step) for step in ordered_steps]
    assert all(position >= 0 for position in positions), (
        "Dockerfile is missing one of the expected ZenML dashboard bundling "
        f"steps: {positions}"
    )
    assert positions == sorted(positions), (
        "ZenML checkout, dashboard bundling, and final local install must stay "
        "in that order."
    )


def test_dockerfile_fails_fast_if_dashboard_assets_are_missing() -> None:
    """The image build should include an explicit dashboard asset sentinel."""
    dockerfile = _read_dockerfile()
    assert "ARG ZENML_DASHBOARD_TAG=" in dockerfile
    assert "import inspect" in dockerfile
    assert "import zenml" in dockerfile
    assert 'dashboard" / "index.html"' in dockerfile
    assert "ZenML dashboard assets missing from installed package" in dockerfile
