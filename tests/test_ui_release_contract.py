from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (_REPO_ROOT / relative_path).read_text()


def _workflow_step_block(workflow: str, step_name: str) -> str:
    marker = f"      - name: {step_name}\n"
    start = workflow.index(marker)
    remainder = workflow[start + len(marker) :]
    next_step = remainder.find("\n      - name: ")
    if next_step == -1:
        return workflow[start:]
    return workflow[start : start + len(marker) + next_step]


def test_release_workflow_uses_stable_monorepo_ui_bundling() -> None:
    """Official releases bundle stable monorepo UI before package/Docker publish."""
    workflow = _read(".github/workflows/release.yml")

    assert "zenml-io/kitaru-ui" not in workflow
    assert "kitaru-ui-v0.1.0" in workflow
    assert "KITARU_UI_RELEASE_TOKEN" in workflow
    assert "KITARU_UI_ALLOW_PRERELEASE" not in workflow
    assert "bash scripts/download-ui.sh" in workflow
    assert "src/kitaru/_ui/bundle_manifest.json" in workflow
    assert "uv run --no-project scripts/verify-ui-wheel.py" in workflow
    assert "Verify PyPI artifacts match local build" in workflow
    assert "Local dist artifacts differ from PyPI" in workflow

    docker_step = _workflow_step_block(workflow, "Build and push Docker image")
    assert "KITARU_UI_TAG=" not in docker_step
    assert "KITARU_VERSION=" in docker_step
    assert "ZENML_SERVER_TAG=0.94.4" in docker_step


def test_ci_bundles_ui_before_source_docker_smoke() -> None:
    """CI Docker smoke should exercise the bundled-then-build path."""
    workflow = _read(".github/workflows/ci.yml")

    assert "KITARU_UI_RELEASE_TOKEN" in workflow
    assert "--build-arg KITARU_UI_TAG" not in workflow
    assert "KITARU_UI_TAG: latest" not in workflow
    assert "github.event_name == 'push'" in workflow
    assert "github.event.pull_request.head.repo.full_name" not in workflow
    assert "== github.repository" not in workflow
    assert "never expose it to PR code" in workflow
    assert "bash scripts/verify-server-ui.sh kitaru-ci-server" in workflow
    assert "uv run --no-project scripts/verify-ui-wheel.py" in workflow

    download_index = workflow.index("Download stable Kitaru UI")
    docker_build_index = workflow.index(
        "docker build -f docker/Dockerfile --target server"
    )
    assert download_index < docker_build_index

    server_ui_script = _read("scripts/verify-server-ui.sh")
    assert "Path(kitaru.__file__).parent" in server_ui_script
    assert "zen_server" in server_ui_script
    assert "dashboard" in server_ui_script


def test_prerelease_smoke_workflow_is_manual_and_non_publishing() -> None:
    """The prerelease lane can opt in to prerelease UI but must not publish."""
    workflow = _read(".github/workflows/ui-prerelease-smoke.yml")

    assert "workflow_dispatch" in workflow
    assert "ui-tag:" in workflow
    assert "kitaru-ref:" in workflow
    assert "docker-smoke:" in workflow
    assert "Checkout trusted workflow ref" in workflow
    assert "path: trusted" in workflow
    assert "path: kitaru" in workflow
    assert "working-directory: kitaru" in workflow
    assert "bash ../trusted/scripts/download-ui.sh" in workflow
    assert "uv run --no-project ../trusted/scripts/verify-ui-wheel.py" in workflow
    assert "bash trusted/scripts/verify-server-ui.sh" in workflow
    assert "KITARU_UI_ALLOW_PRERELEASE: 'true'" in workflow
    assert "KITARU_UI_RELEASE_TOKEN" in workflow
    assert "uv build --wheel" in workflow
    assert "docker build -f docker/Dockerfile --target server" in workflow
    assert "--build-arg KITARU_UI_TAG" not in workflow

    forbidden_publish_markers = [
        "pypa/gh-action-pypi-publish",
        "docker/login-action",
        "docker push",
        "push: true",
        "gh release create",
        "gh release upload",
        "helm push",
        "git push",
    ]
    for marker in forbidden_publish_markers:
        assert marker not in workflow
