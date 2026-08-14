import json
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "typescript-packages.mjs"
CI_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "ci.yml"
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "release-typescript.yml"

PACKAGE_PATHS = (
    "packages/core",
    "packages/mastra",
    "packages/vercel-ai",
)


@pytest.fixture
def typescript_repo(tmp_path: Path) -> Path:
    for relative_path in (
        *(f"{package_path}/package.json" for package_path in PACKAGE_PATHS),
    ):
        source = REPO_ROOT / relative_path
        destination = tmp_path / relative_path
        destination.parent.mkdir(parents=True)
        shutil.copyfile(source, destination)
    return tmp_path


def run_metadata(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["node", str(SCRIPT_PATH), *args],
        check=False,
        capture_output=True,
        text=True,
    )


def get_package_version(repository_root: Path = REPO_ROOT) -> str:
    manifest = json.loads((repository_root / "packages/core/package.json").read_text())
    return manifest["version"]


def test_typescript_release_metadata_describes_the_lockstep_package_set() -> None:
    version = get_package_version()
    tag = f"typescript/kitaru/v{version}"
    result = run_metadata("--tag", tag)

    assert result.returncode == 0, result.stderr
    metadata = json.loads(result.stdout)
    assert metadata == {
        "version": version,
        "tag": tag,
        "npm_tag": "rc" if "-rc." in version else "latest",
        "prerelease": "-rc." in version,
        "packages": [
            {
                "name": "@zenml-io/kitaru",
                "path": "packages/core",
                "tarball": f"zenml-io-kitaru-{version}.tgz",
            },
            {
                "name": "@zenml-io/kitaru-mastra",
                "path": "packages/mastra",
                "tarball": f"zenml-io-kitaru-mastra-{version}.tgz",
            },
            {
                "name": "@zenml-io/kitaru-vercel-ai",
                "path": "packages/vercel-ai",
                "tarball": f"zenml-io-kitaru-vercel-ai-{version}.tgz",
            },
        ],
    }


def test_typescript_release_metadata_rejects_a_mismatched_tag() -> None:
    version = get_package_version()
    result = run_metadata("--tag", "typescript/kitaru/v999.0.0")

    assert result.returncode == 1
    assert f"does not match package version {version}" in result.stderr


def test_typescript_release_metadata_rejects_version_drift(
    typescript_repo: Path,
) -> None:
    version = get_package_version(typescript_repo)
    manifest_path = typescript_repo / "packages/mastra/package.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["version"] = "999.0.0"
    manifest_path.write_text(json.dumps(manifest))

    result = run_metadata("--repo-root", str(typescript_repo))

    assert result.returncode == 1
    assert f"must share version {version}" in result.stderr


def test_typescript_release_metadata_rejects_adapter_dependency_drift(
    typescript_repo: Path,
) -> None:
    version = get_package_version(typescript_repo)
    manifest_path = typescript_repo / "packages/vercel-ai/package.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["dependencies"]["@zenml-io/kitaru"] = "workspace:^"
    manifest_path.write_text(json.dumps(manifest))

    result = run_metadata("--repo-root", str(typescript_repo))

    assert result.returncode == 1
    assert f"must depend on @zenml-io/kitaru as workspace:{version}" in result.stderr


def test_typescript_release_metadata_classifies_stable_versions(
    typescript_repo: Path,
) -> None:
    stable_version = "1.2.3"
    for package_path in PACKAGE_PATHS:
        manifest_path = typescript_repo / package_path / "package.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["version"] = stable_version
        dependency = manifest.get("dependencies", {}).get("@zenml-io/kitaru")
        if dependency is not None:
            manifest["dependencies"]["@zenml-io/kitaru"] = f"workspace:{stable_version}"
        manifest_path.write_text(json.dumps(manifest))
    result = run_metadata("--repo-root", str(typescript_repo))

    assert result.returncode == 0, result.stderr
    metadata = json.loads(result.stdout)
    assert metadata["version"] == stable_version
    assert metadata["npm_tag"] == "latest"
    assert metadata["prerelease"] is False


def test_typescript_release_workflow_contract() -> None:
    workflow_source = WORKFLOW_PATH.read_text()
    build_source, remaining = workflow_source.split("\n  publish:\n", maxsplit=1)
    publish_source, verify_source = remaining.split(
        "\n  verify-and-release:\n", maxsplit=1
    )

    assert "types: [release-kitaru-typescript]" in workflow_source
    assert "if: github.event_name == 'repository_dispatch'" in publish_source
    assert "needs: build" in publish_source
    assert "needs: [build, publish]" in verify_source
    assert "node scripts/typescript-packages.mjs --tag" in workflow_source
    assert "run: pnpm run pack:release:built" in build_source
    assert "environment: npm-publish" in publish_source
    assert "contents: read\n      id-token: write" in publish_source
    assert "permissions:\n      contents: write" in verify_source
    assert "--provenance" in publish_source
    assert "preflight_package" in publish_source
    assert publish_source.count('"./typescript-dist/zenml-io-kitaru') == 3
    assert "NPM_CONFIG_USERCONFIG" in publish_source
    assert "else\n                view_status=$?" in publish_source
    assert "fi\n              view_status=$?" not in publish_source
    assert "Verify registry installation" in verify_source
    assert "for attempt in $(seq 1 60)" in verify_source
    assert 'npm view "@zenml-io/kitaru@$VERSION" version' in verify_source
    assert 'npm view "@zenml-io/kitaru-mastra@$VERSION" version' in verify_source
    assert 'npm view "@zenml-io/kitaru-vercel-ai@$VERSION" version' in verify_source
    assert 'echo "Waiting for npm publish-time scanning' in verify_source
    assert "sleep 20" in verify_source
    assert verify_source.count("npm install --ignore-scripts") == 1
    assert 'gh release view "$PACKAGE_TAG"' in verify_source
    assert "gh release upload" in verify_source
    assert "--clobber" in verify_source
    assert publish_source.index(
        "zenml-io-kitaru-${VERSION}.tgz"
    ) < publish_source.index("zenml-io-kitaru-mastra-${VERSION}.tgz")
    assert publish_source.index(
        "zenml-io-kitaru-mastra-${VERSION}.tgz"
    ) < publish_source.index("zenml-io-kitaru-vercel-ai-${VERSION}.tgz")
    assert "name: typescript-distributions" in publish_source
    assert "name: typescript-distributions" in verify_source


def test_typescript_ci_owns_cross_language_tests() -> None:
    workflow_source = CI_WORKFLOW_PATH.read_text()
    typescript_job = workflow_source.split("\n  typescript:\n", maxsplit=1)[1].split(
        "\n  client-import:\n", maxsplit=1
    )[0]
    base_matrix = workflow_source.split("\n          - name: py311-base\n", maxsplit=1)[
        1
    ].split("\n          - name: py311-cli\n", maxsplit=1)[0]

    assert base_matrix.count("--ignore=tests/typescript") == 4
    assert "run: uv run pytest -q tests/typescript" in typescript_job


def test_typescript_ci_validates_the_standalone_ticket_resolver() -> None:
    workflow_source = CI_WORKFLOW_PATH.read_text()
    job = workflow_source.split("\n  typescript-ticket-resolver:\n", maxsplit=1)[
        1
    ].split("\n  client-import:\n", maxsplit=1)[0]

    assert "image: postgres:16-alpine" in job
    assert "run: pnpm install --frozen-lockfile" in job
    assert "pnpm --filter @zenml-io/kitaru build" in job
    assert "pnpm --filter @zenml-io/kitaru-vercel-ai build" in job
    assert "CI=true pnpm --ignore-workspace install --frozen-lockfile" in job
    for command in (
        "pnpm build",
        "pnpm test",
        "pnpm test:evaluator",
        "pnpm typecheck",
        "pnpm lint",
        "pnpm test:e2e",
    ):
        assert command in job
