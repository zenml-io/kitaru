from pathlib import Path
from zipfile import ZipFile

import pytest
from scripts.smoke_plugin_artifacts import SmokeFailure, _validate_wheel_metadata

VALID_METADATA = """Metadata-Version: 2.4
Name: kitaru-example
Version: 1.0.0
Summary: Example Kitaru package.
Author-email: ZenML GmbH <info@zenml.io>
License-Expression: Apache-2.0
Keywords: ai-agents,kitaru
Classifier: Development Status :: 3 - Alpha
Description-Content-Type: text/markdown
Project-URL: Homepage, https://kitaru.ai
Project-URL: Documentation, https://docs.zenml.io/kitaru
Project-URL: Repository, https://github.com/zenml-io/kitaru
Project-URL: Issues, https://github.com/zenml-io/kitaru/issues
Project-URL: Changelog, https://github.com/zenml-io/kitaru/blob/develop/CHANGELOG.md

# Kitaru example

Package description.
"""


def _write_wheel(tmp_path: Path, metadata: str) -> Path:
    wheel = tmp_path / "kitaru_example-1.0.0-py3-none-any.whl"
    with ZipFile(wheel, "w") as archive:
        archive.writestr("kitaru_example-1.0.0.dist-info/METADATA", metadata)
    return wheel


def test_wheel_metadata_accepts_complete_pypi_metadata(tmp_path: Path) -> None:
    wheel = _write_wheel(tmp_path, VALID_METADATA)

    _validate_wheel_metadata(wheel, "kitaru-example", "1.0.0")


def test_wheel_metadata_rejects_an_empty_description(tmp_path: Path) -> None:
    headers = VALID_METADATA.split("\n\n", maxsplit=1)[0]
    wheel = _write_wheel(tmp_path, f"{headers}\n\n")

    with pytest.raises(SmokeFailure, match="wheel description is empty"):
        _validate_wheel_metadata(wheel, "kitaru-example", "1.0.0")


def test_wheel_metadata_rejects_a_missing_project_url(tmp_path: Path) -> None:
    metadata = VALID_METADATA.replace(
        "Project-URL: Documentation, https://docs.zenml.io/kitaru\n", ""
    )
    wheel = _write_wheel(tmp_path, metadata)

    with pytest.raises(SmokeFailure, match="missing Project-URL Documentation"):
        _validate_wheel_metadata(wheel, "kitaru-example", "1.0.0")


def test_wheel_metadata_rejects_an_invalid_project_url(tmp_path: Path) -> None:
    metadata = VALID_METADATA.replace(
        "Project-URL: Homepage, https://kitaru.ai",
        "Project-URL: Homepage, http://kitaru.ai",
    )
    wheel = _write_wheel(tmp_path, metadata)

    with pytest.raises(SmokeFailure, match="invalid Project-URL"):
        _validate_wheel_metadata(wheel, "kitaru-example", "1.0.0")
