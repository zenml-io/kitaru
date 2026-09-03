#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for published container image references."""

import pytest

from kitaru import images
from kitaru.images import WORKER_IMAGE_REPOSITORY, get_image, get_image_tag


@pytest.mark.parametrize(
    ("package_version", "tag"),
    [
        ("0.22.0", "0.22.0"),
        ("0.22.0rc5", "0.22.0-rc.5"),
        ("0.22.0rc5.post2", "0.22.0-rc.5.post.2"),
        ("1!0.22.0rc5", "1.epoch.0.22.0-rc.5"),
    ],
)
def test_image_tag_formats_released_versions(package_version: str, tag: str) -> None:
    """Released PEP 440 versions map to Docker-compatible tags."""
    assert get_image_tag(package_version) == tag


@pytest.mark.parametrize("package_version", ["0.22.0.dev3", "0.22.0+macos.arm64"])
def test_image_tag_rejects_unpublished_builds(package_version: str) -> None:
    """Development and local builds have no published image."""
    with pytest.raises(ValueError, match="development build"):
        get_image_tag(package_version)


def test_image_tag_rejects_an_invalid_version() -> None:
    """A version that is not PEP 440 raises."""
    with pytest.raises(ValueError):
        get_image_tag("not-a-version")


def test_image_tag_defaults_to_the_installed_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Omitting the version reads the installed Kitaru version."""
    monkeypatch.setattr(images, "version", lambda name: "0.25.0rc1")
    assert get_image_tag() == "0.25.0-rc.1"


def test_image_joins_the_repository_and_tag(monkeypatch: pytest.MonkeyPatch) -> None:
    """An image reference is the repository at the version's tag."""
    monkeypatch.setattr(images, "version", lambda name: "0.25.0")
    assert get_image(WORKER_IMAGE_REPOSITORY) == "zenmldocker/kitaru-worker:0.25.0"
    assert get_image(WORKER_IMAGE_REPOSITORY, "0.24.0") == (
        "zenmldocker/kitaru-worker:0.24.0"
    )
