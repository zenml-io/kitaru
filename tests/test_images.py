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
from packaging.version import Version

from kitaru.images import format_image_version, get_worker_image


@pytest.mark.parametrize(
    ("package_version", "image_version"),
    [
        ("0.22.0", "0.22.0"),
        ("0.22.0rc5.post2", "0.22.0-rc.5.post.2"),
        ("0.22.0.dev3", "0.22.0-dev.3"),
        ("0.22.0+macos.arm64", "0.22.0-local.macos.arm64"),
        ("1!0.22.0rc5", "1.epoch.0.22.0-rc.5"),
        (
            "0.22.0rc5.post2.dev3+macos.arm64",
            "0.22.0-rc.5.post.2.dev.3.local.macos.arm64",
        ),
    ],
)
def test_image_version_formatter_supports_pep440_suffixes(
    package_version: str, image_version: str
) -> None:
    """All canonical PEP 440 suffixes produce Docker-compatible tags."""
    assert format_image_version(Version(package_version)) == image_version


@pytest.mark.parametrize(
    ("package_version", "image"),
    [
        ("0.25.0", "zenmldocker/kitaru-worker:0.25.0"),
        ("0.25.0rc1", "zenmldocker/kitaru-worker:0.25.0-rc.1"),
    ],
)
def test_worker_image_matches_the_published_tag(
    package_version: str, image: str
) -> None:
    """Released versions map to the published worker image tag."""
    assert get_worker_image(package_version) == image


@pytest.mark.parametrize("package_version", ["0.25.0.dev3", "0.25.0+macos.arm64"])
def test_worker_image_rejects_unpublished_builds(package_version: str) -> None:
    """Development and local builds have no published worker image."""
    with pytest.raises(ValueError, match="development build"):
        get_worker_image(package_version)
