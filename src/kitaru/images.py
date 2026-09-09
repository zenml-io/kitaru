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
"""Published container images."""

import re
from importlib.metadata import version

from packaging.version import Version

SERVER_IMAGE_REPOSITORY = "zenmldocker/kitaru-server"
WORKER_IMAGE_REPOSITORY = "zenmldocker/kitaru-worker"


def get_image_tag(package_version: str | None = None) -> str:
    """Get the published image tag for a package version.

    Args:
        package_version: PEP 440 version, the installed Kitaru version when
            omitted.

    Raises:
        ValueError: The version is invalid, or a development or local build
            with no published image.

    Returns:
        Image tag.
    """
    if package_version is None:
        package_version = version("kitaru")
    parsed_version = Version(package_version)
    if parsed_version.is_devrelease or parsed_version.local is not None:
        raise ValueError(
            f"No published image exists for development build {package_version}"
        )
    base_version = parsed_version.base_version
    tag = base_version.replace("!", ".epoch.")
    suffix = str(parsed_version).removeprefix(base_version)
    if not suffix:
        return tag
    suffix = re.sub(r"([A-Za-z]+)([0-9]+)", r"\1.\2", suffix).strip(".")
    return f"{tag}-{suffix}"


def get_image(repository: str, package_version: str | None = None) -> str:
    """Get the published image reference for a package version.

    Args:
        repository: Image repository.
        package_version: PEP 440 version, the installed Kitaru version when
            omitted.

    Raises:
        ValueError: The version is invalid, or a development or local build
            with no published image.

    Returns:
        Image reference.
    """
    return f"{repository}:{get_image_tag(package_version)}"
