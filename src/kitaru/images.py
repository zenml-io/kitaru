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

from packaging.version import Version

SERVER_IMAGE_REPOSITORY = "zenmldocker/kitaru-server"
WORKER_IMAGE_REPOSITORY = "zenmldocker/kitaru-worker"


def format_image_version(version: Version) -> str:
    """Format a PEP 440 version as a Docker-compatible image tag.

    Args:
        version: Parsed package version.

    Returns:
        Image tag.
    """
    canonical_version = str(version)
    base_version = version.base_version
    image_base_version = base_version.replace("!", ".epoch.")
    suffix = canonical_version.removeprefix(base_version)
    if not suffix:
        return image_base_version

    public_suffix, local_separator, local_suffix = suffix.partition("+")
    public_suffix = re.sub(r"([A-Za-z]+)([0-9]+)", r"\1.\2", public_suffix).strip(".")
    suffix_parts = [public_suffix] if public_suffix else []
    if local_separator:
        suffix_parts.append(f"local.{local_suffix}")
    return f"{image_base_version}-{'.'.join(suffix_parts)}"


def get_worker_image(package_version: str) -> str:
    """Build the published worker image reference for a package version.

    Args:
        package_version: Installed Kitaru version.

    Raises:
        ValueError: The version is a development or local build, which has no
            published image.

    Returns:
        Worker image reference.
    """
    parsed_version = Version(package_version)
    if parsed_version.is_devrelease or parsed_version.local is not None:
        raise ValueError(
            f"No published worker image exists for development build {package_version}"
        )
    return f"{WORKER_IMAGE_REPOSITORY}:{format_image_version(parsed_version)}"
