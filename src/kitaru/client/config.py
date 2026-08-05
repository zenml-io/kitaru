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
"""On-disk client configuration."""

import logging

from pydantic import BaseModel, ValidationError

from kitaru.client.credential_store import (
    get_config_file_path,
    normalize_server_url,
    write_json_file,
)

logger = logging.getLogger(__name__)

ENV_CONFIG_PATH = "KITARU_CONFIG_PATH"

CONFIG_FILE_NAME = "config.json"


class ClientConfig(BaseModel):
    """Client configuration."""

    active_server_url: str | None = None


def load_config() -> ClientConfig:
    """Read the configuration file, ignoring one that cannot be parsed.

    Returns:
        Stored configuration, or an empty one when the file is missing or
        malformed.
    """
    path = get_config_file_path(ENV_CONFIG_PATH, CONFIG_FILE_NAME)
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return ClientConfig()
    try:
        return ClientConfig.model_validate_json(raw)
    except ValidationError:
        logger.warning("Ignoring malformed configuration file %s.", path)
        return ClientConfig()


def save_config(config: ClientConfig) -> None:
    """Write the configuration file, replacing it in one step.

    Args:
        config: Configuration to write.
    """
    write_json_file(
        get_config_file_path(ENV_CONFIG_PATH, CONFIG_FILE_NAME),
        config.model_dump(mode="json", exclude_none=True),
    )


def get_active_server_url() -> str | None:
    """Return the stored active server URL.

    Returns:
        Active server URL, or None when none is stored.
    """
    return load_config().active_server_url


def set_active_server_url(url: str | None) -> None:
    """Store the active server URL.

    Args:
        url: Server base URL, None clears the active server.
    """
    config = load_config()
    config.active_server_url = normalize_server_url(url) if url else None
    save_config(config)
