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

import json
import logging
import os
import tempfile
from pathlib import Path

from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

ENV_CONFIG_PATH = "KITARU_CONFIG_PATH"

CONFIG_FILE_NAME = "config.json"
# Only the owner may read the files or list the directory holding them.
FILE_MODE = 0o600
DIRECTORY_MODE = 0o700


def get_config_directory() -> Path:
    """Return the directory holding Kitaru client configuration.

    Returns:
        ``$XDG_CONFIG_HOME/kitaru``, falling back to ``~/.config/kitaru``.
    """
    base = os.environ.get("XDG_CONFIG_HOME")
    root = Path(base) if base else Path.home() / ".config"
    return root / "kitaru"


def get_config_file_path(env_name: str, file_name: str) -> Path:
    """Return the location of a client configuration file.

    Args:
        env_name: Environment variable naming an override location.
        file_name: File name inside the Kitaru config directory.

    Returns:
        Path read from the environment variable, otherwise the file in the
        Kitaru config directory.
    """
    override = os.environ.get(env_name)
    if override:
        return Path(override)
    return get_config_directory() / file_name


def write_json_file(path: Path, payload: object) -> None:
    """Write a JSON file, replacing it in one step.

    Args:
        path: File location.
        payload: JSON-serializable content.
    """
    path.parent.mkdir(parents=True, exist_ok=True, mode=DIRECTORY_MODE)
    # A partial write must never replace a good file, and the content must
    # never be readable by others between creating the file and setting its
    # mode.
    handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2, sort_keys=True)
        os.chmod(temporary, FILE_MODE)
        os.replace(temporary, path)
    except OSError:
        Path(temporary).unlink(missing_ok=True)
        raise


def normalize_server_url(url: str) -> str:
    """Normalize a server URL into the key credentials are stored under.

    Args:
        url: Server base URL.

    Returns:
        URL without a trailing slash.
    """
    return url.rstrip("/")


class ClientConfig(BaseModel):
    """Client configuration."""

    server_url: str | None = None


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


def get_server_url() -> str | None:
    """Return the stored server URL.

    Returns:
        Server URL, or None when none is stored.
    """
    return load_config().server_url


def set_server_url(url: str | None) -> None:
    """Store the server URL.

    Args:
        url: Server base URL, None clears the stored URL.
    """
    config = load_config()
    config.server_url = normalize_server_url(url) if url else None
    save_config(config)
