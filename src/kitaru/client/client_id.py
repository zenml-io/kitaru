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
"""Stable identifier of this client installation."""

import os
import uuid

from kitaru.client.credential_store import (
    DIRECTORY_MODE,
    FILE_MODE,
    get_config_directory,
)

ENV_CLIENT_ID = "KITARU_CLIENT_ID"

CLIENT_ID_FILE_NAME = "client_id"


def get_client_id() -> uuid.UUID:
    """Return the id the control plane knows this installation by.

    The control plane device authorization grant keys a device on the client
    id, so the same machine has to present the same one across logins.

    Returns:
        Id read from ``KITARU_CLIENT_ID``, from the config directory, or newly
        generated and written there.
    """
    override = os.environ.get(ENV_CLIENT_ID)
    if override:
        try:
            return uuid.UUID(override)
        except ValueError:
            pass
    path = get_config_directory() / CLIENT_ID_FILE_NAME
    try:
        return uuid.UUID(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        pass
    client_id = uuid.uuid4()
    try:
        path.parent.mkdir(parents=True, exist_ok=True, mode=DIRECTORY_MODE)
        path.write_text(str(client_id), encoding="utf-8")
        path.chmod(FILE_MODE)
    except OSError:
        # A read-only config directory costs a new device per login, which is
        # worse than reusing one but still authenticates.
        pass
    return client_id
