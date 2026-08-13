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
"""Public SDK surface."""

from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.auth import (
    CredentialStoreTokenSource,
    RenewingTokenAuth,
    StaticTokenAuth,
    TokenAuth,
    TokenSource,
)
from kitaru.client.client import KitaruClient
from kitaru.client.config import (
    ClientConfig,
    get_server_url,
    load_config,
    save_config,
    set_server_url,
)
from kitaru.client.control_plane import (
    ControlPlaneLoginError,
    ControlPlaneSession,
)
from kitaru.client.control_plane_auth import control_plane_login
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ApiToken, ApiType, ServerCredentials
from kitaru.client.device_auth import device_login
from kitaru.client.device_grant import DeviceLoginError
from kitaru.client.exceptions import (
    APIError,
    AuthenticationError,
    AuthorizationError,
    KitaruClientError,
    NotFoundError,
    ServerError,
    TokenGrantError,
    ValidationError,
)
from kitaru.client.sync_client import KitaruSyncClient

__all__ = [
    "APIError",
    "ApiToken",
    "ApiType",
    "AuthenticationError",
    "AuthorizationError",
    "ClientConfig",
    "ControlPlaneLoginError",
    "ControlPlaneSession",
    "CredentialStore",
    "CredentialStoreTokenSource",
    "DeviceLoginError",
    "KitaruAPIClient",
    "KitaruClient",
    "KitaruClientError",
    "KitaruSyncClient",
    "NotFoundError",
    "RenewingTokenAuth",
    "ServerCredentials",
    "ServerError",
    "StaticTokenAuth",
    "TokenAuth",
    "TokenGrantError",
    "TokenSource",
    "ValidationError",
    "control_plane_login",
    "device_login",
    "get_server_url",
    "load_config",
    "save_config",
    "set_server_url",
]
