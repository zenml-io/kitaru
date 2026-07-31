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
"""On-disk store for server credentials."""

import json
import logging
import os
import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pydantic import ValidationError

from kitaru.client.credentials import (
    ApiToken,
    ApiType,
    ServerCredentials,
)

logger = logging.getLogger(__name__)

ENV_CREDENTIALS_PATH = "KITARU_CREDENTIALS_PATH"
ENV_DISABLE_CREDENTIALS_CACHE = "KITARU_DISABLE_CREDENTIALS_CACHE"

CREDENTIALS_FILE_NAME = "credentials.json"
# Only the owner may read the file or list the directory holding it.
FILE_MODE = 0o600
DIRECTORY_MODE = 0o700
# Entries with an expired token and no way to refresh it are dropped once the
# token has been useless for this long.
EVICTION_AGE = timedelta(days=7)


def get_config_directory() -> Path:
    """Return the directory holding Kitaru client configuration.

    Returns:
        ``$XDG_CONFIG_HOME/kitaru``, falling back to ``~/.config/kitaru``.
    """
    base = os.environ.get("XDG_CONFIG_HOME")
    root = Path(base) if base else Path.home() / ".config"
    return root / "kitaru"


def normalize_server_url(url: str) -> str:
    """Normalize a server URL into the key credentials are stored under.

    Args:
        url: Server base URL.

    Returns:
        URL without a trailing slash.
    """
    return url.rstrip("/")


class CredentialStore:
    """Server credentials persisted as JSON in the user's config directory."""

    def __init__(self, path: Path | None = None, persist: bool | None = None) -> None:
        """Initialize the store and load any credentials already on disk.

        Args:
            path: Location of the credentials file.
            persist: Whether to read and write the file. Defaults to reading
                ``KITARU_DISABLE_CREDENTIALS_CACHE``.
        """
        self._path = path or _get_default_path()
        if persist is None:
            persist = os.environ.get(ENV_DISABLE_CREDENTIALS_CACHE, "").lower() not in (
                "1",
                "true",
                "yes",
            )
        self._persist = persist
        self._credentials: dict[str, ServerCredentials] = {}
        self._loaded_at: float | None = None
        self._load()

    @property
    def path(self) -> Path:
        """Return the location of the credentials file.

        Returns:
            Path the store reads and writes.
        """
        return self._path

    def get(self, url: str) -> ServerCredentials | None:
        """Return the credentials stored for a server.

        Args:
            url: Server base URL.

        Returns:
            Stored credentials, or None when the server is unknown.
        """
        self._reload_if_stale()
        return self._credentials.get(normalize_server_url(url))

    def list(self) -> list[ServerCredentials]:
        """Return the credentials of every known server.

        Returns:
            Stored credentials, ordered by server URL.
        """
        self._reload_if_stale()
        return [self._credentials[key] for key in sorted(self._credentials)]

    def get_token(self, url: str, allow_expired: bool = False) -> ApiToken | None:
        """Return the cached token of a server.

        Args:
            url: Server base URL.
            allow_expired: Whether to return a token inside its leeway window.

        Returns:
            Cached token, or None when there is none to return.
        """
        credentials = self.get(url)
        if credentials is None or credentials.api_token is None:
            return None
        if credentials.api_token.expired and not allow_expired:
            return None
        return credentials.api_token

    def set_token(
        self,
        url: str,
        token: ApiToken,
        type: ApiType | None = None,
        control_plane_api_url: str | None = None,
    ) -> None:
        """Cache a token for a server.

        The control plane is recorded with the token it issued rather than on
        its own, because an entry that holds neither a token nor a credential
        is dropped when the store is written.

        Args:
            url: Server base URL.
            token: Token to cache.
            type: API type, left as stored when omitted.
            control_plane_api_url: Control plane that issued the credential
                behind this token, left as stored when omitted.
        """
        values: dict[str, object] = {"api_token": token}
        if type is not None:
            values["type"] = type
        if control_plane_api_url is not None:
            values["control_plane_api_url"] = normalize_server_url(
                control_plane_api_url
            )
        self._mutate(url, values)

    def set_api_key(
        self,
        url: str,
        api_key: str,
        type: ApiType = ApiType.SERVER,
    ) -> None:
        """Store an API key for a server and drop the token it replaces.

        Args:
            url: Server base URL.
            api_key: API key to store.
            type: API type.
        """
        self._mutate(url, {"api_key": api_key, "type": type, "api_token": None})

    def set_device(self, url: str, device_id: uuid.UUID, device_code: str) -> None:
        """Store a device authorization for a server.

        Args:
            url: Server base URL.
            device_id: Id of the authorized device.
            device_code: Device code the client polls with.
        """
        self._mutate(url, {"device_id": device_id, "device_code": device_code})

    def get_control_plane(self, control_plane_api_url: str) -> ServerCredentials | None:
        """Return the credentials stored for a control plane.

        Args:
            control_plane_api_url: Control plane API base URL.

        Returns:
            Stored credentials, or None when the entry is missing or is not a
            control plane entry.
        """
        credentials = self.get(control_plane_api_url)
        if credentials is None or credentials.type is not ApiType.CONTROL_PLANE:
            return None
        return credentials

    def clear_token(self, url: str) -> None:
        """Drop the cached token of a server, keeping the way to get a new one.

        Args:
            url: Server base URL.
        """
        if self.get(url) is None:
            return
        self._mutate(url, {"api_token": None})

    def clear(self, url: str) -> None:
        """Drop every credential stored for a server.

        Args:
            url: Server base URL.
        """
        self._reload_if_stale()
        if self._credentials.pop(normalize_server_url(url), None) is not None:
            self._save()

    def clear_all(self) -> None:
        """Drop every stored credential and remove the credentials file."""
        self._credentials = {}
        if not self._persist:
            return
        self._path.unlink(missing_ok=True)
        self._loaded_at = None

    def _mutate(self, url: str, values: dict[str, object]) -> None:
        """Apply changes to one server's credentials and write them out.

        Args:
            url: Server base URL.
            values: Fields to set on the entry.
        """
        self._reload_if_stale()
        key = normalize_server_url(url)
        current = self._credentials.get(key) or ServerCredentials(url=key)
        self._credentials[key] = current.model_copy(update=values)
        self._save()

    def _load(self) -> None:
        """Read the credentials file, ignoring one that cannot be parsed."""
        if not self._persist:
            return
        try:
            raw = self._path.read_text(encoding="utf-8")
            self._loaded_at = self._path.stat().st_mtime
        except OSError:
            self._credentials = {}
            self._loaded_at = None
            return
        try:
            payload = json.loads(raw)
        except ValueError:
            logger.warning("Ignoring malformed credentials file %s.", self._path)
            self._credentials = {}
            return
        credentials: dict[str, ServerCredentials] = {}
        for key, entry in (payload or {}).items():
            try:
                credentials[key] = ServerCredentials.model_validate(entry)
            except ValidationError:
                logger.warning("Ignoring malformed credentials for %s.", key)
        self._credentials = credentials

    def _reload_if_stale(self) -> None:
        """Reload the credentials file when another process has written it."""
        if not self._persist:
            return
        try:
            modified_at = self._path.stat().st_mtime
        except OSError:
            return
        if self._loaded_at is None or modified_at != self._loaded_at:
            self._load()

    def _save(self) -> None:
        """Write the credentials file, replacing it in one step."""
        if not self._persist:
            return
        payload = {
            key: entry.model_dump(mode="json", exclude_none=True)
            for key, entry in self._credentials.items()
            if _should_persist(entry)
        }
        self._path.parent.mkdir(parents=True, exist_ok=True, mode=DIRECTORY_MODE)
        # A partial write must never replace a good file, and the secrets must
        # never be readable between creating the file and setting its mode.
        handle, temporary = tempfile.mkstemp(
            dir=self._path.parent, prefix=f".{self._path.name}."
        )
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as file:
                json.dump(payload, file, indent=2, sort_keys=True)
            os.chmod(temporary, FILE_MODE)
            os.replace(temporary, self._path)
        except OSError:
            Path(temporary).unlink(missing_ok=True)
            raise
        self._loaded_at = self._path.stat().st_mtime


def _should_persist(credentials: ServerCredentials) -> bool:
    """Report whether an entry still earns its place in the file.

    Args:
        credentials: Entry about to be written.

    Returns:
        Whether the entry can still produce a token, or holds one that has not
        been useless for long.
    """
    if credentials.can_refresh:
        return True
    token = credentials.api_token
    if token is None:
        return False
    if token.expires_at is None:
        return True
    return datetime.now(UTC) - token.expires_at < EVICTION_AGE


def _get_default_path() -> Path:
    """Return the credentials file location.

    Returns:
        ``KITARU_CREDENTIALS_PATH`` when set, otherwise the file in the Kitaru
        config directory.
    """
    override = os.environ.get(ENV_CREDENTIALS_PATH)
    if override:
        return Path(override)
    return get_config_directory() / CREDENTIALS_FILE_NAME
