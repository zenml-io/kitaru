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
"""Kitaru API client."""

import os
from collections.abc import AsyncIterable
from types import TracebackType
from typing import Any

import httpx

from kitaru.analytics.source import (
    CLIENT_HEADER,
    AnalyticsSource,
    format_client_header,
)
from kitaru.api_models.v1.auth import CONTROL_PLANE_API_KEY_PREFIX
from kitaru.client.auth import TokenProvider
from kitaru.client.credential_store import CredentialStore
from kitaru.client.exceptions import raise_for_response
from kitaru.client.resources.accounts import AccountsResource
from kitaru.client.resources.agent_versions import AgentVersionsResource
from kitaru.client.resources.agents import AgentsResource
from kitaru.client.resources.api_keys import ApiKeysResource
from kitaru.client.resources.auth import AuthResource
from kitaru.client.resources.blobs import BlobsResource
from kitaru.client.resources.cohort_versions import CohortVersionsResource
from kitaru.client.resources.cohorts import CohortsResource
from kitaru.client.resources.devices import DevicesResource
from kitaru.client.resources.evaluations import EvaluationsResource
from kitaru.client.resources.evaluators import EvaluatorsResource
from kitaru.client.resources.experiment_runs import ExperimentRunsResource
from kitaru.client.resources.experiments import ExperimentsResource
from kitaru.client.resources.importers import ImportersResource
from kitaru.client.resources.imports import ImportsResource
from kitaru.client.resources.info import InfoResource
from kitaru.client.resources.jobs import JobsResource
from kitaru.client.resources.replays import ReplaysResource
from kitaru.client.resources.secrets import SecretsResource
from kitaru.client.resources.session_runs import SessionRunsResource
from kitaru.client.resources.sessions import SessionsResource
from kitaru.client.resources.tags import TagsResource
from kitaru.client.resources.tasks import TasksResource
from kitaru.client.resources.workers import WorkersResource
from kitaru.transport import build_async_client


class KitaruAPIClient:
    """Kitaru API client."""

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        credential_store: CredentialStore | None = None,
        timeout: float = 30.0,
        retries: int = 3,
        pool_size: int = 20,
    ) -> None:
        """Initialize the client.

        Args:
            base_url: Server base URL.
            api_key: API key authenticating this client. A server key is sent
                as a bearer token, a control plane key is exchanged for a
                session token held in memory. Ignored when a credential store
                is supplied.
            credential_store: Store holding the credentials this client
                authenticates with, renewing its token as it expires.
            timeout: Request timeout in seconds.
            retries: Retry count for failed requests.
            pool_size: Connection pool size.
        """
        identification = format_client_header(AnalyticsSource.PYTHON)
        headers = {"User-Agent": identification, CLIENT_HEADER: identification}
        if api_key and credential_store is None:
            if api_key.startswith(CONTROL_PLANE_API_KEY_PREFIX):
                # Control plane API keys are exchanged for a session token, so
                # we need an in-memory credential store to hold it.
                credential_store = CredentialStore(persist=False)
                credential_store.set_api_key(base_url, api_key)
            else:
                headers["Authorization"] = f"Bearer {api_key}"
        self._http = build_async_client(
            base_url, headers, timeout=timeout, retries=retries, pool_size=pool_size
        )
        self.accounts = AccountsResource(self)
        self.agents = AgentsResource(self)
        self.agent_versions = AgentVersionsResource(self)
        self.api_keys = ApiKeysResource(self)
        self.auth = AuthResource(self)
        self.blobs = BlobsResource(self)
        self.cohorts = CohortsResource(self)
        self.cohort_versions = CohortVersionsResource(self)
        self.devices = DevicesResource(self)
        self.evaluations = EvaluationsResource(self)
        self.evaluators = EvaluatorsResource(self)
        self.experiments = ExperimentsResource(self)
        self.experiment_runs = ExperimentRunsResource(self)
        self.importers = ImportersResource(self)
        self.imports = ImportsResource(self)
        self.info = InfoResource(self)
        self.jobs = JobsResource(self)
        self.replays = ReplaysResource(self)
        self.secrets = SecretsResource(self)
        self.session_runs = SessionRunsResource(self)
        self.sessions = SessionsResource(self)
        self.tags = TagsResource(self)
        self.tasks = TasksResource(self)
        self.workers = WorkersResource(self)
        self._auth: TokenProvider | None = None
        if credential_store is not None:
            self._auth = TokenProvider(base_url, credential_store, self.auth)

    @classmethod
    def from_env(cls) -> "KitaruAPIClient":
        """Construct a client from KITARU_API_URL and KITARU_API_KEY.

        Raises:
            RuntimeError: KITARU_API_URL is not set.

        Returns:
            Client.
        """
        base_url = os.environ.get("KITARU_API_URL")
        if not base_url:
            raise RuntimeError("KITARU_API_URL is not set")
        return cls(base_url=base_url, api_key=os.environ.get("KITARU_API_KEY"))

    async def request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: Any = None,
        data: dict[str, str] | None = None,
        files: dict[str, tuple[str | None, bytes, str]] | None = None,
        content: bytes | AsyncIterable[bytes] | None = None,
        headers: dict[str, str] | None = None,
        authenticate: bool = True,
    ) -> httpx.Response:
        """Send a request and raise a typed error on failure.

        A request rejected with HTTP 401 is retried once with a renewed token,
        unless another caller renewed it first, in which case the retry uses
        theirs.

        Args:
            method: HTTP method.
            path: Request path relative to the base URL.
            params: Query parameters.
            json: JSON request body.
            data: Form request body.
            files: Multipart file fields, filename/content/content-type per
                field.
            content: Raw or streaming request body.
            headers: Additional request headers.
            authenticate: Whether to attach a bearer token from the credential
                store. The login endpoints send their own credential.

        Raises:
            APIError: The response has an error status code.

        Returns:
            HTTP response.
        """
        provider = self._auth if authenticate else None
        generation = provider.generation if provider is not None else 0
        request_headers = dict(headers or {})

        async def send() -> httpx.Response:
            return await self._http.request(
                method,
                path,
                params=params,
                json=json,
                data=data,
                files=files,
                content=content,
                headers=request_headers or None,
            )

        if provider is not None:
            token = await provider.get_token()
            if token is not None:
                request_headers["Authorization"] = f"Bearer {token}"
        response = await send()
        if response.status_code == httpx.codes.UNAUTHORIZED and provider is not None:
            await response.aclose()
            token = await provider.renew(generation)
            if token is not None:
                request_headers["Authorization"] = f"Bearer {token}"
                response = await send()
        raise_for_response(response)
        return response

    async def close(self) -> None:
        """Close the underlying HTTP clients."""
        if self._auth is not None:
            await self._auth.close()
        await self._http.aclose()

    async def __aenter__(self) -> "KitaruAPIClient":
        """Enter the context manager.

        Returns:
            The client.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit the context manager and close the client.

        Args:
            exc_type: Exception type.
            exc: Exception instance.
            traceback: Exception traceback.
        """
        await self.close()
