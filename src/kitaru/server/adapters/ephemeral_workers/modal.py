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
"""Ephemeral workers backed by Modal sandboxes."""

import shlex

import modal

from kitaru.server.application.models.worker import EphemeralWorkerSpec
from kitaru.server.ephemeral_worker_settings import EphemeralWorkerSettings


class ModalEphemeralWorkers:
    """Ephemeral workers starting one Modal sandbox per worker."""

    def __init__(self, settings: EphemeralWorkerSettings) -> None:
        """Initialize the backend.

        Args:
            settings: Ephemeral worker settings.
        """
        # Settings validation requires the modal sub-model under the modal
        # backend, the only backend that constructs this adapter.
        assert settings.modal is not None
        self._modal_settings = settings.modal
        self._image = settings.get_image()
        self._command = shlex.split(settings.command)
        self._timeout_seconds = settings.timeout_seconds
        self._client: modal.Client | None = None

    async def _get_client(self) -> modal.Client:
        """Get the Modal client, opening it on the first call.

        Returns:
            Authenticated client.
        """
        # from_credentials returns the client with its connection open and the
        # SDK exposes no close, so one client serves every start.
        if self._client is None:
            self._client = await modal.Client.from_credentials.aio(
                self._modal_settings.token_id,
                self._modal_settings.token_secret.get_secret_value(),
            )
        return self._client

    async def start(self, spec: EphemeralWorkerSpec) -> None:
        """Start a worker for a job.

        Args:
            spec: Ephemeral worker spec.
        """
        client = await self._get_client()
        app = await modal.App.lookup.aio(
            self._modal_settings.app_name, client=client, create_if_missing=True
        )
        await modal.Sandbox.create.aio(
            *self._command,
            app=app,
            image=modal.Image.from_registry(self._image),
            env={
                "KITARU_API_URL": spec.server_url,
                "KITARU_API_TOKEN": spec.worker_token.get_secret_value(),
                "KITARU_WORKER_ID": str(spec.worker_id),
                "KITARU_WORKER_TIMEOUT": str(self._timeout_seconds),
            },
            timeout=self._timeout_seconds,
            cpu=self._modal_settings.cpu,
            memory=self._modal_settings.memory_mb,
            client=client,
        )
