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
"""Worker launcher backed by Modal sandboxes."""

import modal

from kitaru.server.application.models.worker import WorkerLaunch
from kitaru.server.worker_launcher_settings import ModalWorkerLauncherSettings

WORKER_COMMAND = ("python", "-m", "kitaru.worker")


class ModalWorkerLauncher:
    """Worker launcher starting one Modal sandbox per worker."""

    def __init__(self, settings: ModalWorkerLauncherSettings) -> None:
        """Initialize the launcher.

        Args:
            settings: Modal worker launcher settings.
        """
        self._settings = settings

    async def launch(self, command: WorkerLaunch) -> None:
        """Start a worker for a job.

        Args:
            command: Worker launch.
        """
        client = await modal.Client.from_credentials.aio(
            self._settings.token_id, self._settings.token_secret.get_secret_value()
        )
        async with client:
            app = await modal.App.lookup.aio(
                self._settings.app_name, client=client, create_if_missing=True
            )
            await modal.Sandbox.create.aio(
                *WORKER_COMMAND,
                app=app,
                image=modal.Image.from_registry(self._settings.image),
                env={
                    "KITARU_API_URL": command.server_url,
                    "KITARU_API_TOKEN": command.worker_token.get_secret_value(),
                    "KITARU_WORKER_ID": str(command.worker_id),
                    "KITARU_WORKER_TIMEOUT": str(self._settings.timeout_seconds),
                },
                timeout=self._settings.timeout_seconds,
                cpu=self._settings.cpu,
                memory=self._settings.memory_mb,
                client=client,
            )
