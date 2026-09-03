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
"""Worker launcher backend configuration."""

from enum import StrEnum
from typing import Self

from pydantic import BaseModel, SecretStr, model_validator

from kitaru.images import WORKER_IMAGE_REPOSITORY, get_image

DEFAULT_WORKER_COMMAND = "python -m kitaru.worker"


class WorkerLauncherBackend(StrEnum):
    """Worker launcher backend."""

    NONE = "none"
    MODAL = "modal"


class ModalWorkerLauncherSettings(BaseModel):
    """Modal worker launcher settings."""

    token_id: str
    token_secret: SecretStr
    app_name: str = "kitaru-workers"
    cpu: float | None = None
    memory_mb: int | None = None


class WorkerLauncherSettings(BaseModel):
    """Worker launcher settings."""

    backend: WorkerLauncherBackend = WorkerLauncherBackend.NONE
    image: str | None = None
    command: str = DEFAULT_WORKER_COMMAND
    timeout_seconds: int = 3600
    modal: ModalWorkerLauncherSettings | None = None

    @model_validator(mode="after")
    def validate_backend_settings(self) -> Self:
        """Validate the settings match the selected backend.

        Raises:
            ValueError: The selected backend has no matching sub-model set.

        Returns:
            The validated settings object.
        """
        if self.backend is WorkerLauncherBackend.MODAL and self.modal is None:
            raise ValueError(
                "Set KITARU_SERVER_WORKER_LAUNCHER__MODAL__TOKEN_ID when "
                "KITARU_SERVER_WORKER_LAUNCHER__BACKEND=modal"
            )
        return self

    def get_image(self) -> str:
        """Get the worker image, the published image at this version when unset.

        Raises:
            ValueError: No image is set and this version has no published image.

        Returns:
            Worker image reference.
        """
        if self.image is not None:
            return self.image
        try:
            return get_image(WORKER_IMAGE_REPOSITORY)
        except ValueError as error:
            raise ValueError(
                f"Set KITARU_SERVER_WORKER_LAUNCHER__IMAGE, {error}"
            ) from error
