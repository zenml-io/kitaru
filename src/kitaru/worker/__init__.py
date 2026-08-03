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
"""Lazy exports for the optional client-side task execution worker."""

from typing import Any

__all__ = ["Worker", "WorkerConfig"]


def __getattr__(name: str) -> Any:
    """Load worker-only dependencies when a public runtime export is requested."""
    if name == "Worker":
        from kitaru.worker.worker import Worker

        return Worker
    if name == "WorkerConfig":
        from kitaru.worker.config import WorkerConfig

        return WorkerConfig
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
