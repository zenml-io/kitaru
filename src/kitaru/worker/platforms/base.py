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
"""Worker platform protocol for spawning and tearing down task processes."""

import asyncio
from typing import Protocol


class WorkerPlatform(Protocol):
    """Per-OS strategy for spawning and tearing down task processes."""

    async def spawn(
        self,
        command: str | list[str],
        working_dir: str | None,
        env: dict[str, str],
    ) -> asyncio.subprocess.Process:
        """Spawn a task process with stdout and stderr piped.

        Args:
            command: Shell command to run through a shell, or an argument
                vector to execute directly.
            working_dir: Working directory, None inherits the worker's own.
            env: Process environment.

        Returns:
            Spawned process.
        """
        ...

    async def kill_tree(self, process: asyncio.subprocess.Process) -> None:
        """Kill the process and every descendant it spawned.

        Args:
            process: Process to kill.
        """
        ...

    def stop_signals(self) -> tuple[int, ...]:
        """Signals a foreground supervisor should install handlers for.

        Returns:
            Signals to handle.
        """
        ...
