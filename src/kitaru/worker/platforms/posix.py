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
"""POSIX task process spawning and teardown."""

import asyncio
import contextlib
import os
import signal


class PosixPlatform:
    """Spawns task processes in their own session and kills the process group."""

    async def spawn(
        self,
        command: str | list[str],
        working_dir: str | None,
        env: dict[str, str],
    ) -> asyncio.subprocess.Process:
        """Spawn a task process in a new session, piping stdout and stderr.

        Args:
            command: Shell command to run through a shell, or an argument
                vector to execute directly.
            working_dir: Working directory, None inherits the worker's own.
            env: Process environment.

        Returns:
            Spawned process.
        """
        if isinstance(command, str):
            return await asyncio.create_subprocess_shell(
                command,
                cwd=working_dir,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        return await asyncio.create_subprocess_exec(
            *command,
            cwd=working_dir,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )

    async def kill_tree(self, process: asyncio.subprocess.Process) -> None:
        """Kill the process group the task process leads.

        Args:
            process: Process to kill.
        """
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)

    def stop_signals(self) -> tuple[int, ...]:
        """Signals a foreground supervisor should install handlers for.

        Returns:
            SIGINT and SIGTERM.
        """
        return (signal.SIGINT, signal.SIGTERM)
