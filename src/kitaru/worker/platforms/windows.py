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
"""Windows task process spawning and teardown."""

import asyncio
import contextlib
import signal
import subprocess
import sys

if sys.platform == "win32":
    _CREATE_NEW_PROCESS_GROUP = subprocess.CREATE_NEW_PROCESS_GROUP
    _STOP_SIGNALS: tuple[int, ...] = (signal.SIGINT, signal.SIGBREAK)
else:
    _CREATE_NEW_PROCESS_GROUP = 0
    _STOP_SIGNALS = (signal.SIGINT,)


class WindowsPlatform:
    """Spawns task processes in their own process group and kills via taskkill."""

    async def spawn(
        self,
        command: str | list[str],
        working_dir: str | None,
        env: dict[str, str],
    ) -> asyncio.subprocess.Process:
        """Spawn a task process in a new process group, piping stdout and stderr.

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
                creationflags=_CREATE_NEW_PROCESS_GROUP,
            )
        return await asyncio.create_subprocess_exec(
            *command,
            cwd=working_dir,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=_CREATE_NEW_PROCESS_GROUP,
        )

    async def kill_tree(self, process: asyncio.subprocess.Process) -> None:
        """Kill the process tree rooted at the task process.

        Falls back to killing the process itself when taskkill is
        unavailable or fails.

        Args:
            process: Process to kill.
        """
        try:
            taskkill = await asyncio.create_subprocess_exec(
                "taskkill",
                "/F",
                "/T",
                "/PID",
                str(process.pid),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            returncode = await taskkill.wait()
        except OSError:
            returncode = None
        if returncode != 0:
            with contextlib.suppress(ProcessLookupError):
                process.kill()

    def stop_signals(self) -> tuple[int, ...]:
        """Signals a foreground supervisor should install handlers for.

        Returns:
            SIGINT and SIGBREAK.
        """
        return _STOP_SIGNALS
