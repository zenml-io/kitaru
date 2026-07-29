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
"""Task process entrypoint."""

import asyncio
import sys

from kitaru.client.api_client import KitaruAPIClient
from kitaru.task import evaluator, importer
from kitaru.task.task_io import get_required_env


async def _run(kind: str, task_id: str) -> None:
    """Open the task client and run the selected task flow."""
    async with KitaruAPIClient.from_env() as client:
        if kind == "evaluate":
            await evaluator.run(client, task_id)
        else:
            await importer.run(client, task_id)


def main(argv: list[str] | None = None) -> int:
    """Run a task subprocess.

    Args:
        argv: Optional arguments excluding the program name.

    Returns:
        Process exit code.
    """
    arguments = sys.argv[1:] if argv is None else argv
    try:
        if len(arguments) != 1 or arguments[0] not in {"evaluate", "import"}:
            raise RuntimeError(
                "Expected exactly one task kind: 'evaluate' or 'import'."
            )
        task_id = get_required_env("KITARU_TASK_ID")
        asyncio.run(_run(arguments[0], task_id))
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
