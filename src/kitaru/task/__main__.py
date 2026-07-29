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
"""Task process entry point: python -m kitaru.task <evaluate|import>."""

import argparse
import asyncio
import sys

from kitaru.client.api_client import KitaruAPIClient
from kitaru.task import evaluator, importer
from kitaru.task.task_io import get_required_env


def _parse_kind(argv: list[str]) -> str:
    """Parse the task kind from argv, rejecting anything else.

    Args:
        argv: Command-line arguments, excluding the program name.

    Returns:
        Task kind, evaluate or import.
    """
    parser = argparse.ArgumentParser(prog="python -m kitaru.task")
    parser.add_argument("kind", choices=["evaluate", "import"])
    return parser.parse_args(argv).kind


async def _run_flow(kind: str, client: KitaruAPIClient, task_id: str) -> None:
    """Dispatch to the flow matching the task kind.

    Args:
        kind: Task kind, evaluate or import.
        client: API client.
        task_id: Id of the task.
    """
    if kind == "evaluate":
        await evaluator.run(client, task_id)
    else:
        await importer.run(client, task_id)


def main() -> None:
    """Run the task process and exit 1 with the error on stderr on failure."""
    kind = _parse_kind(sys.argv[1:])
    try:
        task_id = get_required_env("KITARU_TASK_ID")

        async def _main() -> None:
            async with KitaruAPIClient.from_env() as client:
                await _run_flow(kind, client, task_id)

        asyncio.run(_main())
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
