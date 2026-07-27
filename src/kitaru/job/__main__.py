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
"""Process entry: python -m kitaru.job <score|import>."""

import asyncio
import os
import sys
import uuid

from kitaru.client.api_client import KitaruAPIClient
from kitaru.job import importer, scorer

_KINDS = {"score": scorer.run, "import": importer.run}


def _required_env(name: str) -> str:
    """Read an environment variable of the process contract.

    Args:
        name: Name of the variable.

    Raises:
        RuntimeError: The variable is not set.

    Returns:
        Value of the variable.
    """
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


async def _run(kind: str) -> None:
    """Run the flow of a job kind against the ambient environment.

    Args:
        kind: Job kind, "score" or "import".
    """
    job_id = uuid.UUID(_required_env("KITARU_JOB_ID"))
    async with KitaruAPIClient(
        base_url=_required_env("KITARU_API_URL"),
        api_key=_required_env("KITARU_API_KEY"),
    ) as client:
        await _KINDS[kind](client, job_id)


def main() -> int:
    """Run the job process.

    Returns:
        Exit code.
    """
    if len(sys.argv) != 2 or sys.argv[1] not in _KINDS:
        print(f"Usage: python -m kitaru.job {{{'|'.join(_KINDS)}}}", file=sys.stderr)
        return 1
    try:
        asyncio.run(_run(sys.argv[1]))
    except Exception as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
