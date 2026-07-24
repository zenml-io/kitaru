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
"""Executable entry point running the mock agent under the Kitaru adapter."""

import json
import os
import sys
import uuid
from typing import Any

from v2_examples.mock_adapter.adapter import (  # ty: ignore[unresolved-import]
    KitaruAdapter,
)
from v2_examples.mock_adapter.agent import (  # ty: ignore[unresolved-import]
    DEFAULT_MODEL,
    DEFAULT_SYSTEM_PROMPT,
    MockAgent,
)

DEFAULT_INPUTS = {"question": "What is the weather in Berlin, and what is 21 * 2?"}


def build_agent() -> MockAgent:
    """Build the mock agent from KITARU_E2E_* environment variables."""
    model_params: dict[str, Any] | None = None
    raw_params = os.environ.get("KITARU_E2E_MODEL_PARAMS")
    if raw_params:
        model_params = json.loads(raw_params)
    return MockAgent(
        model=os.environ.get("KITARU_E2E_MODEL", DEFAULT_MODEL),
        system_prompt=os.environ.get("KITARU_E2E_SYSTEM_PROMPT", DEFAULT_SYSTEM_PROMPT),
        model_params=model_params,
    )


def parse_inputs(argument: str) -> Any:
    """Parse a CLI inputs argument as JSON, falling back to a question string."""
    try:
        return json.loads(argument)
    except json.JSONDecodeError:
        return {"question": argument}


def main(argv: list[str]) -> int:
    """Run the agent once and print its output."""
    agent = build_agent()
    version_env = os.environ.get("KITARU_E2E_AGENT_VERSION_ID")
    adapter = KitaruAdapter(
        agent,
        agent_id=uuid.UUID(os.environ["KITARU_E2E_AGENT_ID"]),
        agent_version_id=uuid.UUID(version_env) if version_env else None,
    )
    try:
        default = parse_inputs(argv[1]) if len(argv) > 1 else DEFAULT_INPUTS
        inputs = adapter.resolve_inputs(default)
        try:
            output = agent.run(inputs)
        except Exception as error:
            print(f"Run failed: {error}", file=sys.stderr)
            return 1
        print(output)
        return 0
    finally:
        adapter.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
