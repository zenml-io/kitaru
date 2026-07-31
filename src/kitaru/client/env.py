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
"""Client process environment reading."""

import os


def get_required_env(name: str) -> str:
    """Read an environment variable the client or worker process requires.

    Args:
        name: Environment variable name.

    Raises:
        RuntimeError: The variable is missing or empty.

    Returns:
        Variable value.
    """
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value
