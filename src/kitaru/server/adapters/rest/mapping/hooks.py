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
"""Task hook DTO conversions."""

from kitaru.api_models.v1.hook import (
    CopyWorkdirHook as WireCopyWorkdirHook,
)
from kitaru.api_models.v1.hook import (
    SetupCommandHook as WireSetupCommandHook,
)
from kitaru.api_models.v1.hook import (
    TaskHook as WireTaskHook,
)
from kitaru.api_models.v1.hook import (
    TeardownCommandHook as WireTeardownCommandHook,
)
from kitaru.server.domain.hook import (
    CopyWorkdirHook,
    SetupCommandHook,
    TaskHook,
    TeardownCommandHook,
)


def hook_to_domain(hook: WireTaskHook) -> TaskHook:
    """Convert a wire task hook to its domain value object.

    Args:
        hook: Wire task hook.

    Returns:
        Domain task hook.
    """
    if isinstance(hook, WireCopyWorkdirHook):
        return CopyWorkdirHook()
    if isinstance(hook, WireSetupCommandHook):
        return SetupCommandHook(command=hook.command)
    return TeardownCommandHook(command=hook.command, on=hook.on)


def hook_to_response(hook: TaskHook) -> WireTaskHook:
    """Convert a domain task hook to its wire value object.

    Args:
        hook: Domain task hook.

    Returns:
        Wire task hook.
    """
    if isinstance(hook, CopyWorkdirHook):
        return WireCopyWorkdirHook()
    if isinstance(hook, SetupCommandHook):
        return WireSetupCommandHook(command=hook.command)
    return WireTeardownCommandHook(command=hook.command, on=hook.on)
