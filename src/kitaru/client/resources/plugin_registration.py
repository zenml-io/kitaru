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
"""Plugin registration flow shared by the scorer and importer resources."""

from collections.abc import Awaitable, Callable
from typing import TypeVar

from kitaru.api_models.v1.base import Page, ResponseModel
from kitaru.client.exceptions import ConflictError

PluginT = TypeVar("PluginT", bound=ResponseModel)


async def resolve_or_create(
    create: Callable[[], Awaitable[PluginT]],
    find: Callable[[], Awaitable[Page[PluginT]]],
) -> PluginT:
    """Create a plugin, falling back to the one already registered.

    Args:
        create: Creates the plugin.
        find: Lists the plugin by its name.

    Raises:
        APIError: A request failed.

    Returns:
        Created or already registered plugin.
    """
    try:
        return await create()
    except ConflictError:
        return (await find()).items[0]
