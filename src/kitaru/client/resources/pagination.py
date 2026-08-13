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
"""Shared SDK cursor pagination."""

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TypeVar

from kitaru.api_models.v1.base import CursorParams, Page, ResponseModel

ItemT = TypeVar("ItemT", bound=ResponseModel)
ParamsT = TypeVar("ParamsT", bound=CursorParams)


async def iterate_pages(
    params: ParamsT,
    load_page: Callable[[ParamsT], Awaitable[Page[ItemT]]],
) -> AsyncIterator[ItemT]:
    """Yield every item while following opaque page cursors."""
    while True:
        page = await load_page(params)
        for item in page.items:
            yield item
        if page.next_cursor is None:
            return
        params = params.model_copy(update={"cursor": page.next_cursor})
