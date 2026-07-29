"""Shared SDK cursor pagination."""

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TypeVar

from kitaru.api_models.v1.base import ListParams, Page, ResponseModel

ItemT = TypeVar("ItemT", bound=ResponseModel)
ParamsT = TypeVar("ParamsT", bound=ListParams)


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
