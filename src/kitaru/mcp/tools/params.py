#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Shared marshalling from MCP list requests to SDK list params."""

from typing import TypeVar

from pydantic import BaseModel, ValidationError

from kitaru.api_models.v1.base import CursorParams
from kitaru.mcp.errors import map_exception

ParamsT = TypeVar("ParamsT", bound=CursorParams)

_LIST_FIELDS = frozenset({"cursor", "size", "sort", "filter"})


def build_list_params(
    params_type: type[ParamsT], request: BaseModel, *, with_filter: bool = True
) -> ParamsT:
    """Build SDK list params from a validated MCP list request.

    Args:
        params_type: SDK list params model to build.
        request: MCP list request carrying the pagination fields.
        with_filter: Whether the target endpoint accepts a filter.

    Raises:
        MCPToolError: The pagination fields are invalid for these list params.

    Returns:
        The SDK list params.
    """
    fields = _LIST_FIELDS if with_filter else _LIST_FIELDS - {"filter"}
    # Boolean filter operands are named after the `and`/`or`/`not` keywords, so
    # only the aliased dump validates back into the SDK filter models.
    common = request.model_dump(include=set(fields), by_alias=True)
    try:
        return params_type.model_validate(common)
    except ValidationError as error:
        # A request the handler cannot marshal is a client-side fault, not a
        # failure of the Kitaru response against the MCP output schema.
        raise map_exception(error) from error
