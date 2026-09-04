#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Import CLI commands."""

import uuid
from typing import Any

from kitaru.cli.output import CommandResult
from kitaru.cli.registration import list_params, page_result


async def list_imports(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> CommandResult:
    """List one server page of imports."""
    params = list_params("import", size=size, cursor=cursor, sort=sort, filter=filter)
    return page_result(await client.imports.list(params), size=size)


async def get_import(client: Any, import_id: uuid.UUID) -> CommandResult:
    """Get one import without remapping its status."""
    import_ = await client.imports.get(import_id)
    return CommandResult(item=import_.model_dump(mode="json"))
