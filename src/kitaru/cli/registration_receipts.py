#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Receipts for successful parent and version registration."""

from typing import Any

from kitaru.cli.output import CommandResult


async def build_registration_result(
    kind: str, resource: Any, parent: Any, version: Any, *, blob: Any | None = None
) -> CommandResult:
    """Refresh parent state and build a successful multi-phase receipt."""
    warnings: list[str] = []
    try:
        parent = await resource.get(parent.id)
    except Exception:
        warnings.append(
            "Registration succeeded, but the parent could not be refreshed. "
            "Parent fields use the earlier snapshot with latest_version advanced "
            "to at least the confirmed version. Do not retry registration; "
            "read the parent to obtain its current state."
        )
    parent_data = parent.model_dump(mode="json")
    parent_data["latest_version"] = max(parent.latest_version, version.version)
    phases: dict[str, Any] = {
        "parent": {"completed": True, "id": str(parent.id)},
        "version": {
            "completed": True,
            "id": str(version.id),
            "version": version.version,
        },
    }
    if blob is not None:
        phases["blob"] = {"completed": True, "id": str(blob.id)}
    return CommandResult(
        item={
            kind: parent_data,
            "version": version.model_dump(mode="json"),
            "phases": phases,
        },
        warnings=warnings,
    )
