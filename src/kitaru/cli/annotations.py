#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Annotation CLI commands."""

import json
import uuid
from typing import Any

from kitaru.api_models.v1.annotation import (
    AnnotationSelector,
    AnnotationUpdateRequest,
    InvestigationAnswerCreateRequest,
    ManualAnnotationCreateRequest,
)
from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.registration import list_params, page_result, parse_json_object


def _parse_json_value(value: str, *, option: str) -> Any:
    """Parse one arbitrary JSON value from a CLI option."""
    try:
        return json.loads(value)
    except json.JSONDecodeError as error:
        raise CLIError(
            "invalid_arguments", f"{option} is not valid JSON: {error}"
        ) from error


async def create_annotation(
    client: Any,
    *,
    value: str,
    session_id: uuid.UUID | None,
    investigation_session_id: uuid.UUID | None,
    selector: str | None,
) -> CommandResult:
    """Create either a manual annotation or an investigation answer."""
    if (session_id is None) == (investigation_session_id is None):
        raise CLIError(
            "invalid_arguments",
            "Select exactly one annotation target: --session or "
            "--investigation-session.",
        )
    parsed_value = _parse_json_value(value, option="--value")
    parsed_selector = (
        AnnotationSelector.model_validate(
            parse_json_object(selector, option="--selector")
        )
        if selector is not None
        else None
    )
    if session_id is not None:
        manual_fields: dict[str, Any] = {
            "session_id": session_id,
            "value": parsed_value,
        }
        if parsed_selector is not None:
            manual_fields["selector"] = parsed_selector
        request = ManualAnnotationCreateRequest(**manual_fields)
    else:
        assert investigation_session_id is not None
        answer_fields: dict[str, Any] = {
            "investigation_session_id": investigation_session_id,
            "value": parsed_value,
        }
        if parsed_selector is not None:
            answer_fields["selector"] = parsed_selector
        request = InvestigationAnswerCreateRequest(**answer_fields)
    annotation = await client.annotations.create(request)
    return CommandResult(item=annotation.model_dump(mode="json"))


async def list_annotations(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> CommandResult:
    """List one server page of annotations."""
    params = list_params(
        "annotation", size=size, cursor=cursor, sort=sort, filter=filter
    )
    return page_result(await client.annotations.list(params), size=size)


async def get_annotation(client: Any, annotation_id: uuid.UUID) -> CommandResult:
    """Get one annotation by UUID."""
    annotation = await client.annotations.get(annotation_id)
    return CommandResult(item=annotation.model_dump(mode="json"))


async def update_annotation(
    client: Any, annotation_id: uuid.UUID, *, value: str
) -> CommandResult:
    """Replace one annotation's JSON value."""
    annotation = await client.annotations.update(
        annotation_id,
        AnnotationUpdateRequest(value=_parse_json_value(value, option="--value")),
    )
    return CommandResult(item=annotation.model_dump(mode="json"))


async def delete_annotation(
    client: Any, annotation_id: uuid.UUID, *, force: bool
) -> CommandResult:
    """Delete one annotation."""
    if not force:
        raise CLIError("invalid_arguments", "Deleting an annotation requires --force.")
    await client.annotations.delete(annotation_id)
    return CommandResult(item={"id": str(annotation_id), "deleted": True})
