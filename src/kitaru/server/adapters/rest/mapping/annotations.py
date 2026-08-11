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
"""Annotation DTO conversions."""

from typing import Any

from kitaru.api_models.v1.annotation import (
    AnnotationListParams,
    AnnotationResponse,
    AnnotationUpdateRequest,
    InvestigationAnswerCreateRequest,
    ManualAnnotationCreateRequest,
)
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.annotation import (
    AnnotationFilter,
    InvestigationAnswerCreate,
    ManualAnnotationCreate,
)
from kitaru.server.domain.annotation import Annotation


def manual_annotation_create_to_command(
    body: ManualAnnotationCreateRequest,
) -> ManualAnnotationCreate:
    """Convert a manual annotation create request to its application command.

    Args:
        body: Manual annotation create request.

    Returns:
        Create command.
    """
    return ManualAnnotationCreate(
        session_id=body.session_id,
        selector=body.selector,
        value=body.value,
    )


def investigation_answer_create_to_command(
    body: InvestigationAnswerCreateRequest,
) -> InvestigationAnswerCreate:
    """Convert an investigation answer create request to its application command.

    Args:
        body: Investigation answer create request.

    Returns:
        Create command.
    """
    return InvestigationAnswerCreate(
        investigation_session_id=body.investigation_session_id,
        selector=body.selector,
        value=body.value,
    )


def annotation_to_response(annotation: Annotation) -> AnnotationResponse:
    """Convert an annotation entity to its response DTO.

    Args:
        annotation: Stored annotation.

    Returns:
        Annotation response.
    """
    assert annotation.created is not None
    assert annotation.updated is not None
    return AnnotationResponse(
        id=annotation.id,
        owner_id=annotation.owner_id,
        session_id=annotation.session_id,
        investigation_session_id=annotation.investigation_session_id,
        selector=annotation.selector,
        value=annotation.value,
        created=annotation.created,
        updated=annotation.updated,
    )


def annotation_list_params_to_filter(params: AnnotationListParams) -> AnnotationFilter:
    """Convert annotation list params to the application filter.

    Args:
        params: Annotation list params.

    Returns:
        Annotation filter.
    """
    return AnnotationFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def annotation_update_to_value(body: AnnotationUpdateRequest) -> Any:
    """Convert an annotation update request to its new domain value.

    Args:
        body: Annotation update request.

    Returns:
        New annotation value.
    """
    return body.value
