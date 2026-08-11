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
"""Annotation repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.annotation import AnnotationFilter
from kitaru.server.domain.annotation import Annotation


class AnnotationRepository(Protocol):
    """Annotation persistence operations."""

    async def create(self, annotation: Annotation) -> Annotation:
        """Persist a new annotation.

        Args:
            annotation: Annotation to store.

        Returns:
            Stored annotation with timestamps set.
        """
        ...

    async def get(self, annotation_id: uuid.UUID) -> Annotation:
        """Load an annotation by id.

        Args:
            annotation_id: Id of the annotation.

        Raises:
            AnnotationNotFound: No annotation has this id.

        Returns:
            Stored annotation.
        """
        ...

    async def query(
        self, annotation_filter: AnnotationFilter
    ) -> tuple[list[Annotation], str | None]:
        """Query annotations matching a filter.

        Args:
            annotation_filter: Filter and pagination parameters.

        Returns:
            Page of matching annotations and the next cursor.
        """
        ...

    async def update(self, annotation: Annotation) -> Annotation:
        """Persist changes to an existing annotation.

        Args:
            annotation: Annotation with modified fields.

        Raises:
            AnnotationNotFound: No annotation has this id.

        Returns:
            Stored annotation with the updated timestamp renewed.
        """
        ...

    async def delete(self, annotation_id: uuid.UUID) -> None:
        """Delete an annotation by id.

        Args:
            annotation_id: Id of the annotation.

        Raises:
            AnnotationNotFound: No annotation has this id.
        """
        ...
