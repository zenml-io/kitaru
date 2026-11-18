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
"""Rewriting of record references carried from one Kitaru server to another."""

import re
import uuid
from dataclasses import dataclass, field
from typing import Any

from kitaru.api_models.v1.insight import (
    InsightInput,
    InsightResponse,
    TextInsightData,
)

PUSH_METADATA_KEY = "kitaru.push/v1"
UNKNOWN_REFERENCE_KIND = "unknown"

_UUID_PATTERN = re.compile(
    r"(?<![0-9a-fA-F-])"
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
    r"(?![0-9a-fA-F-])"
)


@dataclass(slots=True)
class ReferenceMap:
    """Source record ids and the destination records that replace them."""

    source_url: str
    destination_url: str
    mapped: dict[str, str] = field(default_factory=dict)
    """Source id to the id of its destination copy."""
    unmapped: dict[str, str] = field(default_factory=dict)
    """Source id without a destination copy to the kind of record it names."""

    def add(self, source_id: uuid.UUID, destination_id: uuid.UUID) -> None:
        """Record the destination copy of a source record."""
        key = str(source_id)
        self.mapped[key] = str(destination_id)
        self.unmapped.pop(key, None)

    def add_unmapped(self, source_id: uuid.UUID, kind: str) -> None:
        """Record a source record that has no destination copy."""
        key = str(source_id)
        if key not in self.mapped:
            self.unmapped[key] = kind

    def get(self, source_id: uuid.UUID) -> uuid.UUID | None:
        """Return the destination copy of a source record, if any."""
        destination_id = self.mapped.get(str(source_id))
        return None if destination_id is None else uuid.UUID(destination_id)


def find_uuids(value: Any) -> set[str]:
    """Collect every UUID written anywhere in a JSON value, lowercased."""
    found: set[str] = set()
    if isinstance(value, str):
        found.update(match.lower() for match in _UUID_PATTERN.findall(value))
    elif isinstance(value, dict):
        for key, item in value.items():
            found |= find_uuids(key)
            found |= find_uuids(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            found |= find_uuids(item)
    return found


def rewrite_references(
    value: Any, references: ReferenceMap, unmapped: dict[str, str]
) -> Any:
    """Replace source ids and the source URL anywhere in a JSON value.

    Ids are matched inside longer strings too, because generated prompts
    embed them in prose and serialized JSON.

    Args:
        value: JSON value to rewrite.
        references: Known source ids and their destination copies.
        unmapped: Collects each id left in place, with the kind of source
            record it names or ``unknown``.

    Returns:
        Rewritten copy of the value.
    """
    if isinstance(value, str):
        return _rewrite_string(value, references, unmapped)
    if isinstance(value, dict):
        return {
            (
                _rewrite_string(key, references, unmapped)
                if isinstance(key, str)
                else key
            ): rewrite_references(item, references, unmapped)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [rewrite_references(item, references, unmapped) for item in value]
    return value


def build_pushed_insight(
    insight: InsightResponse, references: ReferenceMap
) -> tuple[InsightInput, dict[str, str]]:
    """Build the destination copy of a source insight.

    Fields the insight create request cannot set, and every reference left
    pointing at a source record, are recorded under ``PUSH_METADATA_KEY``.

    Args:
        insight: Source insight.
        references: Known source ids and their destination copies.

    Returns:
        Insight to create and the references left unmapped, by id.
    """
    unmapped: dict[str, str] = {}
    metadata = {
        key: rewrite_references(item, references, unmapped)
        for key, item in insight.metadata.items()
        if key != PUSH_METADATA_KEY
    }
    data = insight.data
    if isinstance(data, TextInsightData):
        data = TextInsightData(
            content=_rewrite_string(data.content, references, unmapped)
        )
    description = insight.description
    if description is not None:
        description = _rewrite_string(description, references, unmapped)
    marker: dict[str, Any] = {
        "source_insight_id": str(insight.id),
        "source_server": references.source_url,
    }
    if insight.import_id is not None:
        marker["source_import_id"] = str(insight.import_id)
    if insight.analyzer_version_id is not None:
        marker["source_analyzer_version_id"] = str(insight.analyzer_version_id)
    if insight.analyzer_params is not None:
        marker["analyzer_params"] = insight.analyzer_params
    if unmapped:
        marker["unmapped_references"] = [
            {"id": reference, "kind": kind}
            for reference, kind in sorted(unmapped.items())
        ]
    metadata[PUSH_METADATA_KEY] = marker
    return (
        InsightInput(
            name=insight.name,
            title=_rewrite_string(insight.title, references, unmapped),
            description=description,
            data=data,
            metadata=metadata,
        ),
        unmapped,
    )


def get_pushed_insight_source_id(metadata: dict[str, Any]) -> str | None:
    """Return the source insight id a pushed insight records, if any."""
    marker = metadata.get(PUSH_METADATA_KEY)
    if not isinstance(marker, dict):
        return None
    source_id = marker.get("source_insight_id")
    return source_id if isinstance(source_id, str) else None


def _rewrite_string(
    value: str, references: ReferenceMap, unmapped: dict[str, str]
) -> str:
    """Replace the source URL and every known source id in one string."""

    def replace(match: re.Match[str]) -> str:
        found = match.group(0)
        key = found.lower()
        destination_id = references.mapped.get(key)
        if destination_id is not None:
            return destination_id
        unmapped[key] = references.unmapped.get(key, UNKNOWN_REFERENCE_KIND)
        return found

    if references.source_url != references.destination_url:
        value = value.replace(references.source_url, references.destination_url)
    return _UUID_PATTERN.sub(replace, value)
