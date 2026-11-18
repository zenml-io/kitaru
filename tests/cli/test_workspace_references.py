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
"""Reference rewriting for records pushed between servers."""

import uuid
from datetime import UTC, datetime

from kitaru.api_models.v1.insight import (
    CategoricalInsightData,
    CategoryValue,
    InsightResponse,
    TextInsightData,
)
from kitaru.cli.workspace_references import (
    PUSH_METADATA_KEY,
    ReferenceMap,
    build_pushed_insight,
    find_uuids,
    get_pushed_insight_source_id,
    rewrite_references,
)

SOURCE_URL = "http://localhost:8000"
DESTINATION_URL = "https://acme.cloud.example.com"


def _build_references() -> ReferenceMap:
    """Build an empty reference map between the two test servers."""
    return ReferenceMap(SOURCE_URL, DESTINATION_URL)


def _build_insight(**overrides: object) -> InsightResponse:
    """Build a source insight with overridable fields."""
    now = datetime.now(UTC)
    fields: dict[str, object] = {
        "id": uuid.uuid4(),
        "created": now,
        "updated": now,
        "owner_id": uuid.uuid4(),
        "agent_id": uuid.uuid4(),
        "name": "tool-retries",
        "title": "Tool retries",
        "description": None,
        "data": TextInsightData(content="Retries cluster in one tool."),
        "metadata": {},
    }
    fields.update(overrides)
    return InsightResponse.model_validate(fields)


def test_rewrite_replaces_ids_in_values_keys_and_prose() -> None:
    """Mapped ids are replaced wherever they are written."""
    references = _build_references()
    source_session, destination_session = uuid.uuid4(), uuid.uuid4()
    source_node, destination_node = uuid.uuid4(), uuid.uuid4()
    references.add(source_session, destination_session)
    references.add(source_node, destination_node)
    unmapped: dict[str, str] = {}

    rewritten = rewrite_references(
        {
            "contributing_session_ids": [str(source_session)],
            "evidence": [
                {"session_id": str(source_session), "node_id": str(source_node)}
            ],
            "by_session": {str(source_session): 3},
            "prompt": f'Finding data: {{"session":"{source_session}"}} on '
            f"{SOURCE_URL}, node {str(source_node).upper()}.",
            "position": 2,
            "recommended": True,
        },
        references,
        unmapped,
    )

    assert rewritten == {
        "contributing_session_ids": [str(destination_session)],
        "evidence": [
            {
                "session_id": str(destination_session),
                "node_id": str(destination_node),
            }
        ],
        "by_session": {str(destination_session): 3},
        "prompt": f'Finding data: {{"session":"{destination_session}"}} on '
        f"{DESTINATION_URL}, node {destination_node}.",
        "position": 2,
        "recommended": True,
    }
    assert unmapped == {}


def test_rewrite_keeps_and_reports_ids_without_a_destination_copy() -> None:
    """An id that cannot be mapped stays in place and is reported by kind."""
    references = _build_references()
    import_id, stray_id = uuid.uuid4(), uuid.uuid4()
    references.add_unmapped(import_id, "import")
    unmapped: dict[str, str] = {}

    value = {"source_import": {"import_id": str(import_id)}, "other": str(stray_id)}
    rewritten = rewrite_references(value, references, unmapped)

    assert rewritten == value
    assert unmapped == {str(import_id): "import", str(stray_id): "unknown"}


def test_reference_map_prefers_a_mapping_over_an_unmapped_note() -> None:
    """A record noted as unmapped and copied later resolves to its copy."""
    references = _build_references()
    source_id, destination_id = uuid.uuid4(), uuid.uuid4()

    references.add_unmapped(source_id, "session")
    references.add(source_id, destination_id)
    references.add_unmapped(source_id, "session")

    assert references.get(source_id) == destination_id
    assert references.unmapped == {}
    assert references.get(uuid.uuid4()) is None


def test_find_uuids_ignores_longer_hex_runs() -> None:
    """Only standalone UUIDs count as references."""
    reference = uuid.uuid4()

    found = find_uuids(
        {"a": [f"see {reference}"], str(reference).upper(): f"x{reference}-ffff"}
    )

    assert found == {str(reference)}


def test_build_pushed_insight_rewrites_and_marks_the_copy() -> None:
    """The copy points at destination records and records its source."""
    references = _build_references()
    source_session, destination_session = uuid.uuid4(), uuid.uuid4()
    references.add(source_session, destination_session)
    import_id, analyzer_version_id = uuid.uuid4(), uuid.uuid4()
    references.add_unmapped(import_id, "import")
    insight = _build_insight(
        import_id=import_id,
        analyzer_version_id=analyzer_version_id,
        analyzer_params={"min_count": 5},
        title=f"Session {source_session} retries",
        description=f"Open {SOURCE_URL}",
        data=TextInsightData(content=f"See {source_session}."),
        metadata={
            "kitaru.insights/v1": {
                "contributing_session_ids": [str(source_session)],
                "context": {"source_import": {"import_id": str(import_id)}},
            },
            PUSH_METADATA_KEY: {"source_insight_id": "stale"},
        },
    )

    pushed, unmapped = build_pushed_insight(insight, references)

    assert pushed.name == "tool-retries"
    assert pushed.title == f"Session {destination_session} retries"
    assert pushed.description == f"Open {DESTINATION_URL}"
    assert pushed.data == TextInsightData(content=f"See {destination_session}.")
    assert pushed.metadata["kitaru.insights/v1"] == {
        "contributing_session_ids": [str(destination_session)],
        "context": {"source_import": {"import_id": str(import_id)}},
    }
    assert unmapped == {str(import_id): "import"}
    assert pushed.metadata[PUSH_METADATA_KEY] == {
        "source_insight_id": str(insight.id),
        "source_server": SOURCE_URL,
        "source_import_id": str(import_id),
        "source_analyzer_version_id": str(analyzer_version_id),
        "analyzer_params": {"min_count": 5},
        "unmapped_references": [{"id": str(import_id), "kind": "import"}],
    }
    assert get_pushed_insight_source_id(pushed.metadata) == str(insight.id)


def test_build_pushed_insight_keeps_chart_data_unchanged() -> None:
    """Chart data carries no references and is copied as it is."""
    data = CategoricalInsightData(values=[CategoryValue(label="ok", value=3)])
    insight = _build_insight(data=data)

    pushed, unmapped = build_pushed_insight(insight, _build_references())

    assert pushed.data == data
    assert unmapped == {}
    assert pushed.metadata == {
        PUSH_METADATA_KEY: {
            "source_insight_id": str(insight.id),
            "source_server": SOURCE_URL,
        }
    }


def test_get_pushed_insight_source_id_ignores_foreign_metadata() -> None:
    """Metadata without a well-formed marker names no source insight."""
    assert get_pushed_insight_source_id({}) is None
    assert get_pushed_insight_source_id({PUSH_METADATA_KEY: "x"}) is None
    assert get_pushed_insight_source_id({PUSH_METADATA_KEY: {"other": 1}}) is None
