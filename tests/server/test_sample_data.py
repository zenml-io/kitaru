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
"""Integrity tests for the sample data."""

from pathlib import Path

import pytest

from kitaru.api_models.v1.session import SessionOrigin
from kitaru.server.application.services import sample_data_seeding
from kitaru.server.application.services.sample_data_seeding import (
    EVALUATOR_ENTRYPOINT,
    load_sample_data,
)


def test_missing_sample_data_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Raise for an installation carrying no sample data file."""
    monkeypatch.setattr(
        sample_data_seeding, "SAMPLE_DATA_PATH", tmp_path / "sample_data.json"
    )
    monkeypatch.setattr(
        sample_data_seeding,
        "COMPRESSED_SAMPLE_DATA_PATH",
        tmp_path / "sample_data.json.gz",
    )
    with pytest.raises(FileNotFoundError, match="Sample data was not found"):
        load_sample_data()


def test_sessions_carry_a_unique_external_id() -> None:
    """Give every sample session an external id to resolve it by."""
    data = load_sample_data()

    external_ids = [item.session.external_id for item in data.sessions]

    assert all(external_id for external_id in external_ids)
    assert len(set(external_ids)) == len(external_ids)


def test_sessions_are_imported() -> None:
    """Record every sample session as an import, which needs no worker."""
    data = load_sample_data()

    assert {item.session.origin for item in data.sessions} == {SessionOrigin.IMPORTED}


def test_nodes_are_ordered_parent_before_child() -> None:
    """Order every node batch so parents resolve within the batch."""
    data = load_sample_data()

    for item in data.sessions:
        seen: set[int] = set()
        for node in item.nodes:
            parents = list(node.secondary_parent_indexes)
            if node.parent_index is not None:
                parents.append(node.parent_index)
            assert all(parent in seen for parent in parents)
            seen.add(node.index)


def test_one_session_fails_an_evaluation() -> None:
    """Give the workspace exactly one failing session to open."""
    data = load_sample_data()

    failing = {
        item.session.external_id
        for item in data.sessions
        for result in item.evaluations
        if result.passed is False
    }

    assert len(failing) == 1


def test_derived_resources_reference_stored_sessions() -> None:
    """Reference only sessions the sample data stores."""
    data = load_sample_data()
    external_ids = {item.session.external_id for item in data.sessions}

    referenced = set(data.cohort.member_external_ids) | {
        item.external_id for item in data.investigation.sessions
    }

    assert referenced <= external_ids


def test_highlights_reference_stored_nodes() -> None:
    """Pin every highlight to a node index the session stores."""
    data = load_sample_data()
    indexes = {
        item.session.external_id: {node.index for node in item.nodes}
        for item in data.sessions
    }

    for item in data.investigation.sessions:
        for question in item.questions:
            assert question.highlights
            for highlight in question.highlights:
                assert highlight.node_index in indexes[item.external_id]


def test_experiment_scores_with_the_registered_evaluator() -> None:
    """Score the experiment with the evaluator the seed registers."""
    data = load_sample_data()

    assert [config.evaluator for config in data.experiment.evaluators] == [
        data.evaluator.name
    ]


def test_evaluator_source_defines_the_entrypoint() -> None:
    """Ship an evaluator script defining the registered entrypoint."""
    data = load_sample_data()

    assert f"def {EVALUATOR_ENTRYPOINT}(" in data.evaluator.source
