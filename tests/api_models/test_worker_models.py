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
"""Tests for worker API models."""

import uuid

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import LabelSelector, WorkerClaim, WorkerScope


def test_empty_claims_rejected() -> None:
    """Reject an empty claims list."""
    with pytest.raises(ValidationError):
        WorkerScope(claims=[])


def test_duplicate_claims_rejected() -> None:
    """Reject a claims list that repeats the same claim."""
    with pytest.raises(ValidationError):
        WorkerScope(
            claims=[
                WorkerClaim(kind=TaskKind.EVALUATOR),
                WorkerClaim(kind=TaskKind.EVALUATOR),
            ]
        )


def test_agent_version_on_non_agent_kind_rejected() -> None:
    """Reject an agent_version_id on a non-agent claim."""
    with pytest.raises(ValidationError):
        WorkerClaim(kind=TaskKind.EVALUATOR, agent_version_id=uuid.uuid4())


def test_versioned_agent_claim_alongside_unversioned_rejected() -> None:
    """Reject a versioned agent claim alongside an unversioned agent claim."""
    with pytest.raises(ValidationError):
        WorkerScope(
            claims=[
                WorkerClaim(kind=TaskKind.AGENT),
                WorkerClaim(kind=TaskKind.AGENT, agent_version_id=uuid.uuid4()),
            ]
        )


def test_too_many_claims_rejected() -> None:
    """Reject a claims list exceeding the maximum size."""
    claims = [
        WorkerClaim(kind=TaskKind.AGENT, agent_version_id=uuid.uuid4())
        for _ in range(15)
    ]
    claims.append(WorkerClaim(kind=TaskKind.EVALUATOR))
    claims.append(WorkerClaim(kind=TaskKind.IMPORTER))
    with pytest.raises(ValidationError):
        WorkerScope(claims=claims)


def test_valid_mixed_scope_accepted() -> None:
    """Accept a scope mixing a versioned agent claim with other kinds."""
    agent_version_id = uuid.uuid4()
    scope = WorkerScope(
        claims=[
            WorkerClaim(kind=TaskKind.AGENT, agent_version_id=agent_version_id),
            WorkerClaim(kind=TaskKind.EVALUATOR),
            WorkerClaim(kind=TaskKind.IMPORTER),
        ]
    )
    assert len(scope.claims) == 3


def test_empty_selectors_rejected() -> None:
    """Reject an empty selectors list."""
    with pytest.raises(ValidationError):
        WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)], selectors=[])


def test_duplicate_selector_keys_rejected() -> None:
    """Reject selectors that repeat the same key."""
    with pytest.raises(ValidationError):
        WorkerScope(
            claims=[WorkerClaim(kind=TaskKind.AGENT)],
            selectors=[
                LabelSelector(key="team", values=["a"]),
                LabelSelector(key="team", values=["b"]),
            ],
        )


def test_unique_selector_keys_accepted() -> None:
    """Accept selectors with distinct keys."""
    scope = WorkerScope(
        claims=[WorkerClaim(kind=TaskKind.AGENT)],
        selectors=[
            LabelSelector(key="team", values=["a"]),
            LabelSelector(key="region", values=["b"]),
        ],
    )
    assert scope.selectors is not None
    assert len(scope.selectors) == 2


def test_empty_selector_values_rejected() -> None:
    """Reject a selector with an empty values list."""
    with pytest.raises(ValidationError):
        LabelSelector(key="team", values=[])
