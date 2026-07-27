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
"""Tests for schema naming helpers."""

from kitaru.server.adapters.db.orm.orm_utils import (
    index_name,
    unique_constraint_name,
)


def test_index_name_single_column() -> None:
    """Build an index name from one column."""
    assert index_name("agent", ["name"]) == "ix_agent_name"


def test_index_name_multiple_columns() -> None:
    """Join multiple columns in order."""
    assert index_name("run", ["agent_id", "created"]) == "ix_run_agent_id_created"


def test_unique_constraint_name_single_column() -> None:
    """Build a unique constraint name from one column."""
    assert unique_constraint_name("agent", ["name"]) == "uq_agent_name"


def test_names_truncate_to_identifier_limit() -> None:
    """Cap generated names at the Postgres identifier limit."""
    assert len(unique_constraint_name("a" * 40, ["b" * 40])) == 63
