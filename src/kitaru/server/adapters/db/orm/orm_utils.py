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
"""Deterministic naming helpers for ORM tables."""

# Postgres truncates identifiers to 63 bytes.
_MAX_IDENTIFIER_LENGTH = 63


def index_name(table: str, columns: list[str]) -> str:
    """Build the name for an index.

    Args:
        table: Table the index belongs to.
        columns: Columns the index covers, in order.

    Returns:
        Index name of the form ``ix_<table>_<columns>``.
    """
    return f"ix_{table}_{'_'.join(columns)}"[:_MAX_IDENTIFIER_LENGTH]


def unique_constraint_name(table: str, columns: list[str]) -> str:
    """Build the name for a unique constraint.

    Args:
        table: Table the constraint belongs to.
        columns: Columns the constraint spans, in order.

    Returns:
        Constraint name of the form ``uq_<table>_<columns>``.
    """
    return f"uq_{table}_{'_'.join(columns)}"[:_MAX_IDENTIFIER_LENGTH]


def foreign_key_name(table: str, columns: list[str]) -> str:
    """Build the name for a foreign key constraint.

    Args:
        table: Table the constraint belongs to.
        columns: Columns the constraint spans, in order.

    Returns:
        Constraint name of the form ``fk_<table>_<columns>``.
    """
    return f"fk_{table}_{'_'.join(columns)}"[:_MAX_IDENTIFIER_LENGTH]


def check_constraint_name(table: str, columns: list[str]) -> str:
    """Build the name for a check constraint.

    Args:
        table: Table the constraint belongs to.
        columns: Columns the constraint references, in order.

    Returns:
        Constraint name of the form ``ck_<table>_<columns>``.
    """
    return f"ck_{table}_{'_'.join(columns)}"[:_MAX_IDENTIFIER_LENGTH]
