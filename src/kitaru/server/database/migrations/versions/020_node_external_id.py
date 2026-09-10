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
"""Node external id Alembic revision.

Revision ID: 020_node_external_id
Revises: 019_import_max_sessions
Create Date: 2026-09-10

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "020_node_external_id"
down_revision = "019_import_max_sessions"
branch_labels = None
depends_on = None

POSITION_INDEX = "ix_session_node_session_id_started_at_id"
SESSION_ID_INDEX_UNIQUE_CONSTRAINT = "uq_session_node_session_id_index"


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    # Give every node an external id before the column carries the identity
    # the index used to carry.
    op.execute(
        sa.text("""
        UPDATE session_node
        SET external_id = 'index-' || "index"
        WHERE external_id IS NULL
    """)
    )

    with op.batch_alter_table("session_node", schema=None) as batch_op:
        batch_op.add_column(sa.Column("parent_external_id", sa.Text(), nullable=True))
        batch_op.add_column(
            sa.Column(
                "secondary_parent_external_ids",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'[]'::jsonb"),
            )
        )
        # Existing rows take the empty list, new rows always carry the column.
        batch_op.alter_column("secondary_parent_external_ids", server_default=None)

    # Restate the links the id columns carry as the references the source
    # sent, which are resolved back to ids when a session is read.
    op.execute(
        sa.text("""
        UPDATE session_node AS n
        SET parent_external_id = p.external_id
        FROM session_node AS p
        WHERE p.id = n.parent_id
    """)
    )

    op.execute(
        sa.text("""
        UPDATE session_node AS n
        SET secondary_parent_external_ids = coalesce(
            (
                SELECT jsonb_agg(p.external_id ORDER BY secondary.ordinality)
                FROM jsonb_array_elements_text(n.secondary_parent_ids)
                    WITH ORDINALITY AS secondary(parent_id, ordinality)
                JOIN session_node AS p ON p.id = secondary.parent_id::uuid
            ),
            '[]'::jsonb
        )
        WHERE jsonb_array_length(n.secondary_parent_ids) > 0
    """)
    )

    # Nodes are positioned by their start time, so a node that reported none
    # takes its parent's and keeps none when the parent has none either.
    op.execute(
        sa.text("""
        UPDATE session_node AS n
        SET started_at = p.started_at
        FROM session_node AS p
        WHERE p.id = n.parent_id
            AND n.started_at IS NULL
            AND p.started_at IS NOT NULL
    """)
    )

    with op.batch_alter_table("session_node", schema=None) as batch_op:
        batch_op.alter_column("external_id", existing_type=sa.Text(), nullable=False)
        batch_op.create_index(
            POSITION_INDEX,
            ["session_id", "started_at", "id"],
            unique=False,
        )
        batch_op.drop_constraint(SESSION_ID_INDEX_UNIQUE_CONSTRAINT, type_="unique")
        batch_op.drop_column("index")
        batch_op.drop_column("parent_id")
        batch_op.drop_column("secondary_parent_ids")


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    with op.batch_alter_table("session_node", schema=None) as batch_op:
        batch_op.add_column(sa.Column("index", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("parent_id", sa.Uuid(), nullable=True))
        batch_op.add_column(
            sa.Column(
                "secondary_parent_ids",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'[]'::jsonb"),
            )
        )
        batch_op.alter_column("secondary_parent_ids", server_default=None)

    # Resolve the stored references back into the id columns.
    op.execute(
        sa.text("""
        UPDATE session_node AS n
        SET parent_id = p.id
        FROM session_node AS p
        WHERE p.session_id = n.session_id
            AND p.external_id = n.parent_external_id
    """)
    )

    op.execute(
        sa.text("""
        UPDATE session_node AS n
        SET secondary_parent_ids = coalesce(
            (
                SELECT jsonb_agg(p.id::text ORDER BY secondary.ordinality)
                FROM jsonb_array_elements_text(n.secondary_parent_external_ids)
                    WITH ORDINALITY AS secondary(external_id, ordinality)
                JOIN session_node AS p
                    ON p.session_id = n.session_id
                    AND p.external_id = secondary.external_id
            ),
            '[]'::jsonb
        )
        WHERE jsonb_array_length(n.secondary_parent_external_ids) > 0
    """)
    )

    op.execute(
        sa.text("""
        UPDATE session_node AS n
        SET "index" = ranked.position
        FROM (
            SELECT
                id,
                row_number() OVER (
                    PARTITION BY session_id
                    ORDER BY started_at ASC NULLS LAST, id ASC
                ) - 1 AS position
            FROM session_node
        ) AS ranked
        WHERE ranked.id = n.id
    """)
    )

    with op.batch_alter_table("session_node", schema=None) as batch_op:
        batch_op.alter_column("index", existing_type=sa.Integer(), nullable=False)
        batch_op.create_unique_constraint(
            SESSION_ID_INDEX_UNIQUE_CONSTRAINT, ["session_id", "index"]
        )
        batch_op.alter_column("external_id", existing_type=sa.Text(), nullable=True)
        batch_op.drop_index(POSITION_INDEX)
        batch_op.drop_column("secondary_parent_external_ids")
        batch_op.drop_column("parent_external_id")
