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
"""Recompute session token totals without treating missing usage as zero.

Revision ID: 024_token_completeness
Revises: 023_insight_task_id_index
Create Date: 2026-10-31 13:28:28.527435

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "024_token_completeness"
down_revision = "023_insight_task_id_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Recompute each token total, retaining unknown usage as null."""
    op.execute(
        sa.text("""
            WITH totals AS (
                SELECT session.id,
                    CASE WHEN COUNT(*) FILTER (
                        WHERE node.node_type = 'llm_call'
                          AND node.input_tokens IS NULL
                    ) > 0 THEN NULL ELSE SUM(node.input_tokens) END AS input_tokens,
                    CASE WHEN COUNT(*) FILTER (
                        WHERE node.node_type = 'llm_call'
                          AND node.output_tokens IS NULL
                    ) > 0 THEN NULL ELSE SUM(node.output_tokens) END AS output_tokens,
                    CASE WHEN COUNT(*) FILTER (
                        WHERE node.node_type = 'llm_call'
                          AND node.cached_input_tokens IS NULL
                    ) > 0 THEN NULL ELSE SUM(node.cached_input_tokens) END
                        AS cached_input_tokens,
                    CASE WHEN COUNT(*) FILTER (
                        WHERE node.node_type = 'llm_call'
                          AND node.reasoning_tokens IS NULL
                    ) > 0 THEN NULL ELSE SUM(node.reasoning_tokens) END
                        AS reasoning_tokens
                FROM session
                LEFT JOIN session_node AS node ON node.session_id = session.id
                GROUP BY session.id
            )
            UPDATE session
            SET input_tokens = totals.input_tokens,
                output_tokens = totals.output_tokens,
                cached_input_tokens = totals.cached_input_tokens,
                reasoning_tokens = totals.reasoning_tokens
            FROM totals
            WHERE session.id = totals.id
        """)
    )


def downgrade() -> None:
    """Retain corrected totals, which remain compatible with the prior schema."""
