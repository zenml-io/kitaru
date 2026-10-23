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
"""Repair session totals that omit unpriced model calls.

Revision ID: 022_session_cost_completeness
Revises: 021_node_external_id
Create Date: 2026-10-22 16:38:43.764332

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "022_session_cost_completeness"
down_revision = "021_node_external_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Clear totals that treated an unknown LLM cost as zero."""
    op.execute(
        sa.text("""
            UPDATE session
            SET cost = NULL
            WHERE cost IS NOT NULL AND EXISTS (
                SELECT 1 FROM session_node
                WHERE session_node.session_id = session.id
                  AND session_node.node_type = 'llm_call'
                  AND session_node.cost IS NULL
            )
        """)
    )


def downgrade() -> None:
    """Retain corrected costs, which remain compatible with the prior schema."""
