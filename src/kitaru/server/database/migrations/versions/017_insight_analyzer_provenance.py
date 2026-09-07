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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Preserve insight analyzer provenance.

Revision ID: 017_insight_analyzer_provenance
Revises: 016_analyzer
Create Date: 2026-09-07

"""

from alembic import op

from kitaru.server.adapters.db.orm.insight import (
    INSIGHT_ANALYZER_VERSION_ID_FOREIGN_KEY,
)

# revision identifiers, used by Alembic.
revision = "017_insight_analyzer_provenance"
down_revision = "016_analyzer"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("insight", schema=None) as batch_op:
        batch_op.drop_constraint(
            INSIGHT_ANALYZER_VERSION_ID_FOREIGN_KEY, type_="foreignkey"
        )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    op.execute(
        "UPDATE insight SET analyzer_version_id = NULL "
        "WHERE analyzer_version_id IS NOT NULL AND NOT EXISTS ("
        "SELECT 1 FROM plugin_version "
        "WHERE plugin_version.id = insight.analyzer_version_id)"
    )
    with op.batch_alter_table("insight", schema=None) as batch_op:
        batch_op.create_foreign_key(
            INSIGHT_ANALYZER_VERSION_ID_FOREIGN_KEY,
            "plugin_version",
            ["analyzer_version_id"],
            ["id"],
            ondelete="SET NULL",
        )
