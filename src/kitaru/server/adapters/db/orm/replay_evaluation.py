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
"""Replay evaluation link ORM table."""

import uuid

from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import Base, TimestampMixin
from kitaru.server.adapters.db.orm.orm_utils import foreign_key_name

REPLAY_EVALUATION_REPLAY_ID_FOREIGN_KEY = foreign_key_name(
    "replay_evaluation", ["replay_id"]
)
REPLAY_EVALUATION_EVALUATION_ID_FOREIGN_KEY = foreign_key_name(
    "replay_evaluation", ["evaluation_id"]
)


class ReplayEvaluationORM(TimestampMixin, Base):
    """Replay evaluation link table.

    Repository-managed. No domain model, the evaluations a replay's
    comparison is built from round-trip through this link alone.
    """

    __tablename__ = "replay_evaluation"
    __table_args__ = (
        ForeignKeyConstraint(
            ["replay_id"],
            ["replay.id"],
            name=REPLAY_EVALUATION_REPLAY_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["evaluation_id"],
            ["evaluation.id"],
            name=REPLAY_EVALUATION_EVALUATION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
    )

    replay_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    evaluation_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
