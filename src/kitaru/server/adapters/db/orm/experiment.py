"""Experiment ORM table."""

import uuid

from sqlalchemy import ForeignKeyConstraint, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import (
    foreign_key_name,
    unique_constraint_name,
)
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.names import MAX_NAME_LENGTH

EXPERIMENT_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("experiment", ["name"])
EXPERIMENT_OWNER_FOREIGN_KEY = foreign_key_name("experiment", ["owner_id"])
EXPERIMENT_CONFIG_FOREIGN_KEY = foreign_key_name("experiment", ["replay_config_id"])


class ExperimentORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Experiment table."""

    __tablename__ = "experiment"
    __table_args__ = (
        UniqueConstraint("name", name=EXPERIMENT_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=EXPERIMENT_OWNER_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["replay_config_id"],
            ["replay_config.id"],
            name=EXPERIMENT_CONFIG_FOREIGN_KEY,
        ),
    )

    owner_id: Mapped[uuid.UUID]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    description: Mapped[str | None]
    replay_config_id: Mapped[uuid.UUID]

    @classmethod
    def from_domain(cls, experiment: Experiment) -> "ExperimentORM":
        """Build a row from an experiment."""
        return cls(
            id=experiment.id,
            owner_id=experiment.owner_id,
            name=experiment.name,
            description=experiment.description,
            replay_config_id=experiment.replay_config_id,
        )

    def to_domain(self) -> Experiment:
        """Build an experiment from this row."""
        return Experiment(
            id=self.id,
            owner_id=self.owner_id,
            name=self.name,
            description=self.description,
            replay_config_id=self.replay_config_id,
            created=self.created,
            updated=self.updated,
        )
