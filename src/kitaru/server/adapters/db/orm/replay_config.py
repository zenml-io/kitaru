"""Replay-configuration ORM table."""

import uuid

from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import Base, UUIDPrimaryKeyMixin
from kitaru.server.adapters.db.orm.orm_utils import foreign_key_name
from kitaru.server.domain.replay_config import (
    EvaluatorConfig,
    ReplayConfig,
    ReplayOverride,
    ToolPolicy,
)

REPLAY_CONFIG_OWNER_FOREIGN_KEY = foreign_key_name("replay_config", ["owner_id"])


class ReplayConfigORM(UUIDPrimaryKeyMixin, Base):
    """Persisted replay configuration table."""

    __tablename__ = "replay_config"
    __table_args__ = (
        ForeignKeyConstraint(
            ["owner_id"],
            ["account.id"],
            name=REPLAY_CONFIG_OWNER_FOREIGN_KEY,
        ),
    )

    owner_id: Mapped[uuid.UUID]
    override: Mapped[dict | None] = mapped_column(JSONB(none_as_null=True))
    tool_policy: Mapped[dict] = mapped_column(JSONB)
    evaluators: Mapped[list[dict]] = mapped_column(JSONB)

    @classmethod
    def from_domain(cls, config: ReplayConfig) -> "ReplayConfigORM":
        """Build a row from a replay configuration."""
        return cls(
            id=config.id,
            owner_id=config.owner_id,
            override=(
                config.override.model_dump(mode="json")
                if config.override is not None
                else None
            ),
            tool_policy=config.tool_policy.model_dump(mode="json"),
            evaluators=[
                evaluator.model_dump(mode="json") for evaluator in config.evaluators
            ],
        )

    def to_domain(self) -> ReplayConfig:
        """Build a replay configuration from this row."""
        return ReplayConfig(
            id=self.id,
            owner_id=self.owner_id,
            override=(
                ReplayOverride.model_validate(self.override)
                if self.override is not None
                else None
            ),
            tool_policy=ToolPolicy.model_validate(self.tool_policy),
            evaluators=[
                EvaluatorConfig.model_validate(evaluator)
                for evaluator in self.evaluators
            ],
        )
