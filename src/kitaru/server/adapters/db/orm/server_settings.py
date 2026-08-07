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
"""Server settings ORM table."""

import uuid

from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import Base, TimestampMixin


class ServerSettingsORM(TimestampMixin, Base):
    """Server settings table."""

    __tablename__ = "server_settings"

    # A constant primary key bounds the table to its single row, so a
    # concurrent first startup resolves through the primary key conflict.
    singleton: Mapped[bool] = mapped_column(primary_key=True, default=True)
    server_id: Mapped[uuid.UUID]
