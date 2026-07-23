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
"""Server configuration via environment variables."""

from functools import lru_cache
from typing import Self

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings shared by every Kitaru server process."""

    model_config = SettingsConfigDict(
        env_prefix="KITARU_SERVER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    LOG_LEVEL: str = "INFO"
    SKIP_DB_MIGRATION: bool = False

    REPLAY_HEARTBEAT_TIMEOUT_SECONDS: int = 60
    REPLAY_MAX_ATTEMPTS: int = 3

    DB_HOST: str | None = None
    DB_PORT: int = 5432
    DB_USER: str = "postgres"
    DB_PWD: str = "password"
    DB_NAME: str = "kitaru"
    DATABASE_URL: str | None = None

    @model_validator(mode="after")
    def validate_database_settings(self) -> Self:
        """Validate database connection settings.

        Raises:
            ValueError: Neither KITARU_SERVER_DB_HOST nor
                KITARU_SERVER_DATABASE_URL is set.

        Returns:
            The validated settings object.
        """
        if not self.DB_HOST and not self.DATABASE_URL:
            raise ValueError("Set KITARU_SERVER_DB_HOST or KITARU_SERVER_DATABASE_URL")
        return self


@lru_cache
def get_settings() -> Settings:
    """Return the cached settings singleton for this process.

    Returns:
        Process-wide settings.
    """
    return Settings()
