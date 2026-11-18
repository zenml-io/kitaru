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
"""Tests for the identify call sent when the onboarding survey is finished."""

import uuid
from typing import Any

from kitaru.analytics.events import FINISHED_ONBOARDING_SURVEY_KEY
from kitaru.server.adapters.permissions.admin_flag import AdminFlagPermissionProvider
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.analytics_events import build_survey_traits
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import Account
from kitaru_test_support import FakeAccountRepository, FakePasswordHasher

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="admin", is_admin=True))


class _RecordingAnalytics(ServerAnalytics):
    """Analytics tracker recording identify calls instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self.identified: list[tuple[uuid.UUID, dict[str, Any]]] = []

    def identify(
        self, user_id: uuid.UUID, traits: dict[str, Any] | None = None
    ) -> None:
        """Record an identify call instead of buffering it.

        Args:
            user_id: User id.
            traits: User traits.
        """
        self.identified.append((user_id, traits or {}))

    def track(self, *args: Any, **kwargs: Any) -> None:
        """Drop track calls."""


async def test_update_user_identifies_the_finished_survey_once() -> None:
    """Identify the account with its survey answers when the survey is finished."""
    analytics = _RecordingAnalytics()
    service = AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
        analytics=analytics,
    )
    created, _ = await service.create_user(
        name="default",
        email=None,
        password=None,
        is_admin=False,
        actor=ACTOR,
    )
    analytics.identified.clear()
    answers = {
        FINISHED_ONBOARDING_SURVEY_KEY: True,
        "name": "Alice Example",
        "work_email": "alice@example.com",
        "newsletter": False,
        "framework": "pydantic_ai",
        "trace_provider": "langfuse",
    }

    for _ in range(2):
        await service.update_user(
            created.id,
            password=None,
            old_password=None,
            metadata=answers,
            is_admin=None,
            actor=AuthContext(account=created),
        )

    assert analytics.identified == [
        (
            created.id,
            {
                "name": "Alice Example",
                "email": "alice@example.com",
                "newsletter": False,
                "framework": "pydantic_ai",
                "trace_provider": "langfuse",
            },
        )
    ]


def test_survey_traits_keep_the_account_email() -> None:
    """Keep the account email over the address typed into the survey."""
    account = Account(
        name="alice",
        email="alice@example.com",
        metadata={"work_email": "alice@work.test"},
    )

    assert build_survey_traits(account) == {
        "name": "alice",
        "email": "alice@example.com",
    }


def test_survey_traits_without_any_email() -> None:
    """Leave the email trait out when neither the account nor the survey has one."""
    account = Account(name="default", metadata={"newsletter": False})

    assert build_survey_traits(account) == {"name": "default", "newsletter": False}
