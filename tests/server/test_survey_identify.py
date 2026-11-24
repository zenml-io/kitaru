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

import pytest

from kitaru.analytics.events import FINISHED_ONBOARDING_SURVEY_KEY, AnalyticsEvent
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
    """Analytics tracker recording calls in order instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self.calls: list[tuple[str, uuid.UUID, Any]] = []

    def identify(
        self, user_id: uuid.UUID, traits: dict[str, Any] | None = None
    ) -> None:
        """Record an identify call instead of buffering it.

        Args:
            user_id: User id.
            traits: User traits.
        """
        self.calls.append(("identify", user_id, traits or {}))

    def track(
        self,
        user_id: uuid.UUID,
        event: AnalyticsEvent | str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Record a track call instead of buffering it.

        Args:
            user_id: User id.
            event: Event name.
            properties: Event properties.
        """
        self.calls.append(("track", user_id, event))

    def get_identified(self) -> list[tuple[uuid.UUID, dict[str, Any]]]:
        """Return the recorded identify calls.

        Returns:
            User id and traits of each identify call, in call order.
        """
        return [
            (user_id, data) for kind, user_id, data in self.calls if kind == "identify"
        ]


async def _create_account(
    email: str | None = None,
) -> tuple[AccountService, _RecordingAnalytics, Account]:
    """Create an account and forget the analytics calls its creation made.

    Args:
        email: Account email.

    Returns:
        Service, its analytics tracker, and the created account.
    """
    analytics = _RecordingAnalytics()
    service = AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
        analytics=analytics,
    )
    created, _ = await service.create_user(
        name="default",
        email=email,
        password=None,
        is_admin=False,
        actor=ACTOR,
    )
    analytics.calls.clear()
    return service, analytics, created


async def _update_metadata(
    service: AccountService, account: Account, metadata: dict[str, Any]
) -> None:
    """Update the metadata of an account as the account itself.

    Args:
        service: Account service.
        account: Account to update.
        metadata: New metadata.
    """
    await service.update_user(
        account.id,
        password=None,
        old_password=None,
        metadata=metadata,
        is_admin=None,
        actor=AuthContext(account=account),
    )


async def test_update_user_identifies_the_finished_survey_once() -> None:
    """Identify the account with its survey answers when the survey is finished."""
    service, analytics, created = await _create_account()
    answers = {
        FINISHED_ONBOARDING_SURVEY_KEY: True,
        "name": "Alice Example",
        "work_email": "alice@example.com",
        "newsletter": True,
        "framework": "pydantic_ai",
        "trace_provider": "langfuse",
    }

    for _ in range(2):
        await _update_metadata(service, created, answers)

    assert analytics.get_identified() == [
        (
            created.id,
            {
                "name": "Alice Example",
                "email": "alice@example.com",
                "newsletter": True,
                "framework": "pydantic_ai",
                "trace_provider": "langfuse",
            },
        )
    ]


async def test_update_user_identifies_before_tracking_the_enrichment() -> None:
    """Send the identify call ahead of the user enriched event."""
    service, analytics, created = await _create_account()

    await _update_metadata(service, created, {FINISHED_ONBOARDING_SURVEY_KEY: True})

    assert [kind for kind, _, _ in analytics.calls] == ["identify", "track"]
    assert analytics.calls[1][2] == AnalyticsEvent.USER_ENRICHED


async def test_update_user_without_finished_survey_skips_the_identify() -> None:
    """Send no identify call for a metadata update outside the survey."""
    service, analytics, created = await _create_account()

    await _update_metadata(service, created, {"work_email": "alice@example.com"})

    assert analytics.calls == []


def test_survey_traits_prefer_the_survey_email() -> None:
    """Use the address typed into the survey over the account email."""
    account = Account(
        name="alice",
        email="alice@example.com",
        metadata={"work_email": "alice@work.test", "newsletter": True},
    )

    assert build_survey_traits(account) == {
        "name": "alice",
        "email": "alice@work.test",
        "newsletter": True,
    }


@pytest.mark.parametrize("work_email", [None, "", "   ", 42])
def test_survey_traits_fall_back_to_the_account_email(work_email: Any) -> None:
    """Use the account email when the survey has no usable address."""
    account = Account(
        name="alice",
        email="alice@example.com",
        metadata={"work_email": work_email, "newsletter": True},
    )

    assert build_survey_traits(account)["email"] == "alice@example.com"


@pytest.mark.parametrize("account_email", [None, "", "   "])
def test_survey_traits_without_any_email(account_email: str | None) -> None:
    """Leave the email trait out when neither the survey nor the account has one."""
    account = Account(
        name="default",
        email=account_email,
        metadata={"work_email": " ", "newsletter": True},
    )

    assert build_survey_traits(account) == {"name": "default", "newsletter": True}


def test_survey_traits_leave_the_email_out_without_consent() -> None:
    """Leave the email trait out when the newsletter was declined."""
    account = Account(
        name="alice",
        email="alice@example.com",
        metadata={"work_email": "alice@work.test", "newsletter": False},
    )

    assert build_survey_traits(account) == {"name": "alice", "newsletter": False}


@pytest.mark.parametrize("newsletter", [None, "true", 1, []])
def test_survey_traits_ignore_a_non_boolean_newsletter(newsletter: Any) -> None:
    """Treat a newsletter answer that is not a boolean as no answer."""
    account = Account(
        name="alice",
        email="alice@example.com",
        metadata={"newsletter": newsletter},
    )

    assert build_survey_traits(account) == {"name": "alice"}


def test_survey_traits_strip_the_answers() -> None:
    """Strip the survey answers and drop the blank ones."""
    account = Account(
        name="default",
        metadata={
            "name": "  Alice Example ",
            "work_email": " alice@work.test ",
            "newsletter": True,
            "framework": " langgraph ",
            "trace_provider": "  ",
        },
    )

    assert build_survey_traits(account) == {
        "name": "Alice Example",
        "email": "alice@work.test",
        "newsletter": True,
        "framework": "langgraph",
    }


@pytest.mark.parametrize("survey_name", [None, "", "   ", 7])
def test_survey_traits_fall_back_to_the_account_name(survey_name: Any) -> None:
    """Use the account name when the survey has no usable name."""
    account = Account(name="alice", metadata={"name": survey_name})

    assert build_survey_traits(account) == {"name": "alice"}
