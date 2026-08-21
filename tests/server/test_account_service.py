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
"""Tests for account use cases."""

import uuid
from typing import Any

import pytest

from conftest import FakeAccountRepository, FakePasswordHasher
from kitaru.analytics.events import FINISHED_ONBOARDING_SURVEY_KEY, AnalyticsEvent
from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.adapters.permissions.admin_flag import AdminFlagPermissionProvider
from kitaru.server.application.models.account import AccountFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import (
    Account,
    AccountNotFound,
    DuplicateAccountName,
)
from kitaru.server.domain.base import ForbiddenError
from kitaru.server.domain.keys import hash_secret
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="admin", is_admin=True))
NON_ADMIN_ACTOR = AuthContext(
    account=Account(id=uuid.uuid4(), name="alice", is_admin=False)
)


class _RecordingAnalytics(ServerAnalytics):
    """Analytics tracker recording identify calls instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self.identified: list[tuple[uuid.UUID, dict[str, Any]]] = []
        self.tracked: list[tuple[uuid.UUID, str, dict[str, Any]]] = []

    def identify(
        self, user_id: uuid.UUID, traits: dict[str, Any] | None = None
    ) -> None:
        """Record an identify call instead of buffering it.

        Args:
            user_id: User id.
            traits: User traits.
        """
        self.identified.append((user_id, traits or {}))

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
        self.tracked.append((user_id, event, properties or {}))


@pytest.fixture
def service() -> AccountService:
    """Provide an account service backed by fakes."""
    return AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
    )


@pytest.fixture
def service_and_repository() -> tuple[AccountService, FakeAccountRepository]:
    """Provide an account service and its backing repository."""
    repository = FakeAccountRepository()
    service = AccountService(
        repository=repository,
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
    )
    return service, repository


async def test_create_user(service: AccountService) -> None:
    """Create an account with all fields."""
    account, _ = await service.create_user(
        name="alice",
        email="alice@example.com",
        password="secret",
        is_admin=False,
        actor=ACTOR,
    )
    assert account.name == "alice"
    assert account.email == "alice@example.com"
    assert account.password_hash == "hashed:secret"
    assert account.active is True
    assert account.is_service_account is False
    assert account.created is not None
    assert account.updated is not None


async def test_create_user_without_password_pends_activation(
    service: AccountService,
) -> None:
    """Start a password-less account inactive with an activation token."""
    account, activation_token = await service.create_user(
        name="alice",
        email=None,
        password=None,
        is_admin=False,
        actor=ACTOR,
    )
    assert account.password_hash is None
    assert account.active is False
    assert activation_token is not None
    assert account.activation_token_hash == hash_secret(activation_token)


async def test_activate_user(service: AccountService) -> None:
    """Activate a pending account and clear its token."""
    created, activation_token = await service.create_user(
        name="alice",
        email=None,
        password=None,
        is_admin=False,
        actor=ACTOR,
    )
    assert activation_token is not None
    activated = await service.activate_user(
        created.id, activation_token=activation_token, password="secret"
    )
    assert activated.active is True
    assert activated.password_hash == "hashed:secret"
    assert activated.activation_token_hash is None


async def test_activate_user_wrong_token(service: AccountService) -> None:
    """Reject activation with a token that does not match."""
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password=None,
        is_admin=False,
        actor=ACTOR,
    )
    with pytest.raises(ForbiddenError):
        await service.activate_user(
            created.id, activation_token="wrong", password="secret"
        )


async def test_activate_user_without_pending_token(
    service: AccountService,
) -> None:
    """Reject activation of an account that has no pending token."""
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password="secret",
        is_admin=False,
        actor=ACTOR,
    )
    with pytest.raises(ForbiddenError):
        await service.activate_user(
            created.id, activation_token="anything", password="new"
        )


async def test_activate_user_forbidden_for_service_account(
    service_and_repository: tuple[AccountService, FakeAccountRepository],
) -> None:
    """Reject activation of a service account."""
    service, repository = service_and_repository
    created = await repository.create(Account(name="svc", is_service_account=True))
    with pytest.raises(AccountNotFound):
        await service.activate_user(
            created.id, activation_token="anything", password="new"
        )


async def test_deactivate_user_mints_activation_token(
    service: AccountService,
) -> None:
    """Mint a fresh activation token when an account is deactivated."""
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password="secret",
        is_admin=False,
        actor=ACTOR,
    )
    updated, activation_token = await service.deactivate_user(created.id, actor=ACTOR)
    assert updated.active is False
    assert updated.activation_token_hash == hash_secret(activation_token)


async def test_deactivate_user_forbidden_for_service_account(
    service_and_repository: tuple[AccountService, FakeAccountRepository],
) -> None:
    """Reject deactivation of a service account."""
    service, repository = service_and_repository
    created = await repository.create(Account(name="svc", is_service_account=True))
    with pytest.raises(AccountNotFound):
        await service.deactivate_user(created.id, actor=ACTOR)


async def test_create_user_hashes_password(service: AccountService) -> None:
    """Store the hash of a given password, never the plaintext."""
    account, _ = await service.create_user(
        name="alice",
        email=None,
        password="secret",
        is_admin=False,
        actor=ACTOR,
    )
    assert account.password_hash == "hashed:secret"


async def test_create_user_duplicate_name(service: AccountService) -> None:
    """Reject a second account with the same name."""
    await service.create_user(
        name="alice",
        email=None,
        password=None,
        is_admin=False,
        actor=ACTOR,
    )
    with pytest.raises(
        DuplicateAccountName, match="Account name 'alice' is already registered"
    ):
        await service.create_user(
            name="alice",
            email=None,
            password=None,
            is_admin=False,
            actor=ACTOR,
        )


async def test_get_account(service: AccountService) -> None:
    """Load a stored account by id."""
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password=None,
        is_admin=False,
        actor=ACTOR,
    )
    loaded = await service.get_account(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_account_not_found(service: AccountService) -> None:
    """Raise for an unknown account id."""
    missing_id = uuid.uuid4()
    with pytest.raises(AccountNotFound, match=f"Account {missing_id} was not found"):
        await service.get_account(missing_id, actor=ACTOR)


async def test_list_accounts(service: AccountService) -> None:
    """List accounts newest-first with filters."""
    for name in ["alice", "bob", "carol"]:
        await service.create_user(
            name=name,
            email=None,
            password=None,
            is_admin=False,
            actor=ACTOR,
        )

    accounts, next_cursor = await service.list_accounts(AccountFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [account.name for account in accounts] == ["carol", "bob", "alice"]

    accounts, next_cursor = await service.list_accounts(
        AccountFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="bob")
        ),
        actor=ACTOR,
    )
    assert next_cursor is None
    assert accounts[0].name == "bob"


async def test_list_accounts_walks_pages(service: AccountService) -> None:
    """Walk every page of accounts via next_cursor."""
    for name in ["alice", "bob", "carol"]:
        await service.create_user(
            name=name,
            email=None,
            password=None,
            is_admin=False,
            actor=ACTOR,
        )

    collected: list[str] = []
    cursor = None
    while True:
        accounts, next_cursor = await service.list_accounts(
            AccountFilter(cursor=cursor, size=2), actor=ACTOR
        )
        collected.extend(account.name for account in accounts)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == ["carol", "bob", "alice"]


async def test_deactivate_then_activate_user(service: AccountService) -> None:
    """Deactivate an account and bring it back with its fresh token."""
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password="secret",
        is_admin=False,
        actor=ACTOR,
    )
    deactivated, activation_token = await service.deactivate_user(
        created.id, actor=ACTOR
    )
    assert deactivated.active is False
    assert deactivated.updated is not None
    assert created.updated is not None
    assert deactivated.updated > created.updated
    activated = await service.activate_user(
        created.id, activation_token=activation_token, password="new"
    )
    assert activated.active is True


async def test_update_user_password(service: AccountService) -> None:
    """Replace the stored password hash."""
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password="old",
        is_admin=False,
        actor=ACTOR,
    )
    updated = await service.update_user(
        created.id,
        password="new",
        old_password="old",
        metadata=None,
        is_admin=None,
        actor=AuthContext(account=created),
    )
    assert updated.password_hash == "hashed:new"
    assert updated.active is True


async def test_update_user_not_found(service: AccountService) -> None:
    """Raise for an unknown account id."""
    with pytest.raises(AccountNotFound):
        await service.update_user(
            uuid.uuid4(),
            password=None,
            old_password=None,
            metadata=None,
            is_admin=None,
            actor=ACTOR,
        )


async def test_update_user_forbidden_for_service_account(
    service_and_repository: tuple[AccountService, FakeAccountRepository],
) -> None:
    """Reject updating a service account through update_user."""
    service, repository = service_and_repository
    created = await repository.create(Account(name="svc", is_service_account=True))
    with pytest.raises(AccountNotFound):
        await service.update_user(
            created.id,
            password=None,
            old_password=None,
            metadata={"team": "platform"},
            is_admin=None,
            actor=ACTOR,
        )


async def test_update_user_metadata(service: AccountService) -> None:
    """Replace account metadata whole."""
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password=None,
        is_admin=False,
        actor=ACTOR,
    )
    assert created.metadata == {}
    updated = await service.update_user(
        created.id,
        password=None,
        old_password=None,
        metadata={"theme": "dark"},
        is_admin=None,
        actor=AuthContext(account=created),
    )
    assert updated.metadata == {"theme": "dark"}
    updated = await service.update_user(
        created.id,
        password=None,
        old_password=None,
        metadata={"locale": "de"},
        is_admin=None,
        actor=AuthContext(account=created),
    )
    assert updated.metadata == {"locale": "de"}


async def test_update_user_metadata_forbidden_for_other_account(
    service: AccountService,
) -> None:
    """Reject a metadata update targeting another account."""
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password=None,
        is_admin=False,
        actor=ACTOR,
    )
    with pytest.raises(ForbiddenError):
        await service.update_user(
            created.id,
            password=None,
            old_password=None,
            metadata={"theme": "dark"},
            is_admin=None,
            actor=ACTOR,
        )


async def test_update_user_password_without_old_password(
    service: AccountService,
) -> None:
    """Reject a password change that omits the current password."""
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password="old",
        is_admin=False,
        actor=ACTOR,
    )
    with pytest.raises(ForbiddenError):
        await service.update_user(
            created.id,
            password="new",
            old_password=None,
            metadata=None,
            is_admin=None,
            actor=AuthContext(account=created),
        )


async def test_update_user_password_wrong_old_password(
    service: AccountService,
) -> None:
    """Reject a password change whose current password does not match."""
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password="old",
        is_admin=False,
        actor=ACTOR,
    )
    with pytest.raises(ForbiddenError):
        await service.update_user(
            created.id,
            password="new",
            old_password="wrong",
            metadata=None,
            is_admin=None,
            actor=AuthContext(account=created),
        )


async def test_update_user_password_without_stored_password(
    service: AccountService,
) -> None:
    """Reject a password change on an account that has no password set."""
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password=None,
        is_admin=False,
        actor=ACTOR,
    )
    with pytest.raises(ForbiddenError):
        await service.update_user(
            created.id,
            password="new",
            old_password="anything",
            metadata=None,
            is_admin=None,
            actor=AuthContext(account=created),
        )


async def test_create_user_forbidden_for_non_admin(service: AccountService) -> None:
    """Reject account creation by a non-admin actor."""
    with pytest.raises(ForbiddenError):
        await service.create_user(
            name="alice",
            email=None,
            password=None,
            is_admin=False,
            actor=NON_ADMIN_ACTOR,
        )


async def test_create_user_with_is_admin_true(service: AccountService) -> None:
    """Create an account with admin rights."""
    account, _ = await service.create_user(
        name="alice",
        email=None,
        password="secret",
        is_admin=True,
        actor=ACTOR,
    )
    assert account.is_admin is True


async def test_create_service_account(service: AccountService) -> None:
    """Create a service account with no password or activation token."""
    account = await service.create_service_account(
        name="svc",
        email=None,
        actor=ACTOR,
    )
    assert account.is_service_account is True
    assert account.active is True
    assert account.password_hash is None
    assert account.activation_token_hash is None


async def test_create_service_account_forbidden_for_non_admin(
    service: AccountService,
) -> None:
    """Reject service account creation by a non-admin actor."""
    with pytest.raises(ForbiddenError):
        await service.create_service_account(
            name="svc",
            email=None,
            actor=NON_ADMIN_ACTOR,
        )


async def test_deactivate_user_forbidden_for_non_admin(
    service: AccountService,
) -> None:
    """Reject account deactivation by a non-admin actor."""
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password="secret",
        is_admin=False,
        actor=ACTOR,
    )
    with pytest.raises(ForbiddenError):
        await service.deactivate_user(created.id, actor=NON_ADMIN_ACTOR)


async def test_update_user_is_admin_forbidden_for_non_admin(
    service: AccountService,
) -> None:
    """Reject setting the admin flag by a non-admin actor."""
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password=None,
        is_admin=False,
        actor=ACTOR,
    )
    with pytest.raises(ForbiddenError):
        await service.update_user(
            created.id,
            password=None,
            old_password=None,
            metadata=None,
            is_admin=True,
            actor=NON_ADMIN_ACTOR,
        )


async def test_update_user_is_admin_promote_and_demote(
    service: AccountService,
) -> None:
    """Set and clear the admin flag on an account."""
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password=None,
        is_admin=False,
        actor=ACTOR,
    )
    promoted = await service.update_user(
        created.id,
        password=None,
        old_password=None,
        metadata=None,
        is_admin=True,
        actor=ACTOR,
    )
    assert promoted.is_admin is True
    demoted = await service.update_user(
        created.id,
        password=None,
        old_password=None,
        metadata=None,
        is_admin=False,
        actor=ACTOR,
    )
    assert demoted.is_admin is False


async def test_update_service_account_not_found_for_regular_account(
    service_and_repository: tuple[AccountService, FakeAccountRepository],
) -> None:
    """Reject update_service_account targeting a regular account."""
    service, repository = service_and_repository
    created = await repository.create(Account(name="alice"))
    with pytest.raises(AccountNotFound):
        await service.update_service_account(
            created.id,
            metadata={"team": "platform"},
            active=None,
            actor=ACTOR,
        )


async def test_update_service_account_metadata(
    service_and_repository: tuple[AccountService, FakeAccountRepository],
) -> None:
    """Let an admin update a service account's metadata."""
    service, repository = service_and_repository
    created = await repository.create(Account(name="svc", is_service_account=True))
    updated = await service.update_service_account(
        created.id,
        metadata={"team": "platform"},
        active=None,
        actor=ACTOR,
    )
    assert updated.metadata == {"team": "platform"}


async def test_update_service_account_active(
    service_and_repository: tuple[AccountService, FakeAccountRepository],
) -> None:
    """Deactivate and reactivate a service account."""
    service, repository = service_and_repository
    created = await repository.create(Account(name="svc", is_service_account=True))
    deactivated = await service.update_service_account(
        created.id,
        metadata=None,
        active=False,
        actor=ACTOR,
    )
    assert deactivated.active is False
    activated = await service.update_service_account(
        created.id,
        metadata=None,
        active=True,
        actor=ACTOR,
    )
    assert activated.active is True


async def test_update_service_account_forbidden_for_non_admin(
    service_and_repository: tuple[AccountService, FakeAccountRepository],
) -> None:
    """Reject a non-admin updating a service account."""
    service, repository = service_and_repository
    created = await repository.create(Account(name="svc", is_service_account=True))
    with pytest.raises(ForbiddenError):
        await service.update_service_account(
            created.id,
            metadata={"team": "platform"},
            active=None,
            actor=NON_ADMIN_ACTOR,
        )


async def test_ensure_account_creates_default_admin(service: AccountService) -> None:
    """Create the default account as an admin."""
    account = await service.ensure_account("admin", "secret")
    assert account.is_admin is True


async def test_ensure_account_promotes_existing_non_admin(
    service_and_repository: tuple[AccountService, FakeAccountRepository],
) -> None:
    """Promote an existing non-admin account of the same name."""
    service, repository = service_and_repository
    await repository.create(Account(name="admin", is_admin=False))
    account = await service.ensure_account("admin", "secret")
    assert account.is_admin is True


async def test_ensure_account_reactivates_a_deactivated_account(
    service_and_repository: tuple[AccountService, FakeAccountRepository],
) -> None:
    """Reactivate an existing deactivated account of the same name."""
    service, repository = service_and_repository
    await repository.create(
        Account(
            name="admin",
            is_admin=True,
            active=False,
            activation_token_hash="hashed-token",
        )
    )
    account = await service.ensure_account("admin", "secret")
    assert account.active is True
    assert account.activation_token_hash is None


async def test_ensure_account_identifies_the_account() -> None:
    """Identify the ensured account with the bootstrap origin."""
    analytics = _RecordingAnalytics()
    service = AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
        analytics=analytics,
    )

    account = await service.ensure_account("admin", "secret")

    assert len(analytics.identified) == 1
    user_id, traits = analytics.identified[0]
    assert user_id == account.id
    assert traits == {"is_service_account": False, "account_origin": "bootstrap"}
    assert analytics.tracked == [
        (
            account.id,
            AnalyticsEvent.ACCOUNT_CREATED,
            {"account_origin": "bootstrap", "is_service_account": False},
        )
    ]


async def test_ensure_account_does_not_identify_an_existing_account() -> None:
    """Leave the default account unidentified when it already exists."""
    analytics = _RecordingAnalytics()
    repository = FakeAccountRepository()
    await repository.create(Account(name="admin", is_admin=True))
    service = AccountService(
        repository=repository,
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
        analytics=analytics,
    )

    await service.ensure_account("admin", "secret")

    assert analytics.identified == []
    assert analytics.tracked == []


async def test_create_user_identifies_the_account() -> None:
    """Identify a created account with its email and origin."""
    analytics = _RecordingAnalytics()
    service = AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
        analytics=analytics,
    )

    account, _ = await service.create_user(
        name="alice",
        email="alice@example.com",
        password="secret",
        is_admin=False,
        actor=ACTOR,
    )

    assert len(analytics.identified) == 1
    user_id, traits = analytics.identified[0]
    assert user_id == account.id
    assert traits == {
        "is_service_account": False,
        "account_origin": "api",
        "email": "alice@example.com",
    }
    assert analytics.tracked == [
        (
            account.id,
            AnalyticsEvent.ACCOUNT_CREATED,
            {"account_origin": "api", "is_service_account": False},
        )
    ]


async def test_create_user_without_email_omits_the_trait() -> None:
    """Leave the email trait out when the account has no email."""
    analytics = _RecordingAnalytics()
    service = AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
        analytics=analytics,
    )

    account, _ = await service.create_user(
        name="alice",
        email=None,
        password=None,
        is_admin=True,
        actor=ACTOR,
    )

    assert len(analytics.identified) == 1
    user_id, traits = analytics.identified[0]
    assert user_id == account.id
    assert traits == {"is_service_account": False, "account_origin": "api"}


async def test_create_user_without_analytics_tracker(
    service: AccountService,
) -> None:
    """Create an account normally when no analytics tracker is configured."""
    account, _ = await service.create_user(
        name="alice",
        email=None,
        password="secret",
        is_admin=False,
        actor=ACTOR,
    )
    assert account.name == "alice"


async def test_update_user_tracks_the_finished_survey() -> None:
    """Track the user enriched event when the survey is first finished."""
    analytics = _RecordingAnalytics()
    service = AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
        analytics=analytics,
    )
    created, _ = await service.create_user(
        name="alice",
        email="alice@example.com",
        password=None,
        is_admin=False,
        actor=ACTOR,
    )

    await service.update_user(
        created.id,
        password=None,
        old_password=None,
        metadata={FINISHED_ONBOARDING_SURVEY_KEY: True, "usage_reason": "work"},
        is_admin=None,
        actor=AuthContext(account=created),
    )

    enriched = [
        entry for entry in analytics.tracked if entry[1] == AnalyticsEvent.USER_ENRICHED
    ]
    assert len(enriched) == 1
    user_id, _, properties = enriched[0]
    assert user_id == created.id
    assert properties == {
        FINISHED_ONBOARDING_SURVEY_KEY: True,
        "usage_reason": "work",
        "name": "alice",
        "email": "alice@example.com",
    }


async def test_update_user_tracks_the_finished_survey_once() -> None:
    """Skip the user enriched event when the survey was already finished."""
    analytics = _RecordingAnalytics()
    service = AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
        analytics=analytics,
    )
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password=None,
        is_admin=False,
        actor=ACTOR,
    )

    for _ in range(2):
        await service.update_user(
            created.id,
            password=None,
            old_password=None,
            metadata={FINISHED_ONBOARDING_SURVEY_KEY: True},
            is_admin=None,
            actor=AuthContext(account=created),
        )

    enriched = [
        entry for entry in analytics.tracked if entry[1] == AnalyticsEvent.USER_ENRICHED
    ]
    assert len(enriched) == 1


async def test_update_user_without_finished_survey_skips_the_event() -> None:
    """Skip the user enriched event when the survey key is not set."""
    analytics = _RecordingAnalytics()
    service = AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
        analytics=analytics,
    )
    created, _ = await service.create_user(
        name="alice",
        email=None,
        password=None,
        is_admin=False,
        actor=ACTOR,
    )

    await service.update_user(
        created.id,
        password=None,
        old_password=None,
        metadata={"theme": "dark"},
        is_admin=None,
        actor=AuthContext(account=created),
    )

    enriched = [
        entry for entry in analytics.tracked if entry[1] == AnalyticsEvent.USER_ENRICHED
    ]
    assert enriched == []
