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
"""Tests for the control plane authenticator."""

import uuid
from collections.abc import Callable
from importlib.metadata import version
from typing import Any

import httpx
import pytest

from conftest import (
    FakeAccountRepository,
    FakeControlPlaneClient,
    control_plane_settings,
)
from kitaru.server.adapters.auth.control_plane import (
    SERVER_ID_HEADER,
    SERVER_URL_HEADER,
    SERVER_VERSION_HEADER,
    ControlPlaneAuthenticator,
    ControlPlaneClient,
    ControlPlaneError,
    ControlPlaneUser,
)
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import Account

SERVER_ID = uuid.uuid4()


class _RecordingAnalytics(ServerAnalytics):
    """Analytics tracker recording calls instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self.identified: list[tuple[uuid.UUID, dict[str, Any]]] = []
        self.aliased: list[tuple[uuid.UUID, uuid.UUID]] = []

    def identify(
        self, user_id: uuid.UUID, traits: dict[str, Any] | None = None
    ) -> None:
        """Record an identify call instead of buffering it.

        Args:
            user_id: User id.
            traits: User traits.
        """
        self.identified.append((user_id, traits or {}))

    def alias(self, user_id: uuid.UUID, previous_id: uuid.UUID) -> None:
        """Record an alias call instead of buffering it.

        Args:
            user_id: User id the alias points to.
            previous_id: User id the events were recorded under.
        """
        self.aliased.append((user_id, previous_id))


@pytest.fixture
def account_repository() -> FakeAccountRepository:
    """Provide a fake account repository."""
    return FakeAccountRepository()


def control_plane_user(
    username: str | None = "alice",
    email: str | None = "alice@example.com",
    is_service_account: bool = False,
    user_id: uuid.UUID | None = None,
) -> ControlPlaneUser:
    """Build a control plane user.

    Args:
        username: Control plane username.
        email: Control plane email.
        is_service_account: Whether the user is a service account.
        user_id: Control plane user id.

    Returns:
        Control plane user.
    """
    return ControlPlaneUser(
        id=user_id or uuid.uuid4(),
        username=username,
        email=email,
        is_service_account=is_service_account,
    )


def build_authenticator(
    account_repository: FakeAccountRepository,
    user: ControlPlaneUser | None,
    analytics: ServerAnalytics | None = None,
) -> tuple[ControlPlaneAuthenticator, FakeControlPlaneClient]:
    """Build an authenticator wired to a fake control plane client.

    Args:
        account_repository: Fake account repository.
        user: Control plane user returned by the fake client.
        analytics: Analytics tracker, None skips tracking.

    Returns:
        Authenticator under test and the fake client it calls.
    """
    client = FakeControlPlaneClient(user=user)
    authenticator = ControlPlaneAuthenticator(
        client=client,
        account_repository=account_repository,
        server_id=SERVER_ID,
        analytics=analytics,
    )
    return authenticator, client


async def test_authenticate_creates_new_account(
    account_repository: FakeAccountRepository,
) -> None:
    """Create a mirrored account when no account matches the control plane user."""
    user = control_plane_user()
    authenticator, _ = build_authenticator(account_repository, user)

    context = await authenticator.authenticate("credential")

    account = context.account
    assert account.external_id == user.id
    assert account.name == user.username
    assert account.email == user.email
    assert account.is_service_account == user.is_service_account
    assert account.active is True


async def test_authenticate_refreshes_existing_mirrored_account(
    account_repository: FakeAccountRepository,
) -> None:
    """Refresh the name and email on an account already mirroring this user."""
    external_id = uuid.uuid4()
    stored = await account_repository.create(
        Account(external_id=external_id, name="alice", email="old@example.com")
    )
    user = control_plane_user(
        username="alice2", email="new@example.com", user_id=external_id
    )
    authenticator, _ = build_authenticator(account_repository, user)

    context = await authenticator.authenticate("credential")

    assert context.account.id == stored.id
    assert context.account.name == "alice2"
    assert context.account.email == "new@example.com"


class CountingAccountRepository(FakeAccountRepository):
    """Account repository counting the updates it received."""

    def __init__(self) -> None:
        """Initialize the repository with no recorded updates."""
        super().__init__()
        self.updates = 0

    async def update(self, account: Account) -> Account:
        """Record the update and persist the account.

        Args:
            account: Account with modified fields.

        Returns:
            Stored account with the updated timestamp renewed.
        """
        self.updates += 1
        return await super().update(account)


async def test_authenticate_skips_the_write_when_nothing_changed() -> None:
    """Leave a mirrored account untouched when the control plane reports no change."""
    external_id = uuid.uuid4()
    account_repository = CountingAccountRepository()
    await account_repository.create(
        Account(external_id=external_id, name="alice", email="alice@example.com")
    )
    user = control_plane_user(
        username="alice", email="alice@example.com", user_id=external_id
    )
    authenticator, _ = build_authenticator(account_repository, user)

    await authenticator.authenticate("credential")
    await authenticator.authenticate("credential")

    assert account_repository.updates == 0


async def test_authenticate_reactivates_deactivated_account(
    account_repository: FakeAccountRepository,
) -> None:
    """Reactivate a mirrored account that was deactivated locally."""
    external_id = uuid.uuid4()
    stored = await account_repository.create(
        Account(external_id=external_id, name="alice", active=False)
    )
    user = control_plane_user(username="alice", user_id=external_id)
    authenticator, _ = build_authenticator(account_repository, user)

    context = await authenticator.authenticate("credential")

    assert context.account.id == stored.id
    assert context.account.active is True


async def test_authenticate_never_claims_a_local_account(
    account_repository: FakeAccountRepository,
) -> None:
    """Raise instead of turning a same-named local account into an external one."""
    local_account = await account_repository.create(Account(name="alice"))
    user = control_plane_user(username="alice")
    authenticator, _ = build_authenticator(account_repository, user)

    with pytest.raises(ControlPlaneError):
        await authenticator.authenticate("credential")

    stored = await account_repository.get(local_account.id)
    assert stored.external_id is None


async def test_authenticate_keeps_user_and_service_account_namespaces_separate(
    account_repository: FakeAccountRepository,
) -> None:
    """Mirror a service account alongside a same-named local user account."""
    user_account = await account_repository.create(
        Account(name="alice", is_service_account=False)
    )
    user = control_plane_user(username="alice", is_service_account=True)
    authenticator, _ = build_authenticator(account_repository, user)

    context = await authenticator.authenticate("credential")

    assert context.account.id != user_account.id
    assert context.account.is_service_account is True


def _client_routed_to(
    handler: Callable[[httpx.Request], httpx.Response], **overrides: Any
) -> ControlPlaneClient:
    """Build a control plane client whose requests reach a handler.

    Only the transport is replaced, so the client keeps the headers and
    timeouts it configures itself.

    Args:
        handler: Answers each request.
        **overrides: Settings values applied to the client.

    Returns:
        Client wired to a mock transport.
    """
    client = ControlPlaneClient(control_plane_settings(**overrides))
    client._client._transport = httpx.MockTransport(handler)
    return client


def _answer_with_user(request: httpx.Request) -> httpx.Response:
    """Answer any request with a minimal control plane user.

    Args:
        request: Incoming request.

    Returns:
        HTTP response.
    """
    _ = request
    return httpx.Response(200, json={"id": str(uuid.uuid4()), "username": "alice"})


async def test_authorize_user_reads_the_user_from_the_top_level() -> None:
    """Accept the user object the control plane returns without a wrapper."""
    user_id = uuid.uuid4()
    client = _client_routed_to(
        lambda _: httpx.Response(
            200,
            json={
                "id": str(user_id),
                "username": "alice",
                "email": "alice@example.com",
                "is_service_account": False,
                "password": None,
                "is_onboarded": True,
            },
        )
    )

    user = await client.authorize_user("credential", SERVER_ID)
    await client.close()

    assert user.id == user_id
    assert user.username == "alice"
    assert user.email == "alice@example.com"


async def test_authorize_user_describes_this_server_to_the_control_plane() -> None:
    """Send the id, version, and URL the control plane tracks the server by."""
    requests: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _answer_with_user(request)

    client = _client_routed_to(record, SERVER_URL="https://kitaru.example.com")

    await client.authorize_user("credential", SERVER_ID)
    await client.close()

    headers = requests[0].headers
    assert headers[SERVER_ID_HEADER] == str(client._settings.SERVER_ID)
    assert headers[SERVER_VERSION_HEADER] == version("kitaru")
    assert headers[SERVER_URL_HEADER] == "https://kitaru.example.com"


async def test_authorize_user_omits_the_url_header_when_unset() -> None:
    """Leave the URL header off rather than reporting an empty server URL."""
    requests: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _answer_with_user(request)

    client = _client_routed_to(record)

    await client.authorize_user("credential", SERVER_ID)
    await client.close()

    assert SERVER_URL_HEADER not in requests[0].headers


async def test_a_rejected_credential_is_not_retried() -> None:
    """Fail a rejected credential rather than sending it again."""
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(401)

    client = _client_routed_to(handle)

    with pytest.raises(ControlPlaneError, match="HTTP 401"):
        await client.authorize_user("credential", SERVER_ID)
    await client.close()

    assert [r.url.path for r in requests] == ["/users/authorize_server"]


async def test_authorize_user_raises_when_the_response_is_not_a_user() -> None:
    """Raise a control plane error rather than escaping as a validation error."""
    client = _client_routed_to(
        lambda _: httpx.Response(200, json={"detail": "nothing here"})
    )

    with pytest.raises(ControlPlaneError, match="no recognizable user"):
        await client.authorize_user("credential", SERVER_ID)
    await client.close()


async def test_authenticate_raises_when_username_missing(
    account_repository: FakeAccountRepository,
) -> None:
    """Raise when the control plane user has no username to mirror."""
    user = control_plane_user(username=None)
    authenticator, _ = build_authenticator(account_repository, user)

    with pytest.raises(ControlPlaneError, match="no username to mirror"):
        await authenticator.authenticate("credential")


async def test_authenticate_mirrors_an_email_username(
    account_repository: FakeAccountRepository,
) -> None:
    """Mirror an account for an SSO user, whose username is an email address."""
    user = control_plane_user(username="michael@zenml.io", email="michael@zenml.io")
    authenticator, _ = build_authenticator(account_repository, user)

    context = await authenticator.authenticate("credential")

    assert context.account.name == "michael@zenml.io"
    assert context.account.external_id == user.id


async def test_authenticate_raises_when_username_invalid(
    account_repository: FakeAccountRepository,
) -> None:
    """Raise when the control plane username is not a valid account name."""
    user = control_plane_user(username="alice smith")
    authenticator, _ = build_authenticator(account_repository, user)

    with pytest.raises(ControlPlaneError, match="not a valid account name"):
        await authenticator.authenticate("credential")


async def test_authenticate_passes_server_id_to_client(
    account_repository: FakeAccountRepository,
) -> None:
    """Pass the configured server id through to the control plane client."""
    user = control_plane_user()
    authenticator, client = build_authenticator(account_repository, user)

    await authenticator.authenticate("credential")

    assert client.received_server_id == SERVER_ID
    assert client.received_credentials == ["credential"]


async def test_authenticate_new_account_identifies_and_aliases(
    account_repository: FakeAccountRepository,
) -> None:
    """Identify a mirrored account and alias it to the control plane user."""
    analytics = _RecordingAnalytics()
    user = control_plane_user()
    authenticator, _ = build_authenticator(account_repository, user, analytics)

    context = await authenticator.authenticate("credential")

    assert len(analytics.identified) == 1
    user_id, traits = analytics.identified[0]
    assert user_id == context.account.id
    assert traits == {
        "is_service_account": False,
        "source": "control_plane",
        "email": user.email,
    }
    assert analytics.aliased == [(user.id, context.account.id)]


async def test_authenticate_existing_account_does_not_identify(
    account_repository: FakeAccountRepository,
) -> None:
    """Skip identify and alias when the account already mirrors the user."""
    analytics = _RecordingAnalytics()
    external_id = uuid.uuid4()
    await account_repository.create(
        Account(external_id=external_id, name="alice", email="alice@example.com")
    )
    user = control_plane_user(user_id=external_id)
    authenticator, _ = build_authenticator(account_repository, user, analytics)

    await authenticator.authenticate("credential")

    assert analytics.identified == []
    assert analytics.aliased == []


async def test_authenticate_service_account_identifies_the_flag(
    account_repository: FakeAccountRepository,
) -> None:
    """Carry the service account flag into the mirrored account's traits."""
    analytics = _RecordingAnalytics()
    user = control_plane_user(is_service_account=True)
    authenticator, _ = build_authenticator(account_repository, user, analytics)

    await authenticator.authenticate("credential")

    _, traits = analytics.identified[0]
    assert traits["is_service_account"] is True
