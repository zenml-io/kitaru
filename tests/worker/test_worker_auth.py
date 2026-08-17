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
"""Tests for the worker registration token source."""

from fastapi import FastAPI

from conftest import (
    UNSCOPED_WORKER_SCOPE,
    FakeAccountRepository,
    FakeApiKeyRepository,
    FakePasswordHasher,
    FakeWorkerRepository,
    asgi_api_client,
    local_settings,
    stub_auth_session,
)
from kitaru.api_models.v1.worker import WorkerCreateRequest, WorkerRuntime
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.rest.dependencies import (
    get_auth_service,
    get_auth_session,
    get_worker_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.account import Account
from kitaru.server.domain.worker import Worker
from kitaru.worker.auth import WorkerTokenSource

RUNTIME = WorkerRuntime(platform="bare")


class _CountingWorkerRepository(FakeWorkerRepository):
    """Fake worker repository counting the registration calls it received."""

    def __init__(self) -> None:
        """Initialize the repository with no recorded calls."""
        super().__init__()
        self.register_calls = 0

    async def register(self, worker: Worker) -> Worker:
        """Record the call and delegate to the fake upsert-by-name behavior."""
        self.register_calls += 1
        return await super().register(worker)


async def _registration_app() -> tuple[FastAPI, _CountingWorkerRepository, str]:
    """Build a fake-backed app plus a bearer token authenticating as its account.

    Returns:
        App registering workers under a real auth service, the repository
        recording registration calls, and a bearer token for the account.
    """
    account_repository = FakeAccountRepository()
    account = await account_repository.create(Account(name="ann"))
    auth_service = AuthService(
        settings=local_settings(),
        account_repository=account_repository,
        api_key_repository=FakeApiKeyRepository(),
        password_hasher=FakePasswordHasher(),
    )
    account_token = auth_service.issue_token(AuthContext(account=account)).token
    repository = _CountingWorkerRepository()
    app = create_app(local_settings())
    app.dependency_overrides[get_worker_service] = lambda: WorkerService(
        repository=repository
    )
    app.dependency_overrides[get_auth_service] = lambda: auth_service
    app.dependency_overrides[get_auth_session] = stub_auth_session
    return app, repository, account_token


def _registration_request(name: str = "worker-1") -> WorkerCreateRequest:
    """Build a worker create request with sane defaults.

    Args:
        name: Worker name.

    Returns:
        Worker create request.
    """
    return WorkerCreateRequest(
        name=name, scope=UNSCOPED_WORKER_SCOPE, runtime=RUNTIME, metadata={}
    )


async def test_initial_token_is_served_without_registering() -> None:
    """Return the token from the initial registration without a new registration."""
    app, repository, account_token = await _registration_app()
    client = asgi_api_client(app, api_key=account_token)
    source = WorkerTokenSource(client, _registration_request(), "initial-token")

    token = source.get_cached_token()

    assert token == "initial-token"
    assert repository.register_calls == 0


async def test_fetch_reregisters_and_caches_the_fresh_token() -> None:
    """Re-register the worker and serve the freshly issued token from the cache."""
    app, repository, account_token = await _registration_app()
    client = asgi_api_client(app, api_key=account_token)
    source = WorkerTokenSource(client, _registration_request(), "initial-token")

    token = await source.fetch_token()

    assert isinstance(token, str) and token != "initial-token"
    assert repository.register_calls == 1
    assert source.get_cached_token() == token


async def test_reregistration_upserts_by_name() -> None:
    """Keep the same worker id when re-registering under the same worker name."""
    app, repository, account_token = await _registration_app()
    client = asgi_api_client(app, api_key=account_token)
    request = _registration_request(name="shared-worker")

    first = await client.workers.create(request)
    second = await client.workers.create(request)

    assert first.worker.id == second.worker.id
    assert repository.register_calls == 2
