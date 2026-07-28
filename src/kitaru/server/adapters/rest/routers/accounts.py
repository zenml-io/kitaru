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
"""Account routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.account import (
    AccountCreateRequest,
    AccountResponse,
    AccountUpdateRequest,
)
from kitaru.api_models.v1.base import Page
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_account_service,
    require_local_account_management,
)
from kitaru.server.adapters.rest.mapping.accounts import account_to_response
from kitaru.server.application.models.account import AccountFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService

router = APIRouter()


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_local_account_management)],
)
async def create_account(
    body: AccountCreateRequest,
    service: Annotated[AccountService, Depends(get_account_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AccountResponse:
    """Create an account.

    Clients observe HTTP 201 on success, 403 outside the ``local`` auth
    scheme, 409 when the name is already registered, and 422 on invalid input.

    Args:
        body: Account create request.
        service: Account service.
        actor: Caller context.

    Returns:
        Created account.
    """
    account = await service.create_account(
        name=body.name,
        email=body.email,
        password=body.password,
        actor=actor,
    )
    return account_to_response(account)


@router.get("")
async def list_accounts(
    service: Annotated[AccountService, Depends(get_account_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    name: str | None = None,
    active: bool | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[AccountResponse]:
    """List accounts.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Account service.
        actor: Caller context.
        name: Filter on account name.
        active: Filter on active state.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of accounts.
    """
    account_filter = AccountFilter(
        name=name, active=active, page=page, page_size=page_size
    )
    accounts, total = await service.list_accounts(account_filter, actor=actor)
    return Page[AccountResponse](
        items=[account_to_response(account) for account in accounts],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{account_id}")
async def get_account(
    account_id: uuid.UUID,
    service: Annotated[AccountService, Depends(get_account_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AccountResponse:
    """Get an account by id.

    Clients observe HTTP 200 on success and 404 when the account does not
    exist.

    Args:
        account_id: Id of the account.
        service: Account service.
        actor: Caller context.

    Returns:
        Stored account.
    """
    account = await service.get_account(account_id, actor=actor)
    return account_to_response(account)


@router.patch(
    "/{account_id}",
    dependencies=[Depends(require_local_account_management)],
)
async def update_account(
    account_id: uuid.UUID,
    body: AccountUpdateRequest,
    service: Annotated[AccountService, Depends(get_account_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AccountResponse:
    """Partially update an account.

    Clients observe HTTP 200 on success, 403 outside the ``local`` auth
    scheme, 404 when the account does not exist, and 422 on invalid input.

    Args:
        account_id: Id of the account.
        body: Account update request.
        service: Account service.
        actor: Caller context.

    Returns:
        Updated account.
    """
    account = await service.update_account(
        account_id,
        active=body.active,
        password=body.password,
        actor=actor,
    )
    return account_to_response(account)
