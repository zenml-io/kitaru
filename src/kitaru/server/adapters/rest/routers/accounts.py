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

from fastapi import APIRouter, Depends, Query

from kitaru.api_models.v1.account import (
    AccountListParams,
    AccountResponse,
)
from kitaru.api_models.v1.base import Page
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_account_service,
)
from kitaru.server.adapters.rest.mapping.accounts import (
    account_list_params_to_filter,
    account_to_response,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService

router = APIRouter(route_class=CommitRoute)


@router.get("")
async def list_accounts(
    service: Annotated[AccountService, Depends(get_account_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[AccountListParams, Query()],
) -> Page[AccountResponse]:
    """List accounts.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Account service.
        actor: Caller context.
        params: Account list params.

    Returns:
        Page of accounts.
    """
    account_filter = account_list_params_to_filter(params)
    accounts, next_cursor = await service.list_accounts(account_filter, actor=actor)
    return Page[AccountResponse](
        items=[account_to_response(account) for account in accounts],
        next_cursor=next_cursor,
    )


@router.get("/me")
async def get_current_account(
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AccountResponse:
    """Get the calling account.

    Clients observe HTTP 200 on success.

    Args:
        actor: Caller context.

    Returns:
        Calling account.
    """
    return account_to_response(actor.account)


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
