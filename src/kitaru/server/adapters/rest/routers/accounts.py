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
    AccountActivateRequest,
    AccountActivationTokenResponse,
    AccountCreateRequest,
    AccountListParams,
    AccountResponse,
    AccountUpdateRequest,
)
from kitaru.api_models.v1.base import Page
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_account_service,
    get_app_settings,
    require_local_account_management,
)
from kitaru.server.adapters.rest.mapping.accounts import (
    account_list_params_to_filter,
    account_to_activation_token_response,
    account_to_response,
)
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService

router = APIRouter(route_class=CommitRoute)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_local_account_management)],
)
async def create_account(
    body: AccountCreateRequest,
    service: Annotated[AccountService, Depends(get_account_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AccountResponse | AccountActivationTokenResponse:
    """Create an account.

    An account created without a password starts inactive and its response
    carries the activation token once.

    Clients observe HTTP 201 on success, 403 outside the ``local`` auth
    scheme, 409 when the name is already registered, and 422 on invalid input.

    Args:
        body: Account create request.
        service: Account service.
        actor: Caller context.

    Returns:
        Created account.
    """
    account, activation_token = await service.create_account(
        name=body.name,
        email=body.email,
        password=body.password,
        is_admin=body.is_admin,
        actor=actor,
    )
    if activation_token is not None:
        return account_to_activation_token_response(account, activation_token)
    return account_to_response(account)


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


@router.patch("/{account_id}")
async def update_account(
    account_id: uuid.UUID,
    body: AccountUpdateRequest,
    service: Annotated[AccountService, Depends(get_account_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> AccountResponse:
    """Partially update an account.

    A password write carries the current password in ``old_password``.

    Clients observe HTTP 200 on success, 403 when writing a password or the
    admin flag outside the ``local`` auth scheme, when writing another
    account's password or metadata, when changing the calling account's own
    admin flag, when making a service account an admin, and when the
    supplied current password is missing or wrong, 404 when the account does
    not exist, and 422 on invalid input.

    Args:
        account_id: Id of the account.
        body: Account update request.
        service: Account service.
        actor: Caller context.
        settings: API settings for this process.

    Returns:
        Updated account.
    """
    if body.password is not None or body.is_admin is not None:
        require_local_account_management(settings)
    account = await service.update_account(
        account_id,
        password=body.password,
        old_password=body.old_password,
        metadata=body.metadata,
        is_admin=body.is_admin,
        actor=actor,
    )
    return account_to_response(account)


@router.post(
    "/{account_id}/deactivate",
    dependencies=[Depends(require_local_account_management)],
)
async def deactivate_account(
    account_id: uuid.UUID,
    service: Annotated[AccountService, Depends(get_account_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AccountActivationTokenResponse:
    """Deactivate an account and return its fresh activation token once.

    Clients observe HTTP 200 on success, 403 outside the ``local`` auth scheme
    and when deactivating the calling account, and 404 when the account does
    not exist.

    Args:
        account_id: Id of the account.
        service: Account service.
        actor: Caller context.

    Returns:
        Deactivated account carrying its activation token.
    """
    account, activation_token = await service.deactivate_account(
        account_id, actor=actor
    )
    return account_to_activation_token_response(account, activation_token)


@router.post("/{account_id}/activate")
async def activate_account(
    account_id: uuid.UUID,
    body: AccountActivateRequest,
    service: Annotated[AccountService, Depends(get_account_service)],
) -> AccountResponse:
    """Activate an account with its activation token and a new password.

    The route is unauthenticated, because the account it activates cannot log
    in until it holds a password.

    Clients observe HTTP 200 on success, 403 when the account has no pending
    token or the token does not match, 404 when the account does not exist,
    and 422 on invalid input.

    Args:
        account_id: Id of the account.
        body: Account activate request.
        service: Account service.

    Returns:
        Activated account.
    """
    account = await service.activate_account(
        account_id,
        activation_token=body.activation_token,
        password=body.password,
    )
    return account_to_response(account)
