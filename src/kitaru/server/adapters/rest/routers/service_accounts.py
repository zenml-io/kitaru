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
"""Service account routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, status

from kitaru.api_models.v1.account import (
    AccountResponse,
    ServiceAccountCreateRequest,
    ServiceAccountUpdateRequest,
)
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_account_service,
    require_local_account_management,
)
from kitaru.server.adapters.rest.mapping.accounts import account_to_response
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService

router = APIRouter(route_class=CommitRoute)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_local_account_management)],
)
async def create_service_account(
    body: ServiceAccountCreateRequest,
    service: Annotated[AccountService, Depends(get_account_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AccountResponse:
    """Create a service account, active without credentials.

    Clients observe HTTP 201 on success, 403 outside the ``local`` auth
    scheme and when the caller may not create accounts, 409 when the name is
    already registered, and 422 on invalid input.

    Args:
        body: Service account create request.
        service: Account service.
        actor: Caller context.

    Returns:
        Created account.
    """
    account = await service.create_service_account(
        name=body.name, email=body.email, actor=actor
    )
    return account_to_response(account)


@router.patch(
    "/{account_id}",
    dependencies=[Depends(require_local_account_management)],
)
async def update_service_account(
    account_id: uuid.UUID,
    body: ServiceAccountUpdateRequest,
    service: Annotated[AccountService, Depends(get_account_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AccountResponse:
    """Partially update a service account.

    Clients observe HTTP 200 on success, 403 outside the ``local`` auth
    scheme and when the caller may not update service accounts, 404 when the
    service account does not exist, and 422 on invalid input.

    Args:
        account_id: Id of the account.
        body: Service account update request.
        service: Account service.
        actor: Caller context.

    Returns:
        Updated account.
    """
    account = await service.update_service_account(
        account_id,
        metadata=body.metadata,
        active=body.active,
        actor=actor,
    )
    return account_to_response(account)
