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
"""Account DTO conversions."""

from kitaru.api_models.v1.accounts import AccountResponse
from kitaru.server.domain.account import Account


def account_to_response(account: Account) -> AccountResponse:
    """Convert an account entity to its response DTO.

    Args:
        account: Stored account.

    Returns:
        Account response.
    """
    assert account.created is not None
    assert account.updated is not None
    return AccountResponse(
        id=account.id,
        name=account.name,
        email=account.email,
        is_service_account=account.is_service_account,
        active=account.active,
        created=account.created,
        updated=account.updated,
    )
