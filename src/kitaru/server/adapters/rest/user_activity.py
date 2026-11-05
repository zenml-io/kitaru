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
"""Marker excluding endpoints from counting as user activity."""

from collections.abc import Callable
from typing import Any, TypeVar

_NOT_USER_ACTIVITY_ENDPOINT_ATTR = "_kitaru_not_user_activity"

F = TypeVar("F", bound=Callable[..., Any])


def not_user_activity(endpoint: F) -> F:
    """Exclude an endpoint from counting as user activity.

    Marks a route an account credential reaches on behalf of a machine, such
    as a worker renewing its token, which says nothing about a person using
    the server.

    Args:
        endpoint: Route handler function.

    Returns:
        The endpoint, unchanged.
    """
    setattr(endpoint, _NOT_USER_ACTIVITY_ENDPOINT_ATTR, True)
    return endpoint


def is_not_user_activity(endpoint: Callable[..., Any]) -> bool:
    """Whether a route handler carries the not-user-activity marker.

    Args:
        endpoint: Route handler to check.

    Returns:
        Whether ``endpoint`` was decorated with ``not_user_activity``.
    """
    return getattr(endpoint, _NOT_USER_ACTIVITY_ENDPOINT_ATTR, False)
