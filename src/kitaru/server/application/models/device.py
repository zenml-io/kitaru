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
"""Device filter, fingerprint, and policy models."""

import uuid
from collections.abc import Mapping
from typing import ClassVar

from kitaru.server.base import FrozenModel, ListFilter
from kitaru.server.domain.device import DeviceStatus
from kitaru.server.filtering import EQUALITY_OPS, FilterField


class DeviceFilter(ListFilter):
    """Device list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "status": FilterField(value_type=DeviceStatus, ops=EQUALITY_OPS),
    }

    account_id: uuid.UUID | None = None


class DeviceFingerprint(FrozenModel):
    """Device fingerprint."""

    hostname: str | None = None
    os: str | None = None
    ip_address: str | None = None
    python_version: str | None = None
    client_version: str | None = None


class DevicePolicy(FrozenModel):
    """Device authorization policy."""

    auth_timeout_seconds: int = 300
    polling_interval_seconds: int = 5
    max_failed_attempts: int = 3
    # None keeps the device usable until an account deletes it.
    expiration_minutes: int | None = None
    trusted_expiration_minutes: int | None = None
