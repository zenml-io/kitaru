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
"""Permission models."""

import uuid
from enum import StrEnum


class ResourceType(StrEnum):
    """Resource type."""

    ACCOUNT = "account"


class Action(StrEnum):
    """Action."""

    CREATE = "create"
    DEACTIVATE = "deactivate"
    SET_ADMIN = "set_admin"


class AllIds:
    """Sentinel for every id being allowed."""


# Sentinel meaning every id is allowed.
ALL_IDS = AllIds()

AllowedIds = frozenset[uuid.UUID] | AllIds
