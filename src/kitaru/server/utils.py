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
"""Generic helpers shared across server layers."""

from datetime import UTC, datetime, tzinfo


def to_tz_aware(value: datetime, tz: tzinfo = UTC) -> datetime:
    """Normalize a datetime to the given timezone, treating naive values as being in it.

    Args:
        value: Datetime to normalize.
        tz: Target timezone.

    Returns:
        Aware datetime in the target timezone.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=tz)
    return value.astimezone(tz)
