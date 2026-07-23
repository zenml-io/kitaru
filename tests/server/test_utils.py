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
"""Tests for the shared server helpers."""

from datetime import UTC, datetime, timedelta, timezone

from kitaru.server.utils import to_tz_aware


def test_to_tz_aware_naive() -> None:
    """Treat a naive datetime as UTC."""
    value = datetime(2026, 7, 23, 12, 0, 0)
    result = to_tz_aware(value)
    assert result.tzinfo == UTC
    assert result.timestamp() == value.replace(tzinfo=UTC).timestamp()


def test_to_tz_aware_other_timezone() -> None:
    """Convert an aware datetime to UTC without shifting the instant."""
    value = datetime(2026, 7, 23, 14, 0, 0, tzinfo=timezone(timedelta(hours=2)))
    result = to_tz_aware(value)
    assert result.tzinfo == UTC
    assert result == value
    assert result.hour == 12


def test_to_tz_aware_utc_passthrough() -> None:
    """Return a UTC datetime unchanged."""
    value = datetime(2026, 7, 23, 12, 0, 0, tzinfo=UTC)
    assert to_tz_aware(value) == value
