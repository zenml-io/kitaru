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
"""Tests for the analytics event context."""

from kitaru.analytics.source import (
    AnalyticsSource,
    analytics_event_context,
    current_event_context,
)


def test_event_context_merges_and_restores() -> None:
    """Merge properties inside the block and restore the context on exit."""
    with analytics_event_context(sample_data=True):
        context = current_event_context.get()
        assert context.source is AnalyticsSource.PYTHON
        assert context.properties == {"sample_data": True}
        with analytics_event_context(retried=True):
            assert current_event_context.get().properties == {
                "sample_data": True,
                "retried": True,
            }
        assert current_event_context.get().properties == {"sample_data": True}
    assert current_event_context.get().properties == {}
