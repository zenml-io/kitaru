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
"""Tests for the domain event dispatcher."""

import uuid

from kitaru.server.application.events import EventDispatcher, JobsSettled
from kitaru.server.domain.job import Job, JobKind, JobStatus


async def test_dispatch_runs_handlers_in_registration_order() -> None:
    """Run two handlers registered for the same event in registration order."""
    dispatcher = EventDispatcher()
    calls = []

    async def first(event: JobsSettled) -> None:
        calls.append(("first", event.jobs[0].id))

    async def second(event: JobsSettled) -> None:
        calls.append(("second", event.jobs[0].id))

    dispatcher.register(JobsSettled, first)
    dispatcher.register(JobsSettled, second)
    job = Job(
        owner_id=uuid.uuid4(), kind=JobKind.SESSION_RUN, status=JobStatus.COMPLETED
    )

    await dispatcher.dispatch(JobsSettled([job]))

    assert calls == [("first", job.id), ("second", job.id)]
