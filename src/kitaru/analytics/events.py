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
"""Analytics events."""

from enum import StrEnum


class AnalyticsEvent(StrEnum):
    """Analytics event."""

    SESSION_COMPLETED = "session_completed"
    IMPORT_COMPLETED = "import_completed"
    REPLAY_CREATED = "replay_created"
    EVALUATION_COMPLETED = "evaluation_completed"
    EXPERIMENT_CREATED = "experiment_created"
    EXPERIMENT_RUN_COMPLETED = "experiment_run_completed"
    COHORT_CREATED = "cohort_created"
    COHORT_VERSION_CREATED = "cohort_version_created"
    JOB_COMPLETED = "job_completed"
    PLUGIN_REGISTERED = "plugin_registered"
