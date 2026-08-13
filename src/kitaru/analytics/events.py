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

FINISHED_ONBOARDING_SURVEY_KEY = "finished_onboarding_survey"


class AnalyticsEvent(StrEnum):
    """Analytics event."""

    SESSION_COMPLETED = "Session Completed"
    IMPORT_COMPLETED = "Import Completed"
    REPLAY_CREATED = "Replay Created"
    EVALUATION_COMPLETED = "Evaluation Completed"
    EXPERIMENT_CREATED = "Experiment Created"
    EXPERIMENT_RUN_COMPLETED = "Experiment Run Completed"
    AGENT_CREATED = "Agent Created"
    AGENT_VERSION_CREATED = "Agent Version Created"
    COHORT_CREATED = "Cohort Created"
    COHORT_VERSION_CREATED = "Cohort Version Created"
    INVESTIGATION_CREATED = "Investigation Created"
    ANNOTATION_CREATED = "Annotation Created"
    JOB_COMPLETED = "Job Completed"
    PLUGIN_VERSION_REGISTERED = "Plugin Version Registered"
    USER_ENRICHED = "User Enriched"


class AccountOrigin(StrEnum):
    """Account origin."""

    BOOTSTRAP = "bootstrap"
    API = "api"
    CONTROL_PLANE = "control_plane"
