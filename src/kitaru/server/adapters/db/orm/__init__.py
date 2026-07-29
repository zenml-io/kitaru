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
"""ORM table definitions."""

from kitaru.server.adapters.db.orm.account import AccountORM
from kitaru.server.adapters.db.orm.agent import AgentORM
from kitaru.server.adapters.db.orm.agent_version import AgentVersionORM
from kitaru.server.adapters.db.orm.agent_version_secret import AgentVersionSecretORM
from kitaru.server.adapters.db.orm.api_key import ApiKeyORM
from kitaru.server.adapters.db.orm.blob import BlobORM
from kitaru.server.adapters.db.orm.device import DeviceORM
from kitaru.server.adapters.db.orm.experiment import ExperimentORM, ReplayConfigORM
from kitaru.server.adapters.db.orm.plugin import PluginORM, PluginVersionORM
from kitaru.server.adapters.db.orm.secret import SecretORM
from kitaru.server.adapters.db.orm.tag import TagLinkORM, TagORM
from kitaru.server.adapters.db.orm.worker import WorkerORM

__all__ = [
    "AccountORM",
    "AgentORM",
    "AgentVersionORM",
    "AgentVersionSecretORM",
    "ApiKeyORM",
    "BlobORM",
    "DeviceORM",
    "ExperimentORM",
    "PluginORM",
    "PluginVersionORM",
    "ReplayConfigORM",
    "SecretORM",
    "TagLinkORM",
    "TagORM",
    "WorkerORM",
]
