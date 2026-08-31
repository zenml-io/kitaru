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
"""Hypothesis profiles shared by plugin tests."""

import os

from hypothesis import settings
from hypothesis.database import DirectoryBasedExampleDatabase

_HYPOTHESIS_DB = DirectoryBasedExampleDatabase(".hypothesis/examples")
settings.register_profile("dev", max_examples=100, database=_HYPOTHESIS_DB)
# Hypothesis rejects derandomize=True together with a database (derandomize
# replaces the database-driven search with a fixed pseudo-random seed). PR
# runs need that determinism, so "ci" keeps the database off; only the
# nightly run caches examples for replay across runs.
settings.register_profile(
    "ci", max_examples=50, derandomize=True, deadline=None, database=None
)
settings.register_profile(
    "nightly", max_examples=2000, deadline=None, database=_HYPOTHESIS_DB
)
settings.load_profile(
    os.environ.get("HYPOTHESIS_PROFILE", "ci" if os.environ.get("CI") else "dev")
)
