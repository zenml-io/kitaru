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
"""Trace importer and registry interfaces."""

from typing import Protocol

from kitaru.server.application.models.import_jobs import (
    ImportContext,
    ImporterDescriptor,
    NormalizedImport,
)


class TraceImporter(Protocol):
    """Normalize one supported trace export without performing I/O."""

    @property
    def descriptor(self) -> ImporterDescriptor:
        """Return importer metadata."""
        ...

    def parse(self, content: bytes, context: ImportContext) -> NormalizedImport:
        """Parse an uploaded export into normalized sessions.

        Args:
            content: Complete uploaded file.
            context: User import selections.

        Returns:
            Normalized sessions and isolated source-session errors.
        """
        ...


class TraceImporterRegistry(Protocol):
    """Resolve deploy-time trace importers."""

    def list(self) -> list[ImporterDescriptor]:
        """List registered importers."""
        ...

    def get(self, importer_id: str) -> TraceImporter:
        """Resolve an importer by id."""
        ...
