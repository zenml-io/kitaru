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
"""Deploy-time trace importer discovery."""

from importlib.metadata import entry_points

from kitaru.server.adapters.importers.langfuse import LangfuseJSONLImporter
from kitaru.server.application.interfaces.trace_importer import TraceImporter
from kitaru.server.application.models.import_jobs import ImporterDescriptor
from kitaru.server.domain.import_job import ImporterNotFound

ENTRY_POINT_GROUP = "kitaru.trace_importers"


class ImporterRegistry:
    """Registry of built-in and installed trace importers."""

    def __init__(self) -> None:
        """Discover importers once for the process."""
        importers: list[TraceImporter] = [LangfuseJSONLImporter()]
        for entry_point in entry_points(group=ENTRY_POINT_GROUP):
            loaded = entry_point.load()
            importer = loaded() if isinstance(loaded, type) else loaded
            importers.append(importer)
        self._importers = {importer.descriptor.id: importer for importer in importers}
        if len(self._importers) != len(importers):
            raise RuntimeError("Trace importer ids must be unique")

    def list(self) -> list[ImporterDescriptor]:
        """List registered importers.

        Returns:
            Importer descriptions sorted by id.
        """
        return [
            self._importers[importer_id].descriptor
            for importer_id in sorted(self._importers)
        ]

    def get(self, importer_id: str) -> TraceImporter:
        """Resolve an importer by id.

        Args:
            importer_id: Registered importer id.

        Raises:
            ImporterNotFound: No importer has the id.

        Returns:
            Registered importer.
        """
        importer = self._importers.get(importer_id)
        if importer is None:
            raise ImporterNotFound(importer_id)
        return importer
