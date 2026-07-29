"""Plugin registry filters and commands."""

import uuid

from kitaru.server.base import FrozenModel, ListFilter
from kitaru.server.domain.plugin import PluginKind


class PluginFilter(ListFilter):
    """Plugin list filter."""

    kind: PluginKind
    name: str | None = None
    provider: str | None = None


class PluginVersionFilter(ListFilter):
    """Plugin version list filter."""

    plugin_id: uuid.UUID


class PluginUpdate(FrozenModel):
    """Partial plugin update."""

    description: str | None = None
    metadata: dict | None = None
