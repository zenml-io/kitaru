"""Configuration and connection management.

``kitaru.configure()`` sets project-level runtime defaults.
``kitaru.connect()`` establishes a connection to a Kitaru server
(which is a ZenML server under the hood).

Configuration precedence (highest to lowest):
1. Invocation-time overrides
2. Decorator defaults
3. ``kitaru.configure()``
4. Environment variables
5. ``pyproject.toml`` under ``[tool.kitaru]``
6. Built-in defaults

Example::

    kitaru.configure(cache=False)
    kitaru.connect("https://my-server.example.com")

Note: This is scaffolding. Configuration is not yet implemented.
"""

from __future__ import annotations

from typing import Any

from kitaru.runtime import _not_implemented


def configure(**kwargs: Any) -> None:
    """Set project-level runtime defaults.

    Args:
        **kwargs: Configuration key-value pairs (e.g. ``cache=False``).
    """
    _not_implemented("configure")


def connect(server_url: str, **kwargs: Any) -> None:
    """Connect to a Kitaru server.

    Under the hood, this connects to a ZenML server. The user does
    not need to know this.

    Args:
        server_url: URL of the Kitaru server.
        **kwargs: Additional connection options.
    """
    _not_implemented("connect")
