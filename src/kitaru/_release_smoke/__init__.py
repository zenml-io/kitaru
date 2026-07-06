"""Private release-smoke helpers that must be importable by module path.

These modules are shipped inside ``src/kitaru`` because remote execution needs
to import Kitaru flows by dotted Python path. They are not part of Kitaru's
public SDK surface.
"""
