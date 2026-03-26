"""Dapr workflow engine backend for Kitaru.

This package is isolated behind the optional ``kitaru[dapr]`` extra.
Importing ``kitaru`` or ``kitaru.engines`` must work without Dapr
dependencies installed. Only code that explicitly needs the Dapr backend
should import from this package.
"""
