"""Logfire importer plugin for Kitaru."""

from kitaru_logfire_importer.importer import parse

from .adapter import LogfireAdapter

__all__ = ["LogfireAdapter", "parse"]
