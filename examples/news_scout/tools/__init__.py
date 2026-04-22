"""News scout agent tools — re-exports for clean imports."""

from .sources import search_news, search_twitter
from .web import fetch_url, investigate

__all__ = ["fetch_url", "investigate", "search_news", "search_twitter"]
