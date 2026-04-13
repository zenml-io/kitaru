"""News scout agent tools — re-exports for clean imports."""

from tools.sources import search_news, search_twitter
from tools.web import fetch_url, investigate

__all__ = ["search_news", "search_twitter", "investigate", "fetch_url"]
