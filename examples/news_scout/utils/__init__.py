"""News scout utility re-exports."""

from .dotenv import load_dotenv
from .http import http_get_json, http_get_text

__all__ = ["http_get_json", "http_get_text", "load_dotenv"]
