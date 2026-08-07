"""Default plugin catalog entry for the LangSmith importer."""


def get_definitions() -> list[dict[str, str | None]]:
    """Return the LangSmith importer definition."""
    return [
        {
            "kind": "importer",
            "name": "kitaru/langsmith",
            "description": "Import LangSmith run-query and bulk-export records.",
            "provider": "langsmith",
            "entrypoint": "kitaru_langsmith_importer.importer:parse",
        }
    ]
