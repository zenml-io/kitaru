"""Default plugin catalog entry for the Langfuse importer."""


def get_definitions() -> list[dict[str, str | None]]:
    """Return the Langfuse importer definition."""
    return [
        {
            "kind": "importer",
            "name": "kitaru/langfuse",
            "description": "Import Langfuse JSON and JSONL trace exports.",
            "provider": "langfuse",
            "entrypoint": "kitaru_langfuse_importer.importer:parse",
        }
    ]
