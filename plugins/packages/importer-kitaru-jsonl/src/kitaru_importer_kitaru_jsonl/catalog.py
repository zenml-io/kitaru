"""Default plugin catalog entry for the Kitaru JSONL importer."""


def get_definitions() -> list[dict[str, str | None]]:
    """Return the Kitaru JSONL importer definition."""
    return [
        {
            "kind": "importer",
            "name": "kitaru/kitaru-jsonl",
            "description": "Import sessions matching the Kitaru JSONL contract.",
            "provider": "kitaru-jsonl",
            "entrypoint": "kitaru_importer_kitaru_jsonl.importer:parse",
        }
    ]
