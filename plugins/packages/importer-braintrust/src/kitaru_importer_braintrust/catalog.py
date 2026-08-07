"""Default plugin catalog entry for the Braintrust importer."""


def get_definitions() -> list[dict[str, str | None]]:
    """Return the Braintrust importer definition."""
    return [
        {
            "kind": "importer",
            "name": "kitaru/braintrust",
            "description": "Import Braintrust project-log and UI exports.",
            "provider": "braintrust",
            "entrypoint": "kitaru_importer_braintrust.importer:parse",
        }
    ]
