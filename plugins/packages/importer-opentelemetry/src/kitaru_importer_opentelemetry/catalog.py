"""Default plugin catalog entry for the OpenTelemetry importer."""


def get_definitions() -> list[dict[str, str | None]]:
    """Return the OpenTelemetry importer definition."""
    return [
        {
            "kind": "importer",
            "name": "kitaru/opentelemetry",
            "description": "Import OpenTelemetry, Arize, and Logfire JSON exports.",
            "provider": "opentelemetry",
            "entrypoint": "kitaru_importer_opentelemetry.importer:parse",
        }
    ]
