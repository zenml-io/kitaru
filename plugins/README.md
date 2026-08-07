# Kitaru plugins

Each importer under `packages/` is an independently versioned Python distribution. The built-in evaluators share the `kitaru-evaluator` distribution and are released together.

Every distribution exposes one catalog through the `kitaru.default_plugins` entry-point group. At startup, Kitaru records the owning distribution and exact version as the package requirement for that plugin.

## Packages

| Directory | Distribution | Contents |
|---|---|---|
| `braintrust-importer` | `kitaru-braintrust-importer` | Braintrust importer |
| `evaluator` | `kitaru-evaluator` | All built-in evaluators |
| `jsonl-importer` | `kitaru-jsonl-importer` | Kitaru JSONL importer |
| `langfuse-importer` | `kitaru-langfuse-importer` | Langfuse importer |
| `langsmith-importer` | `kitaru-langsmith-importer` | LangSmith importer |
| `opentelemetry-importer` | `kitaru-opentelemetry-importer` | OpenTelemetry importer |

## Development and releases

Read [DEVELOPMENT.md](DEVELOPMENT.md) before you change, test, or publish a plugin package. It contains the local artifact test, release-image Compose rehearsal, manual registration example, clean worker setup, version bump procedure, dry-run workflow, and PyPI publishing procedure.

Manual workflow dispatches build and test without publishing. A package tag publishes only when the tagged commit is contained in `main`.

Run the main package gate from the repository root:

```bash
just plugin-artifact-smoke
```

`default-requirements.txt` pins the plugin versions installed in Kitaru server images. A provider-specific release updates one importer pin. An evaluator release updates the shared `kitaru-evaluator` pin.
