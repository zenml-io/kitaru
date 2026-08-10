# Kitaru plugins

Each adapter and importer under `packages/` is an independently versioned Python distribution. The built-in evaluators share the `kitaru-evaluator` distribution and are released together.

Kitaru keeps the default catalog in `src/kitaru/server/api/bootstrap.py`. At startup, the server records each exact distribution requirement and `module:callable` entrypoint without installing or importing the plugin package.

## Packages

| Directory | Distribution | Contents |
|---|---|---|
| `braintrust-importer` | `kitaru-braintrust-importer` | Braintrust importer |
| `evaluator` | `kitaru-evaluator` | All built-in evaluators |
| `jsonl-importer` | `kitaru-jsonl-importer` | Kitaru JSONL importer |
| `langfuse-importer` | `kitaru-langfuse-importer` | Langfuse importer |
| `langsmith-importer` | `kitaru-langsmith-importer` | LangSmith importer |
| `openai-agents` | `kitaru-openai-agents` | OpenAI Agents SDK recording adapter |
| `opentelemetry-importer` | `kitaru-opentelemetry-importer` | OpenTelemetry importer |
| `pydantic-ai` | `kitaru-pydantic-ai` | PydanticAI recording and replay adapter |

## Development and releases

Read [DEVELOPMENT.md](DEVELOPMENT.md) before you change, test, or publish a plugin package. It contains the local artifact test, candidate-image Compose rehearsal, manual registration example, clean worker setup, version bump procedure, dry-run workflow, and PyPI publishing procedure.

Manual workflow dispatches build and test without publishing. A package tag publishes only when the tagged commit is contained in `main`.

`candidate.Dockerfile` and `docker-compose.candidate.yml` are tracked development infrastructure. Files generated under `candidate-wheels/` are local artifacts and must not be committed. Production release Dockerfiles install Kitaru from PyPI and do not install plugin distributions.

Run the main package gate from the repository root:

```bash
just plugin-artifact-smoke
```

`default-requirements.txt` mirrors the exact package versions in Kitaru's default catalog. Adapter distributions are installed by agent projects and are not included in that file. A provider-specific release updates one importer pin. An evaluator release updates the shared `kitaru-evaluator` pin.
