# Kitaru plugins

Each adapter, importer, and exporter under `packages/` is an independently versioned Python distribution. The built-in evaluators share the `kitaru-evaluator` distribution and are released together.

Kitaru keeps the default catalog in `src/kitaru/server/api/bootstrap.py`. At startup, the server records each exact distribution requirement and `module:callable` entrypoint without installing or importing the plugin package.

## Packages

| Directory | Distribution | Contents |
|---|---|---|
| `braintrust-importer` | `kitaru-braintrust-importer` | Braintrust importer |
| `evaluator` | `kitaru-evaluator` | All built-in evaluators |
| `harbor-exporter` | `kitaru-harbor-exporter` | Harbor experiment exporter |
| `jsonl-importer` | `kitaru-jsonl-importer` | Kitaru JSONL importer |
| `langfuse-importer` | `kitaru-langfuse-importer` | Langfuse importer |
| `langgraph` | `kitaru-langgraph` | LangGraph recording and replay adapter |
| `langsmith-importer` | `kitaru-langsmith-importer` | LangSmith importer |
| `openai-agents` | `kitaru-openai-agents` | OpenAI Agents SDK recording adapter |
| `pydantic-ai` | `kitaru-pydantic-ai` | PydanticAI recording and replay adapter |
| `verifiers-exporter` | `kitaru-verifiers-exporter` | Verifiers experiment exporter |

## Development and releases

Read [DEVELOPMENT.md](DEVELOPMENT.md) before you change, test, or publish a plugin package. It contains the local artifact test, candidate-image Compose rehearsal, manual registration example, clean worker setup, version bump procedure, dry-run workflow, and PyPI publishing procedure.

Manual workflow dispatches build and test without publishing. A package tag publishes only when the tagged commit is contained in `main`.

`candidate.Dockerfile` and `docker-compose.candidate.yml` are tracked development infrastructure. Files generated under `candidate-wheels/` are local artifacts and must not be committed. Production release Dockerfiles install Kitaru from PyPI and do not install plugin distributions.

Run the main package gate from the repository root:

```bash
just plugin-artifact-smoke
```

`default-requirements.txt` pins every independently released plugin distribution that is bundled with Kitaru. The server catalog remains limited to importers and evaluators; adapter and exporter distributions are installed directly in the environment that uses them and are not registered as server plugins. A provider-specific release updates one importer pin. An evaluator release updates the shared `kitaru-evaluator` pin.
