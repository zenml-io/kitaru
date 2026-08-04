# Improve a document agent with the Kitaru CLI

This example has one journey:

1. Generate real PydanticAI traces in Langfuse.
2. Import the traces into Kitaru.
3. Score the imported baselines.
4. Replay them through an improved agent.
5. Compare the baseline and candidate scores.

Run every command from `examples/document_processing`.

## Set up the example

Create `.env`:

```bash
cp .env.example .env
```

Add your OpenAI and Langfuse credentials:

```dotenv
KITARU_API_URL=http://localhost:8000
OPENAI_API_KEY=your-openai-api-key
LANGFUSE_PUBLIC_KEY=your-langfuse-public-key
LANGFUSE_SECRET_KEY=your-langfuse-secret-key
LANGFUSE_BASE_URL=https://cloud.langfuse.com
```

Install the dependencies:

```bash
uv sync --extra cli --extra worker --extra pydantic-ai --extra examples
```

Start Kitaru and PostgreSQL with the repository Docker Compose file:

```bash
docker compose -f ../../docker-compose.yml up -d --build db server
```

The local server listens on `http://localhost:8000`. It accepts local requests
without an API key.

Check the connection:

```bash
uv run --env-file .env kitaru status
```

## 1. Generate real traces

```bash
./generate.sh
```

The script downloads three public NIST PDFs. It runs the baseline PydanticAI
agent once for each PDF. Langfuse captures the model calls. The script exports
the three traces to:

```text
traces/langfuse-traces.jsonl
```

The generated corpus can contain exact results and real extraction errors. The
field-accuracy evaluator measures the result of each run.

## 2. Register the agent and evaluator

Your Kitaru server provides the default Langfuse importer. The import command
will use `langfuse@latest`. You do not need to upload or register importer code.

Test the example-specific evaluator locally:

```bash
uv run --env-file .env kitaru evaluator test \
  evaluator.py \
  --entrypoint evaluate
```

Register the baseline source agent:

```bash
uv run --env-file .env kitaru agent register \
  standards-extractor \
  --entrypoint examples.document_processing.agent:main \
  --description "Extract fields from standards PDFs." \
  --display-version prompt-v2 \
  --working-dir ../.. \
  --timeout-seconds 180
```

Register the field-accuracy evaluator:

```bash
uv run --env-file .env kitaru evaluator register \
  document-field-accuracy \
  --script evaluator.py \
  --entrypoint evaluate \
  --display-version 1.0
```

The agent and evaluator commands create version `1`. If a name exists, select a
new name and use it in the remaining commands.

## 3. Start a worker

Open a second terminal in this directory. Keep the worker active for the rest
of the example:

```bash
uv run --env-file .env kitaru --output jsonl worker start \
  --name document-example-worker \
  --kinds importer \
  --kinds evaluator \
  --kinds agent \
  --concurrency 4 \
  --poll-interval 0.2
```

The worker runs the registered code on your computer. The local Kitaru server
stores the jobs, sessions, evaluations, cohorts, and experiments.

## 4. Import the traces

Return to the first terminal:

```bash
uv run --env-file .env kitaru session import \
  traces/langfuse-traces.jsonl \
  --importer langfuse@latest \
  --agent standards-extractor \
  --params '{"source_instance":"nist-example"}' \
  --media-type application/x-ndjson \
  --wait \
  --interval 0.2 \
  --timeout 300
```

List the imported sessions:

```bash
uv run --env-file .env kitaru session list \
  --agent standards-extractor \
  --origin imported \
  --size 100
```

Copy the three session IDs from the output.

## 5. Score the imported baselines

Replace the placeholders with the imported session IDs:

```bash
uv run --env-file .env kitaru session evaluate \
  SESSION_ID_1 SESSION_ID_2 SESSION_ID_3 \
  --evaluator document-field-accuracy@1 \
  --wait \
  --interval 0.2 \
  --timeout 300
```

Kitaru stores one field-accuracy evaluation for each baseline session. A score
of `1.0` means all four extracted fields match the reviewed record.

## 6. Create a cohort

Create the cohort:

```bash
uv run --env-file .env kitaru cohort create \
  nist-document-baselines \
  --agent standards-extractor \
  --description "Real PydanticAI traces imported from Langfuse."
```

Create an immutable cohort version:

```bash
uv run --env-file .env kitaru cohort version create \
  nist-document-baselines \
  --add-session SESSION_ID_1 \
  --add-session SESSION_ID_2 \
  --add-session SESSION_ID_3 \
  --display-version import-v1
```

Copy the cohort-version ID from the output.

## 7. Create an experiment

```bash
uv run --env-file .env kitaru experiment create \
  improve-document-extraction \
  --evaluator document-field-accuracy@1 \
  --description "Compare prompt-v1 baselines with the prompt-v2 agent."
```

The experiment defines which evaluator Kitaru applies to each replay.

## 8. Run the experiment

Replace `COHORT_VERSION_ID` with the ID from the cohort command:

```bash
uv run --env-file .env kitaru experiment run start \
  improve-document-extraction \
  --cohort-version COHORT_VERSION_ID \
  --agent standards-extractor@1 \
  --evaluate-baselines \
  --wait \
  --interval 0.5 \
  --timeout 1800
```

Kitaru replays each baseline input through the improved agent. It scores the
baseline and candidate sessions with the same evaluator. The final receipt
contains the experiment-run ID and aggregate progress.

## Stop the local server

After the example, stop the containers:

```bash
docker compose -f ../../docker-compose.yml down
```

This command retains the PostgreSQL volume and the Kitaru data.
