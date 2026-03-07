# 14. CLI Reference

The CLI is the command-line entry point for interacting with Kitaru. It should mirror the SDK and core runtime model rather than invent a separate one.

**Implementation order:** The SDK is built first — the CLI wraps the SDK. The exception is `kitaru login`, which is needed early to unblock everything else.

## Tier 1: basics (build first)

### Auth and connection

```bash
kitaru login https://my-zenml-server.mycompany.com
kitaru status
kitaru info
kitaru logout
```

- `login` stores connection/auth state for the current user (connects to a ZenML server)
- `status` shows the current connection and active stack context
- `info` shows detailed environment information: connection, active stack, project config, SDK version, and server version
- `logout` clears stored auth state

### Stack selection

```bash
kitaru stack list
kitaru stack use local
kitaru stack use prod
kitaru stack current
```

For MVP, stack UX should focus on **selecting** from available stacks, not assembling infra components by hand.

### Providing input to a wait (resume)

This is a **resume** operation — it continues the same execution.

```bash
kitaru executions input kr-a8f3c2 --wait approve_deploy --value true
```

Structured input should also work:

```bash
kitaru executions input kr-a8f3c2 \
  --wait review_draft \
  --value '{"approved": false, "notes": "Revise the intro"}'
```

User-facing CLI should accept a wait name for convenience, but the runtime should still resolve that to an exact wait call instance internally.

Invalid input should fail validation and leave the execution in `waiting`.

### Retry

This is a **same-execution** operation — it does not create a new execution.

```bash
kitaru executions retry kr-a8f3c2
```

### Getting execution details

```bash
kitaru executions get kr-a8f3c2
```

This should show useful summary information such as:

- execution ID
- flow name
- status
- start/end times
- stack
- current wait if any
- recent checkpoints / durable calls

## Tier 2: broader execution management

### Running a flow

For local or ad hoc execution, the CLI should allow invoking a flow by module path.

```bash
kitaru run agent.py:content_pipeline --args '{"topic":"AI safety"}'
```

With an explicit stack:

```bash
kitaru run agent.py:content_pipeline --stack prod --args '{"topic":"AI safety"}'
```

### Listing executions

```bash
kitaru executions list
kitaru executions list --status waiting
kitaru executions list --flow content_pipeline
```

### Replay

Replay creates a **new execution**.

```bash
kitaru executions replay kr-a8f3c2 --from write_draft
```

Replay with an override:

```bash
kitaru executions replay kr-a8f3c2 \
  --from approve \
  --override wait.approve=false
```

Flow inputs can be passed directly:

```bash
kitaru executions replay kr-a8f3c2 \
  --from write_draft \
  --input topic='"New topic"'
```

The exact CLI shape can be refined, but the semantics should remain:

- replay creates a new execution
- the old execution is unchanged
- overrides replace selected reused outcomes

### Cancel

```bash
kitaru executions cancel kr-a8f3c2
```

### Streaming logs

```bash
kitaru executions logs kr-a8f3c2 --follow
```

## Tier 3: stack authoring, artifacts, and config (later)

### Stack creation

Stack creation must expose infrastructure details and credentials that map to ZenML service connectors and components underneath:

```bash
kitaru stack create prod \
    --runner kubernetes \
    --runner-namespace ml-agents \
    --artifact-store s3://my-bucket \
    --artifact-store-role arn:aws:iam::123:role/kitaru \
    --container-registry ghcr.io/myorg
```

The exact flags are not frozen — the principle is that the CLI must surface enough detail to configure real infrastructure, not just accept a runner name.

### Log store configuration

```bash
kitaru log-store set datadog --endpoint https://logs.datadoghq.com --api-key {{ DATADOG_KEY }}
kitaru log-store show
kitaru log-store reset
```

This is a **global setting** — it switches the default log backend for all flows. See chapter 9 for the log store model.

### Artifact browsing

```bash
kitaru artifacts list kr-a8f3c2
kitaru artifacts list kr-a8f3c2 --name raw_notes
kitaru artifacts list kr-a8f3c2 --type context
kitaru artifacts get art_abc123
kitaru artifacts get art_abc123 --download report.pdf
```

### Config inspection

```bash
kitaru config show
```

This should show the resolved view of:

- connection context
- active stack
- project config (from `pyproject.toml` `[tool.kitaru]`)
- relevant defaults for execution

## MVP notes

For the MVP, the CLI should stay focused on the core lifecycle. Tier 1 commands are the priority:

- login / status / info / logout
- stack list / use / current
- executions input / retry / get

Tier 2 extends to broader execution management. Tier 3 (stack authoring, artifacts, config) can come later.

Anything beyond the core lifecycle should be clearly marked as later or admin-only rather than mixed into the main SDK story.
