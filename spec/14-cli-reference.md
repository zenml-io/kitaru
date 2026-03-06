# 14. CLI Reference

The CLI is the command-line entry point for:

- connecting to a server
- selecting stacks
- inspecting executions
- providing input to waits (resume)
- retrying failed executions
- triggering replay
- browsing artifacts

The CLI should mirror the core runtime model rather than invent a separate one.

## Auth and connection

```bash
kitaru login https://kitaru.mycompany.com
kitaru status
kitaru info
kitaru logout
```

### What these do

- `login` stores connection/auth state for the current user
- `status` shows the current connection and active stack context
- `info` shows detailed environment information: connection, active stack, project config, SDK version, and server version
- `logout` clears stored auth state

## Stack selection

```bash
kitaru stack list
kitaru stack use local
kitaru stack use prod
kitaru stack current
```

For MVP, stack UX should focus on **selecting** from available stacks, not assembling infra components by hand.

## Running a flow

For local or ad hoc execution, the CLI should allow invoking a flow by module path.

```bash
kitaru run agent.py:content_pipeline --args '{"topic":"AI safety"}'
```

### With an explicit stack

```bash
kitaru run agent.py:content_pipeline --stack prod --args '{"topic":"AI safety"}'
```

This should resolve a new execution using the same execution model as the Python SDK.

## Listing executions

```bash
kitaru executions list
kitaru executions list --status waiting
kitaru executions list --flow content_pipeline
```

Useful filters include:

- `--status`
- `--flow`
- `--stack`
- possibly time range later

## Getting execution details

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

## Streaming logs

```bash
kitaru executions logs kr-a8f3c2 --follow
```

This is useful for live inspection, especially in connected mode.

## Providing input to a wait (resume)

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

## Retry

This is a **same-execution** operation — it does not create a new execution.

```bash
kitaru executions retry kr-a8f3c2
```

Retry semantics:

- same logical execution
- fixed code, fixed config, no user overrides
- reruns from the top, replaying prior durable outcomes
- re-executes from the failure point forward

## Replay

Replay creates a **new execution**.

```bash
kitaru executions replay kr-a8f3c2 --from write_draft
```

Replay with an override should also be possible:

```bash
kitaru executions replay kr-a8f3c2 \
  --from approve \
  --override wait.approve=false
```

Or conceptually:

```bash
kitaru executions replay kr-a8f3c2 \
  --from write_draft \
  --override flow.input.topic='"New topic"'
```

The exact CLI shape can be refined, but the semantics should remain:

- replay creates a new execution
- the old execution is unchanged
- overrides replace selected reused outcomes

## Cancel

```bash
kitaru executions cancel kr-a8f3c2
```

Cancellation behavior should be explicit in implementation, especially for:

- running executions
- waiting executions
- already terminal executions

## Artifact browsing

```bash
kitaru artifacts list kr-a8f3c2
kitaru artifacts list kr-a8f3c2 --name raw_notes
kitaru artifacts list kr-a8f3c2 --type context
```

Get one artifact:

```bash
kitaru artifacts get art_abc123
```

Download a blob artifact:

```bash
kitaru artifacts get art_abc123 --download report.pdf
```

## Config inspection

```bash
kitaru config show
```

This should show the resolved view of:

- connection context
- active stack
- project app config
- maybe selected defaults relevant to execution

It should avoid pretending that all config belongs to one flat hierarchy.

## MVP notes

For March, the CLI should stay focused on the core lifecycle:

- login / status / info / logout
- stack list / use / current
- run
- executions list / get / logs / input / retry / replay / cancel
- artifacts list / get / download
- config show

Anything beyond that should be clearly marked as later or admin-only rather than mixed into the main SDK story.
