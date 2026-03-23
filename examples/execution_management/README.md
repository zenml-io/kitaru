# Execution management examples

These examples show the "control plane" side of Kitaru: inspecting runs,
handling waits, and resuming execution. Once your flows are running, this is
how you interact with them programmatically.

## Getting started

```bash
cd examples/execution_management
uv sync --extra local       # Install dependencies (from repo root, or use pip)
kitaru init                  # Initialize a Kitaru project in this directory
python <module_name>.py      # Run any example
```

These examples use your current Kitaru connection context. If you want to
inspect or resume executions on a deployed Kitaru server, connect first with
`kitaru login <server>` and verify with `kitaru status`.

For the full catalog, see [../README.md](../README.md).

## Examples

### `client_execution_management.py` — Browse executions and artifacts

Shows `KitaruClient`, the programmatic interface for inspecting past work.
Runs a flow, then uses the client to fetch execution details, find the
latest execution by flow name, list all artifacts from a run, and load a
saved artifact back into Python. This is how dashboards, scripts, and
downstream flows retrieve results from earlier executions.

```bash
python client_execution_management.py
```

### `wait_and_resume.py` — Pause for human input, resume later

Demonstrates `kitaru.wait()` — a durable suspension point that releases
compute while waiting for input. The flow drafts a release note, then
pauses for human approval before publishing. When running interactively,
you are prompted inline. When running remotely (or after a timeout), the
example prints fallback CLI commands so you can provide input and resume
from a separate terminal:

```bash
kitaru executions input <exec_id> --value true
kitaru executions resume <exec_id>
```

This pattern is how human-in-the-loop works in production: the agent
suspends at $0 compute cost, and a human (or another agent) can approve
minutes, hours, or days later.

```bash
python wait_and_resume.py
```
