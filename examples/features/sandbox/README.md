# Sandbox examples

This group shows how to run a command through the sandbox component attached to
the currently active Kitaru stack from inside a tracked Kitaru flow.

The example starts a flow, runs a checkpoint, and calls the top-level SDK helper,
`kitaru.run_sandbox_command(...)`, inside that checkpoint. It does not create a
stack, switch stacks, or choose a sandbox provider for you. It reads whichever
stack is active, finds that stack's sandbox component, runs one command, returns
the command output, and then cleans up the temporary sandbox session.

## Getting started

From the repository root, install the local runtime dependencies and initialize
Kitaru once:

```bash
uv sync --extra local
uv run kitaru init
```

If you do not already have an active stack with a sandbox, create a local stack:

```bash
uv run kitaru stack create sandbox-demo
```

Local stack creation adds a local sandbox by default and activates the new stack.
Then run the example from the repository root:

```bash
uv run python examples/features/sandbox/active_stack_sandbox_command.py
```

You should see a short result that includes the sandbox command's stdout, exit
code, active stack name, sandbox name if Kitaru reports one, and cleanup status.
Because the command runs inside a checkpoint, the run also creates a Kitaru
execution that you can inspect in the UI or with `kitaru executions list`.

## Safety note

The local sandbox runs a local subprocess. It is useful for development and for
simple deterministic examples, but it is not a security boundary. Treat a command
run through the local sandbox like a command you typed into your own terminal:
it can access local files, environment variables, credentials, and the network
available to your user. If a model or untrusted prompt controls the command, it
can ask the subprocess to print visible values to stdout or stderr. That output
may be returned to the model and stored in Kitaru execution/checkpoint artifacts.
Use an isolated sandbox provider and minimal credentials for untrusted
model-controlled commands.

For the full catalog, see [../../README.md](../../README.md).

## Examples

| Example | Run | What it demonstrates |
|---|---|---|
| [active_stack_sandbox_command.py](active_stack_sandbox_command.py) | `uv run python examples/features/sandbox/active_stack_sandbox_command.py` | Run one harmless command through the active stack's sandbox from inside a tracked flow checkpoint |
