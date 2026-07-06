---
description: List, inspect, create, switch, and delete Kitaru projects from the CLI, SDK, and MCP server
icon: folder-tree
---

# Projects

A **Kitaru project** is the workspace where your executions, artifacts,
deployments, stacks, and secrets are grouped on a Kitaru server.

A concrete example helps: imagine one server used by the same team for
`production` and `staging`. If your active project is `production`, then
`kitaru executions list` shows production executions and `KitaruClient()` reads
production deployments. If you switch to `staging`, the same commands look at
staging instead. The server did not change; the selected project changed.

There are two normal ways to choose a project:

1. **Persist it for your local shell:** run `kitaru project use production`.
   Kitaru remembers that choice for later CLI and SDK calls.
2. **Set it explicitly for headless environments:** set `KITARU_PROJECT` in CI,
   Docker, cron jobs, and other non-interactive processes.

`KITARU_PROJECT` wins over the persisted choice. That is intentional: a CI job
should not silently inherit whatever project someone last selected on a laptop.

## List and inspect projects

```bash
kitaru project list
kitaru project current
kitaru project show production
```

Use JSON output when another tool will consume the result:

```bash
kitaru project list -o json
kitaru project current -o json
```

The serialized project shape is the same across CLI and MCP:

```json
{
  "id": "project-id",
  "name": "production",
  "display_name": "Production",
  "description": "Customer-facing runs",
  "is_active": true
}
```

`display_name` and `description` may be `null` when they are not set.

## Create and switch projects

Create a project:

```bash
kitaru project create staging
```

By default, `project create` also activates the new project. After that command,
subsequent Kitaru commands use `staging` unless an environment variable or
process-local override says otherwise.

If you want to create a project without switching to it:

```bash
kitaru project create staging --no-activate
```

Switch to an existing project:

```bash
kitaru project use production
```

That command asks the Kitaru backend to make `production` the active project and
persists the choice in the same place the backend already uses for active
project state. In practice, that means you do not get two competing answers to
"which project am I using?" — Kitaru reads the same active project that it writes.

## Delete projects

Project deletion is deliberately explicit:

```bash
kitaru project delete staging --yes
```

Without `--yes`, the CLI refuses to call the backend delete operation. Deleting a
project removes durable server state, so do not use it as a smoke-test or health
check command.

## Login with a project

When connecting to a remote server, you can choose the project in the same step:

```bash
kitaru login https://kitaru.example.com --project production
```

Text output says `Project: production`. It does not print the older `Active
project` wording. JSON output includes the supplied project name or `null`.

If you omit `--project`, Kitaru does not guess one in the login output. You can
then run:

```bash
kitaru project list
kitaru project use production
```

## Headless, Docker, and CI

For non-interactive processes, set the connection and project explicitly:

```bash
export KITARU_SERVER_URL=https://kitaru.example.com
export KITARU_AUTH_TOKEN=kat_...
export KITARU_PROJECT=production
```

This is safer than relying on persisted local state. The job starts, reads the
three variables, and there is no ambiguity about which server and project it
will use.

If `KITARU_SERVER_URL` and `KITARU_AUTH_TOKEN` come from environment variables,
Kitaru requires `KITARU_PROJECT` before project-scoped operations such as
running, listing, replaying, or invoking executions. Auth-management commands
are the exception because service accounts and API keys belong to the server,
not to one project.

## Python SDK

Use a normal client once a project is selected:

```python
from kitaru import KitaruClient

client = KitaruClient()
current = client.projects.current()
print(current.name)
```

Project operations live under `client.projects`:

```python
projects = client.projects.list()
staging = client.projects.get("staging")
created = client.projects.create("experiment", activate=False)
active = client.projects.use("production")
```

Use `KitaruClient.for_project_management()` when the process needs to list,
create, or select projects before a project-scoped client can exist:

```python
from kitaru import KitaruClient

client = KitaruClient.for_project_management()
for project in client.projects.list():
    print(project.name)

client.projects.use("production")
```

The distinction is simple:

- `KitaruClient()` is for project-scoped work: executions, artifacts,
  deployments, and normal runtime operations.
- `KitaruClient.for_project_management()` is for choosing the project itself.
  It still validates the server/auth pairing, but it does not require a project
  to already be selected.

## MCP tools

The MCP server exposes read/switch project tools for automation agents:

- `kitaru_projects_list`
- `kitaru_projects_current`
- `kitaru_projects_show`
- `kitaru_projects_use`

It does **not** expose project create/delete tools in this first pass. Agents can
inspect the current project and switch to a named project, but durable project
creation and deletion stay in the CLI and SDK.

A useful agent instruction is:

```text
Check the current Kitaru project. If it is not production, switch to production before listing deployments.
```

## Precedence summary

When several sources name a project, Kitaru resolves them from lower to higher
priority like this:

1. Persisted active project from `kitaru login --project` or
   `kitaru project use ...`
2. Compatibility `ZENML_ACTIVE_PROJECT_ID` environment variable
3. Public `KITARU_PROJECT` environment variable
4. Process-local `kitaru.configure(project=...)`

For normal usage, prefer the first and third entries: `kitaru project use` for
interactive local work, and `KITARU_PROJECT` for CI, Docker, and other headless
execution.

## Related pages

- [Configuration](configuration.md)
- [Authentication](authentication.md)
- [Deployments](deployments.md)
- [MCP Server](../agent-native/mcp-server.md)
- [Python and CLI reference](https://sdkdocs.kitaru.ai)
