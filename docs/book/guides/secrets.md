---
description: Create, inspect, list, and delete centralized secrets from the Kitaru CLI and Python SDK
icon: lock
---

# Manage Secrets

Secrets are the credentials your flows need at runtime — provider API keys for
`kitaru.llm()`, tokens for the external services your tools call. Storing them
centrally keeps keys out of code and lets a run reproduce later from the same
checkpoint without you re-supplying them. Manage them with `kitaru secrets ...`
or the Python SDK helpers.

{% hint style="info" %}
If you want the full LLM setup story — secret, model alias, and
`kitaru.llm()` inside a flow — start with
[Secrets + Model Registration](secrets-and-model-registration.md).
{% endhint %}

## Create or update a secret

```bash
kitaru secrets set openai-creds --OPENAI_API_KEY=sk-...
```

`set` is an upsert command:

- If the secret does not exist, Kitaru creates it.
- If it already exists, Kitaru updates the provided keys.

New secrets are **public by default**. In this context, "public" means visible to
other users who can access the configured Kitaru/ZenML secret store — it does not
mean internet-public.

To create a private secret instead:

```bash
kitaru secrets set openai-creds --private --OPENAI_API_KEY=sk-...
```

If a secret already exists, `set` updates values only and leaves that secret's
existing visibility unchanged.

## Secret key naming

Use real environment-variable-style key names so downstream tooling can consume
credentials directly:

- `OPENAI_API_KEY`
- `ANTHROPIC_API_KEY`
- `AZURE_OPENAI_API_KEY`

## Show one secret

```bash
kitaru secrets show openai-creds
```

This prints metadata and key names. To include raw values when available:

```bash
kitaru secrets show openai-creds --show-values
```

If your current context cannot access one or more values, those keys appear as
`unavailable`.

## List all accessible secrets

```bash
kitaru secrets list
```

## Delete a secret

```bash
kitaru secrets delete openai-creds
```

## Use secrets from Python

Create and delete helpers return `SecretSummary`, a metadata-only model that
lists key names but never includes raw secret values:

```python
from kitaru import create_secret, delete_secret, get_secret

created = create_secret(
    "github-creds",
    {"GITHUB_TOKEN": "ghp_..."},
)
print(created.private)  # False (public secrets are the default)

private_created = create_secret(
    "openai-creds",
    {"OPENAI_API_KEY": "sk-..."},
    private=True,
)

secret = get_secret("github-creds")
token = secret.get("GITHUB_TOKEN")

deleted = delete_secret("github-creds")
```

`get_secret()` performs an exact lookup by secret name or ID. It returns a
Kitaru `Secret` model with `.name`, `.id`, `.values: dict[str, str]`, and
`.get("KEY")` for optional access.

## Use a secret inside a checkpoint

Kitaru auto-resolves linked secrets for `kitaru.llm()`. If you need credentials
for some other external service, load the secret explicitly with
`kitaru.get_secret()` inside your checkpoint or flow function body:

```python
from kitaru import checkpoint, get_secret


@checkpoint
def call_external_service() -> str:
    secret = get_secret("github-creds")
    token = secret.get("GITHUB_TOKEN")
    if token is None:
        raise RuntimeError("Secret `github-creds` is missing GITHUB_TOKEN.")
    return f"Loaded token with length {len(token)}"
```

Keep the lookup inside the function body so it happens in the actual runtime
context. Do not load secrets at import time.

This applies to *implicit* credential reads too. Provider SDK clients often read
their key the moment they are constructed: building `Agent("openai:gpt-5-nano")`
(PydanticAI) or an OpenAI client at module scope reads `OPENAI_API_KEY` right
then. On a remote stack the runner pod imports your module *before* the run's
secret is applied to the environment, so a module-scope client crashes at import
with a missing-key error. Build provider-backed clients and agents inside your
flow or checkpoint (a small factory function), not at module scope.

{% hint style="warning" %}
Secret values are raw credentials. Avoid logging `secret.values` or returning
raw secret values from checkpoints unless that is explicitly intended.
{% endhint %}

## MCP support

The Kitaru MCP server exposes `kitaru_secrets_create` for metadata-only secret
creation from MCP clients. It intentionally does not expose secret deletion; use
the CLI or Python SDK when you need to delete a secret.

## Related reference pages

- [Secrets + Model Registration](secrets-and-model-registration.md)
- [CLI secrets commands](https://sdkdocs.kitaru.ai)
- [Python secrets reference](https://sdkdocs.kitaru.ai)
- [MCP Server](../agent-native/mcp-server.md)
- [Tracked LLM calls](llm-calls.md)
- [Configuration guide](configuration.md)
