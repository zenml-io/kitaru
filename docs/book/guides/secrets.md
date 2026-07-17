---
description: Store provider credentials as secrets, register a model alias, and use them from kitaru.llm() and your tools.
icon: lock
---

# Secrets

Secrets are the credentials your flows need at runtime — provider API keys for
`kitaru.llm()`, tokens for the external services your tools call. Storing them
centrally keeps keys out of code and lets a run reproduce later from the same
checkpoint without you re-supplying them. Manage them with `kitaru secrets ...`
or the Python SDK helpers.

This page covers both secret management and the model-alias workflow that pairs
with it: a model alias decouples your flow code from a specific provider/model
and its credentials, so you can rotate a key or swap a model — including on
replay — without touching code. Jump to
[Register a model alias](#store-credentials-and-register-a-model-alias) for that
walkthrough.

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
never includes raw secret values. Its `keys_known` field tells you whether
`keys` and `has_missing_values` are authoritative:

```python
from kitaru import create_secret, delete_secret, get_secret, list_secrets

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

listed = list_secrets()
for summary in listed:
    print(summary.name, summary.keys_known)  # keys_known is False for list results

deleted = delete_secret("github-creds")
```

`create_secret()` and `delete_secret()` receive authoritative key metadata, so
`keys_known=True` and `keys` is authoritative, including when it is empty.
`list_secrets()` deliberately uses metadata-only backend responses and does not
load each secret individually. Its summaries therefore use `keys_known=False`,
`keys=[]`, and `has_missing_values=False`. Those empty and false values mean the
key metadata was unavailable, not that the stored secret has no keys or missing
values.

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
creation and `kitaru_secrets_list` for paginated discovery without values. List
results follow the same `keys_known=False` contract described above. The server
intentionally does not expose secret deletion; use the CLI or Python SDK when
you need to delete a secret.

## Store credentials and register a model alias

A model alias points a stable name (`fast`) at a real provider/model string
(`openai/gpt-5-nano`) and, optionally, at a secret that holds its credentials.
Your flow code uses the alias, so you can rotate the key or swap the model — even
on a [replay](replay-and-overrides.md) — without editing code.

The full path is three steps: store credentials in a secret, register an alias
that references it, then call `kitaru.llm()` with the alias.

### 1. Store provider credentials in a secret

```bash
kitaru secrets set openai-creds --OPENAI_API_KEY=sk-...
```

Use the exact key names your provider expects (`OPENAI_API_KEY`,
`ANTHROPIC_API_KEY`, `OPENROUTER_API_KEY`). Pass `--private` if the credentials
should not be visible to other users of the secret store.

### 2. Register a model alias

```bash
kitaru model register fast --model openai/gpt-5-nano --secret openai-creds
kitaru model list
```

Kitaru stores the alias name, the real model string, and a *reference* to the
secret — never the secret's raw values.

{% hint style="info" %}
The first alias you register also becomes your default alias. Submitted and
replayed runs automatically receive the current registry snapshot through
`KITARU_MODEL_REGISTRY`, so remote executions can still resolve the alias.
{% endhint %}

### 3. Use the alias inside a flow

```python
import kitaru
from kitaru import checkpoint, flow


@checkpoint
def write_draft(topic: str, outline: str) -> str:
    return kitaru.llm(
        f"Write a short paragraph about {topic} using this outline:\n{outline}",
        model="fast",
        name="draft_call",
    )


@flow
def llm_writer(topic: str) -> str:
    outline = kitaru.llm(
        f"Create a 3-bullet outline about {topic}.",
        model="fast",
        name="outline_call",
    )
    return write_draft(topic, outline)
```

### What happens at runtime

When `kitaru.llm()` runs, Kitaru resolves the model, checks whether it is an alias
in the effective registry, and — for built-in providers that need credentials
(OpenAI, Anthropic, OpenRouter) — resolves credentials **environment first**:

1. if the provider's env var is already set, Kitaru uses the environment;
2. otherwise, if the alias has a linked secret, Kitaru loads that secret;
3. if neither is available, the call fails with setup guidance.

So environment variables win over a linked secret for known providers. Model
selection precedence is the explicit `model=` argument, then
`KITARU_DEFAULT_MODEL`, then the effective default alias.

### Environment-only shortcut

Skip the linked secret and keep credentials in the environment when developing
locally:

```bash
kitaru model register fast --model openai/gpt-5-nano
export OPENAI_API_KEY=sk-...
```

## Related reference pages

- [Track cost and model usage](llm-calls.md)
- [CLI secrets commands](https://sdkdocs.kitaru.ai)
- [Python secrets reference](https://sdkdocs.kitaru.ai)
- [MCP Server](../agent-native/mcp-server.md)
- [Configuration guide](configuration.md)
