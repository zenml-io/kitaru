---
description: Store provider credentials, register a model alias, and use kitaru.llm() inside a flow
icon: key
---

# Secrets + Model Registration

This walkthrough shows the full setup path for tracked LLM calls in Kitaru:

1. store provider credentials in a secret
2. register a model alias that points at that secret
3. call `kitaru.llm()` inside a flow using the alias

This is the most reusable setup because your flow code can stay stable while you
change credentials or swap the underlying model later.

## 1) Store provider credentials in a secret

Create a secret with real environment-variable-style key names:

```bash
kitaru secrets set openai-creds --OPENAI_API_KEY=sk-...
```

What this does:

- creates the secret if it does not exist yet
- updates the provided keys if the secret already exists
- creates a public secret by default

In this context, "public" means visible to other users who can access the
configured Kitaru/ZenML secret store — it does not mean internet-public. If your
provider credentials should be private, pass `--private` when creating the
secret:

```bash
kitaru secrets set openai-creds --private --OPENAI_API_KEY=sk-...
```

Use the exact key names your provider expects, for example:

- `OPENAI_API_KEY`
- `ANTHROPIC_API_KEY`
- `OPENROUTER_API_KEY`

## 2) Register a model alias

Now register a reusable alias that points to a provider-qualified model string and
links the secret by name:

```bash
kitaru model register fast --model openai/gpt-5-nano --secret openai-creds
```

You can inspect the aliases you have registered with:

```bash
kitaru model list
```

What Kitaru stores here:

- the alias name (`fast`)
- the real model string (`openai/gpt-5-nano`)
- a reference to the secret (`openai-creds`), not the secret's raw values

{% hint style="info" %}
`kitaru model register` stores a secret reference, not the secret value
itself. The first alias you register also becomes your default alias
automatically. Submitted and replayed runs automatically receive the current
registry snapshot through `KITARU_MODEL_REGISTRY`, so remote executions can
still resolve the alias.
{% endhint %}

## 3) Use the alias inside a flow

Once the alias exists, your flow code can stay simple:

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

That code uses the alias name `fast`, not the raw provider/model string.

## 4) What happens at runtime

When `kitaru.llm()` runs, Kitaru does the following:

1. resolves the model you asked for
2. checks whether that value is an alias in the effective registry visible to the runtime
3. if the alias has a linked secret, tries to resolve credentials at runtime
4. calls the provider SDK with the resolved model and credentials
5. saves prompt/response artifacts and logs usage and latency metadata

For built-in providers that require credentials (OpenAI, Anthropic,
OpenRouter), credential lookup is **environment first**:

1. if the provider's env var is already set, Kitaru uses the environment
2. otherwise, if the alias has a linked secret, Kitaru loads that secret
3. if neither is available, the call fails with setup guidance

{% hint style="info" %}
Model selection precedence for `kitaru.llm()` is: explicit `model=` argument
first, then `KITARU_DEFAULT_MODEL`, then the effective default alias visible
in the current environment. If
`KITARU_DEFAULT_MODEL` matches a registered alias, Kitaru resolves it as an
alias. Otherwise it is treated as a raw provider/model string.
{% endhint %}

## 5) Environment-only shortcut

If you do not want to link a secret, you can keep credentials in the runtime
environment instead:

```bash
kitaru model register fast --model openai/gpt-5-nano
export OPENAI_API_KEY=sk-...
```

This is a useful shortcut for local development.

For advanced custom environments, you can also set `KITARU_MODEL_REGISTRY`
explicitly to add aliases or override matching transported aliases.

For known providers, environment variables still win if both are present. In
other words: if your alias links `openai-creds` but `OPENAI_API_KEY` is already
exported in the process, Kitaru uses the environment value.

## 6) Non-LLM secrets

The walkthrough above covers the fully automatic LLM path: alias-linked
credentials are loaded for `kitaru.llm()` when the provider environment variable
is not already set.

If you need a secret for some other external service, you have two options:

- **Read the secret inside a flow or checkpoint.** Call `kitaru.get_secret()`
  where the credential is needed. See [Manage Secrets](secrets.md) for an
  example.
- **Expose the secret's keys as environment variables for the whole run.**
  Configure `ImageSettings.secret_environment_from` with a list of secret
  names/IDs:

  ```python
  @flow(
      image=kitaru.ImageSettings(
          secret_environment_from=["openai-creds"],
      ),
  )
  def my_flow() -> None:
      import os

      # Keys from the referenced secret are available via os.environ at runtime.
      client.authenticate(os.environ["OPENAI_API_KEY"])
  ```

  Kitaru forwards the list to ZenML via `Pipeline.with_options(secrets=[...])`,
  so secret values are resolved at runtime and never placed in image metadata,
  Docker environment, or the frozen execution spec. This is distinct from
  `KITARU_MODEL_REGISTRY`, which transports **non-secret** model alias data to
  remote executions — secret references and alias data travel on separate
  paths. See
  [Containerization → Secret-backed environment variables](containerization.md#secret-backed-environment-variables)
  for the full story.

## Related pages

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Manage Secrets</strong></td><td>Secret CRUD commands and explicit non-LLM secret reads</td><td><a href="secrets.md">secrets.md</a></td></tr><tr><td><strong>Tracked LLM Calls</strong></td><td>Model resolution, credential precedence, and runtime behavior</td><td><a href="llm-calls.md">llm-calls.md</a></td></tr><tr><td><strong>CLI model commands</strong></td><td>Reference for kitaru model register and model list</td><td><a href="../cli/model.md">../cli/model.md</a></td></tr><tr><td><strong>CLI secrets commands</strong></td><td>Reference for kitaru secrets set/show/list/delete</td><td><a href="../cli/secrets.md">../cli/secrets.md</a></td></tr></tbody></table>
