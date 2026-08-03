---
description: Store credentials once, encrypted at rest, and inject them into replayed agents through agent version run specs.
icon: lock
---

# Secrets

A replayed agent needs the same credentials the original had — a model
provider key, a database URL. You could bake them into every worker's
environment; **secrets** are the managed alternative: named bundles of
key-value pairs, stored encrypted on the server
(`KITARU_SERVER_SECRET_ENCRYPTION_KEY`), and injected into agent
subprocesses at run time.

## Create a secret

```python
from kitaru.api_models.v1.secret import SecretCreateRequest

secret = await client.secrets.create(
    SecretCreateRequest(
        name="openai",
        values={"OPENAI_API_KEY": "sk-..."},
    )
)
```

<!-- TODO(v2-launch): a `kitaru secret` CLI noun is not in the current
     CLI — update this page if it ships. -->

Values are write-mostly: listings and gets return metadata only unless
you explicitly request values (`include_values`), and updates replace the
value map wholesale.

## Attach it to an agent version

Reference secrets when registering the version — each key in the secret
becomes an environment variable of the replayed agent's process:

```bash
kitaru agent register support-agent \
  --command "python support.py" \
  --secret-id <openai-secret-id>
```

When a [worker](workers.md) runs a replay for that version, it fetches
the referenced secrets and layers them onto the subprocess environment —
after the version's own `--env` entries, with later secrets winning on
key collisions. The worker's own `KITARU_API_URL`/`KITARU_API_KEY` can
never be overridden by a secret.

## What secrets don't cover (yet)

Evaluator and importer plugins run without a run spec, so they don't
receive per-plugin secrets — an LLM-judge evaluator reads its provider
key from the **worker's** environment. Put judge credentials in the
environment of the workers that run evaluations; per-plugin secret
references are on the roadmap.

Rotation is an update plus nothing else: the next task fetches the new
values. Nothing caches decrypted secrets on disk.
