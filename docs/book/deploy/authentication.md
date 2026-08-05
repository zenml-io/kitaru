---
description: Accounts, logins, and KITKEY_ API keys — how people and processes authenticate to your Kitaru server.
icon: key
---

# Authentication & API keys

A Kitaru server is a **trusted-team deployment**: everyone authenticated
can read and write everything, and ownership records who created a
resource without gating access. Authentication decides who gets in, not
who sees what — keep one server per trust boundary. The one exception
is account administration, which is reserved for **admin** accounts.

Two schemes, set by `KITARU_SERVER_AUTH_SCHEME`:

* `none` — no authentication. For local development only.
* `local` — accounts with passwords and API keys, issued and checked by
  the server itself. This is the mode for a shared deployment.

## Logging in

```bash
kitaru login https://kitaru.internal.example.com
```

Interactive login uses a device flow — the CLI shows a short `XXXX-XXXX`
code and opens your browser — or a password prompt. Credentials are
stored per server **context**, so `kitaru context use` switches between
servers cleanly. Non-interactive variants:

```bash
kitaru login https://... --username you --password-stdin
kitaru login https://... --api-key-stdin
kitaru logout            # this server; --all for every context
```

## API keys for processes

Workers, CI, and production services authenticate with API keys — the
`KITKEY_` prefix — passed through the environment everything reads:

```bash
export KITARU_API_URL="https://kitaru.internal.example.com"
export KITARU_API_KEY="KITKEY_..."
```

Create a key with the Python client (the plaintext is returned exactly
once, at creation):

```python
from kitaru.api_models.v1.api_key import ApiKeyCreateRequest

issued = await client.api_keys.create(ApiKeyCreateRequest(name="ci-runner"))
print(issued.key)        # shown once — store it in your secret manager
```

<!-- TODO(v2-launch): `kitaru api-key create` / `kitaru account` CLI verbs
     are not in the current CLI — update to CLI commands if they land
     before launch. -->

Keys can be rotated in place — `client.api_keys.rotate(key_id)` returns
a fresh plaintext (again, exactly once), with an optional
`retain_period_minutes` grace window during which the old key still
works, so a worker fleet can pick up the new key without a
stop-the-world cutover. Keys can also be deactivated (`update` with
`active=False`) and deleted; `last_used` on the key tells you which ones
are dead. Give each consumer its own named key so revocation is
surgical.

## Workers and tasks get scoped tokens

An API key is the only long-lived credential a worker ever holds — and it
uses it once, to register. From there the credentials narrow at each
step:

* Registering (`kitaru worker start`) returns a **worker token**, a
  bearer token scoped to that one worker, which the worker renews on its
  own by re-registering under the same name.
* Each claimed task comes with a **task token** scoped to that single
  task and attempt, carrying an explicit allowlist of the sessions and
  blobs the task may touch. The worker hands *that* to your agent
  subprocess as `KITARU_TASK_TOKEN` — your broad API key is stripped
  from the child environment.

So the process running arbitrary agent code holds a credential that can
write its own session and nothing else, and it expires with the attempt.
None of this needs configuration; it's how workers authenticate.

## Accounts for the team

Accounts are created through the API, by an **admin** — creating and
deactivating accounts and granting admin rights are the only
admin-gated operations on the server. An account can't change its own
admin flag, and service accounts can't be admins. An account created
without a password returns a one-time **activation token**; hand it to
the teammate and they set their own password:

```python
from kitaru.api_models.v1.account import AccountCreateRequest

account = await client.accounts.create(AccountCreateRequest(name="dana"))
print(account.activation_token)   # share once, out of band
```

Deactivating an account (`POST /v1/accounts/{id}/deactivate`) locks it
out and mints a fresh activation token for later reinstatement. There is
no account deletion — provenance on resources stays intact.

The server bootstraps a `default` account — an admin — on first start;
`KITARU_SERVER_DEFAULT_ACCOUNT_PASSWORD` sets its initial password so
your first `kitaru login` works. Pass `is_admin=True` when creating an
account to make more admins.
