---
description: Accounts, logins, and KITKEY_ API keys — how people and processes authenticate to your Kitaru server.
icon: key
---

# Authentication & API keys

A Kitaru server is a **trusted-team deployment**: everyone authenticated can read and write everything, and ownership records who created a resource without gating access. Authentication decides who gets in, not who sees what — keep one server per trust boundary. The one exception is account administration, which is reserved for **admin** accounts.

Two schemes, set by `KITARU_SERVER_AUTH_SCHEME`:

- `none` — no authentication. For local development only.
- `local` — accounts with passwords and API keys, issued and checked by the server itself. This is the mode for a shared deployment.

## Logging in

```bash
kitaru login https://kitaru.internal.example.com
```

Interactive login uses a device flow — the CLI shows a short `XXXX-XXXX` code and opens your browser — or a password prompt. Credentials are stored separately for each server. Logging in selects that server for later commands; you can override it with `--server` or `KITARU_API_URL`. Non-interactive variants:

```bash
kitaru login https://... --username you --password-stdin
kitaru login https://... --api-key-stdin
kitaru logout            # selected server; --all for every stored credential
```

## API keys for processes

Workers, CI, and production services authenticate with API keys — the `KITKEY_` prefix — passed through the environment everything reads:

```bash
export KITARU_API_URL="https://kitaru.internal.example.com"
export KITARU_API_KEY="KITKEY_..."
```

Create a key with the Python client (the plaintext is returned exactly once, at creation):

```python
from kitaru.api_models.v1.api_key import ApiKeyCreateRequest

issued = await client.api_keys.create(ApiKeyCreateRequest(name="ci-runner"))
print(issued.key)        # shown once — store it in your secret manager
```

<!-- TODO(v2-launch): `kitaru api-key create` / `kitaru account` CLI verbs
     are not in the current CLI — update to CLI commands if they land
     before launch. -->

Keys can be rotated in place — `client.api_keys.rotate(key_id)` returns a fresh plaintext (again, exactly once), with an optional `retain_period_minutes` grace window during which the old key still works, so a worker fleet can pick up the new key without a stop-the-world cutover. Keys can also be deactivated (`update` with `active=False`) and deleted; `last_used` on the key tells you which ones are dead. Give each consumer its own named key so revocation is surgical.

## Workers and tasks get scoped tokens

An API key is the only long-lived credential a worker holds. It uses the key to register and again whenever it re-registers to renew its worker token. Credentials narrow for task execution:

- Registering (`kitaru worker start`) returns a **worker token**, a bearer token scoped to that one worker, which the worker renews on its own by re-registering under the same name.
- Each claimed task comes with a **task token** scoped to that single task and attempt, carrying an explicit allowlist of the sessions and blobs the task may touch. The worker hands _that_ to your agent subprocess as `KITARU_API_TOKEN` — your broad API key is stripped from the child environment.

Clients that consume `KITARU_API_TOKEN`, including `KitaruAPIClient`, use the task-scoped credential. Its expiry is set when the task is claimed to the task's execution timeout plus a server-configured leeway; completing the attempt does not revoke it immediately. The CLI does not currently consume that variable and may fall back to a stored login credential. Run workers under a dedicated OS or container identity with no broader stored Kitaru credentials when agent code can invoke the CLI.

## Accounts for the team

There are two kinds of account, and they are managed separately. **Users** are people who log in; **service accounts** are non-human identities that carry API keys. `/v1/accounts` reads across both — list them, fetch one, or ask who you are with `client.accounts.get_current()` — but every change goes through the specific surface.

Creating accounts and granting admin rights are admin-gated. An account can't change its own admin flag, and service accounts can't be admins.

A user created without a password returns a one-time **activation token**; hand it to the teammate and they set their own password with it:

```python
from kitaru.api_models.v1.account import (
    UserActivationTokenResponse,
    UserCreateRequest,
)

account = await client.users.create(UserCreateRequest(name="dana"))
assert isinstance(account, UserActivationTokenResponse)
print(account.activation_token)   # share once, out of band
```

When a password is supplied, `create()` returns a normal `AccountResponse`. Without one, it returns `UserActivationTokenResponse` with the one-time token.

`client.users.deactivate(account_id)` (`POST /v1/users/{id}/deactivate`) locks a person out and returns a fresh activation token, shown once, so the same account can be reinstated later with `client.users.activate(...)` — the token plus a new password.

Service accounts have no activation dance, because nobody logs into them. Create one with `client.service_accounts.create(...)`, then issue it an API key; disable it by setting `active=False` through `client.service_accounts.update(...)` (`PATCH /v1/service-accounts/{id}`).

Neither kind can be deleted — provenance on resources stays intact.

The server bootstraps a `default` account — an admin — on first start; `KITARU_SERVER_DEFAULT_ACCOUNT_PASSWORD` sets its initial password so your first `kitaru login` works. Pass `is_admin=True` when creating an account to make more admins.
