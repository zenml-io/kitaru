"""Secret-template resolution for sandbox proxy rules.

`SandboxProxyRule.headers` values can contain `{{ secret-name.key }}`
templates. This module turns a Profile's proxy rules into a resolved
`{host: {header: value}}` map suitable for `DockerProxy(credential_map=...)`.

The resolution happens once, at flow start, on the host process. The
worker container never sees the templates *or* the resolved values —
the credential map is handed straight to the proxy container's env.
"""

import re

import kitaru

from .profile import Profile

_TEMPLATE_RE = re.compile(
    r"\{\{\s*(?P<name>[A-Za-z0-9._\-]+)\.(?P<key>[A-Za-z0-9._\-]+)\s*\}\}"
)


def resolve_secret_templates(value: str, *, cache: dict[str, dict[str, str]]) -> str:
    """Replace `{{ name.key }}` in `value` with values from kitaru.secrets.

    `cache` is shared across calls so we don't re-fetch the same secret
    multiple times within a single resolve_credential_map invocation.
    """

    def _sub(match: re.Match[str]) -> str:
        name = match.group("name")
        key = match.group("key")
        if name not in cache:
            cache[name] = kitaru.get_secret(name).values
        if key not in cache[name]:
            raise KeyError(
                f"Secret {name!r} has no key {key!r}. Known keys: {sorted(cache[name])}"
            )
        return cache[name][key]

    return _TEMPLATE_RE.sub(_sub, value)


def build_credential_map(profile: Profile) -> dict[str, dict[str, str]]:
    """Resolve a Profile's sandbox_proxy_rules into a host→headers map.

    Returns the `{host: {header: value}}` shape that DockerProxy expects.
    Values are fully resolved — no `{{ … }}` templates remain.
    """
    cache: dict[str, dict[str, str]] = {}
    result: dict[str, dict[str, str]] = {}
    for rule in profile.sandbox_proxy_rules:
        resolved = {
            header: resolve_secret_templates(value, cache=cache)
            for header, value in rule.headers.items()
        }
        for host in rule.hosts:
            result.setdefault(host, {}).update(resolved)
    return result
