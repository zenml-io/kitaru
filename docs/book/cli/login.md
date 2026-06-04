---
description: "Connect to a remote server, or start and connect to a local server."
---

# kitaru login

## Usage

```bash
kitaru login [SERVER] [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `SERVER` | `str` | No | `None` | Kitaru server URL, managed workspace name, or managed workspace ID. Omit to start a local server. |
| `--api-key` | `str` | No | `None` | API key for remote server authentication. |
| `--refresh` | `bool` | No | `False` | Force a fresh authentication flow (remote only). |
| `--project` | `str` | No | `None` | Project to activate after connecting (remote only). |
| `--no-verify-ssl` | `bool` | No | `False` | Disable TLS certificate verification (remote only). |
| `--ssl-ca-cert` | `str` | No | `None` | Path to a CA bundle for server verification (remote only). |
| `--port` | `int` | No | `None` | Port for the local server (default: 8383). |
| `--timeout` | `int` | No | `60` | Timeout in seconds for server startup or connection. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

