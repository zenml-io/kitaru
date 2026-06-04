---
description: "Create a service-account API key and print its one-time value."
---

# kitaru auth api-keys create

## Usage

```bash
kitaru auth api-keys create SERVICE-ACCOUNT NAME [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `SERVICE-ACCOUNT` | `str` | Yes |  | Owning service-account name or ID. |
| `NAME` | `str` | Yes |  | API-key name. |
| `--description` | `str` | No | `""` | Optional API-key description. |
| `--set-key` | `bool` | No | `False` | Use this API key as the active local server credential. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

