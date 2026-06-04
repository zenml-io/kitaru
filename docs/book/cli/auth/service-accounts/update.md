---
description: "Update service-account metadata."
---

# kitaru auth service-accounts update

## Usage

```bash
kitaru auth service-accounts update NAME-OR-ID [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `NAME-OR-ID` | `str` | Yes |  | Service-account name or ID. |
| `--name` | `str` | No | `None` | New service-account name. |
| `--description` | `str` | No | `None` | New service-account description. |
| `--active` | `bool` | No | `None` | Set whether the service account is active. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

