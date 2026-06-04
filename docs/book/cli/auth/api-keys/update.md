---
description: "Update API-key metadata without revealing the raw key value."
---

# kitaru auth api-keys update

## Usage

```bash
kitaru auth api-keys update SERVICE-ACCOUNT NAME-OR-ID [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `SERVICE-ACCOUNT` | `str` | Yes |  | Owning service-account name or ID. |
| `NAME-OR-ID` | `str` | Yes |  | API-key name or ID. |
| `--name` | `str` | No | `None` | New API-key name. |
| `--description` | `str` | No | `None` | New API-key description. |
| `--active` | `bool` | No | `None` | Set whether the API key is active. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

