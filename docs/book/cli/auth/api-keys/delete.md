---
description: "Delete an API key."
---

# kitaru auth api-keys delete

## Usage

```bash
kitaru auth api-keys delete SERVICE-ACCOUNT NAME-OR-ID [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `SERVICE-ACCOUNT` | `str` | Yes |  | Owning service-account name or ID. |
| `NAME-OR-ID` | `str` | Yes |  | API-key name or ID. |
| `--yes`, `-y` | `bool` | No | `False` | Skip confirmation prompt. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

