---
description: "Show API-key metadata without revealing the raw key value."
---

# kitaru auth api-keys show

## Usage

```bash
kitaru auth api-keys show SERVICE-ACCOUNT NAME-OR-ID [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `SERVICE-ACCOUNT` | `str` | Yes |  | Owning service-account name or ID. |
| `NAME-OR-ID` | `str` | Yes |  | API-key name or ID. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

