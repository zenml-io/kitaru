---
description: "Rotate an API key and print its one-time replacement value."
---

# kitaru auth api-keys rotate

## Usage

```bash
kitaru auth api-keys rotate SERVICE-ACCOUNT NAME-OR-ID [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `SERVICE-ACCOUNT` | `str` | Yes |  | Owning service-account name or ID. |
| `NAME-OR-ID` | `str` | Yes |  | API-key name or ID. |
| `--retain-minutes` | `int` | No | `0` | Minutes to keep the old key valid during rotation. |
| `--set-key` | `bool` | No | `False` | Use the rotated key as the active local server credential. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

