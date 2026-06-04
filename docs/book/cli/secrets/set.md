---
description: "Set a public secret by default, or create it privately with --private."
---

# kitaru secrets set

## Usage

```bash
kitaru secrets set NAME ASSIGNMENTS... [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `NAME` | `str` | Yes |  | Secret name. |
| `ASSIGNMENTS...` | `list[str]` | Yes |  | One or more secret assignments in `--KEY=value` form. |
| `--private` | `bool` | No | `False` | Create a private secret instead of the default public secret. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

