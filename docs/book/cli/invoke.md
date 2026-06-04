---
description: "Invoke a deployed flow snapshot."
---

# kitaru invoke

If you omit `--version` and `--tag`, Kitaru tries the implicit `default` route. If that route is missing, invoke with an explicit tag/version or move `default` with `kitaru flow tag`.

## Usage

```bash
kitaru invoke FLOW [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `FLOW` | `str` | Yes |  | Deployment-backed flow name. |
| `--version` | `int` | No | `None` | Exact deployment version. |
| `--tag` | `str` | No | `None` | Deployment tag selector. |
| `--input` | `str` | No | `None` | Invocation inputs as JSON or `@file`. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

